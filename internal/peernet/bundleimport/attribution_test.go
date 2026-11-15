package bundleimport

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reasonsInTypes lists every constant of type Reason declared in types.go, so a
// new reason cannot be added without an attribution decision.
func reasonsInTypes(t *testing.T) map[string]Reason {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "types.go", nil, 0)
	require.NoError(t, err)
	out := map[string]Reason{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, sp := range gd.Specs {
			vs := sp.(*ast.ValueSpec)
			id, ok := vs.Type.(*ast.Ident)
			if !ok || id.Name != "Reason" {
				continue
			}
			for k, n := range vs.Names {
				lit, ok := vs.Values[k].(*ast.BasicLit)
				require.True(t, ok)
				v, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)
				out[n.Name] = Reason(v)
			}
		}
	}
	return out
}

func TestReasonAttributionCoversEveryReason(t *testing.T) {
	declared := reasonsInTypes(t)
	require.NotEmpty(t, declared)
	seen := map[Reason]bool{}
	for name, r := range declared {
		_, ok := reasonAttribution[r]
		assert.True(t, ok, "%s needs an entry in reasonAttribution (can article bytes contradict the bundle under it?)", name)
		seen[r] = true
	}
	for r := range reasonAttribution {
		assert.True(t, seen[r], "reasonAttribution lists %q, which types.go does not declare", r)
	}
}

// Expected attribution of every reason when the verifier says bytes
// contradicted the bundle: only the two byte-contradiction reasons are
// attributable.
func TestDecide_EveryReasonWithItsExpectedAttribution(t *testing.T) {
	attributable := map[Reason]bool{ReasonYencMismatch: true, ReasonExtentMismatch: true}
	for _, r := range reasonsInTypes(t) {
		t.Run(string(r), func(t *testing.T) {
			i := &Importer{}
			d := i.decide(context.Background(), strings.Repeat("a", 64), rejection{Reason: r, Bytes: true, Evidence: "x"})
			assert.Equal(t, attributable[r], d.Attributable)
			assert.False(t, d.Blacklist, "one article never blacklists")
			// Without bytes contradicting, nothing is attributable.
			d = i.decide(context.Background(), strings.Repeat("a", 64), rejection{Reason: r, Evidence: "y"})
			assert.Equal(t, decision{}, d)
		})
	}
}

func TestDecide_CancelledContextIsNeverAttributable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := (&Importer{}).decide(ctx, "b", rejection{Reason: ReasonYencMismatch, Bytes: true, Evidence: "x"})
	assert.Equal(t, decision{}, d)
}

func TestDecide_BlacklistsOnlyAfterTwoDifferentArticles(t *testing.T) {
	i := &Importer{}
	ctx := context.Background()
	rej := func(ev string) rejection { return rejection{Reason: ReasonYencMismatch, Bytes: true, Evidence: ev} }
	assert.Equal(t, decision{Attributable: true}, i.decide(ctx, "b1", rej("article 3")))
	assert.Equal(t, decision{Attributable: true}, i.decide(ctx, "b1", rej("article 3")), "the same article again is still one")
	assert.Equal(t, decision{Attributable: true, Blacklist: true}, i.decide(ctx, "b1", rej("article 9")))
	assert.Equal(t, decision{Attributable: true}, i.decide(ctx, "b2", rej("article 9")), "strikes are per Bundle ID")
	assert.Equal(t, decision{Attributable: true}, i.decide(ctx, "", rej("a")), "no Bundle ID, nothing to blacklist")
	assert.Equal(t, decision{Attributable: true}, i.decide(ctx, "", rej("b")))
}

func TestStrikeBook_ExpiresAndIsBounded(t *testing.T) {
	var s strikeBook
	t0 := time.Unix(1_700_000_000, 0)
	assert.Equal(t, 1, s.add("b", "a", t0))
	assert.Equal(t, 1, s.add("b", "c", t0.Add(strikeWindow+time.Second)), "the old strike expired")
	for k := 0; k < maxStrikeEvidence+5; k++ {
		s.add("e", strconv.Itoa(k), t0)
	}
	assert.Equal(t, maxStrikeEvidence, len(s.m["e"]), "evidence per bundle is bounded")
	for k := 0; k < maxStrikeBundles+50; k++ {
		s.add("id"+strconv.Itoa(k), "x", t0.Add(time.Duration(k)*time.Second))
	}
	assert.LessOrEqual(t, len(s.m), maxStrikeBundles, "bundles are bounded")
}
