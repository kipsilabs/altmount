package bundleimport

import (
	"fmt"
	"strings"

	"github.com/kipsilabs/altmount/internal/importer/rarname"
)

// nameVerdict is what the volume names say about the order of the parts.
type nameVerdict uint8

const (
	// namesSilent: the names do not help (none parses, or every parsed name is a
	// set of its own, as the random names of an obfuscated release are).
	namesSilent nameVerdict = iota
	// namesBind: every part is a volume of one numbered set and the numbers run
	// consecutively in the order of the parts.
	namesBind
	// namesClash: the names form a numbered set (two or more parts share one)
	// and the parts contradict it: a part outside the set, a number out of order
	// or missing.
	namesClash
)

// nameOrder judges the order of the parts of one file by the volume names the
// articles carry (old style .rar/.r00, new style .partNN.rar, numeric .001).
// A decline needs an actual numbering clash within one set: names that each form
// a set of their own say nothing and the order is left to the payload check.
// When two or more parts share a set, every part must belong to it and the
// numbers must run consecutively in the order of the parts, so a swapped,
// dropped or foreign part is refused. The detail says what clashed.
func nameOrder(names []string) (nameVerdict, string) {
	type vol struct {
		scheme rarname.Scheme
		num    int
		key    string
		ok     bool
	}
	vols := make([]vol, len(names))
	type setID struct {
		scheme rarname.Scheme
		key    string
	}
	sets := map[setID]int{}
	parsed := 0
	for i, n := range names {
		if n == "" {
			continue
		}
		scheme, num, ok := rarname.VolumeNumber(n)
		if !ok {
			continue
		}
		key, _ := rarname.SetKey(n)
		vols[i] = vol{scheme, num, key, true}
		sets[setID{scheme, key}]++
		parsed++
	}
	shared := false
	for _, c := range sets {
		shared = shared || c >= 2
	}
	if !shared {
		return namesSilent, ""
	}
	if parsed != len(names) {
		return namesClash, fmt.Sprintf("only %d of %d parts have volume names (%s): the order of the parts cannot be trusted", parsed, len(names), quoteNames(names))
	}
	for i := 1; i < len(vols); i++ {
		a, b := vols[i-1], vols[i]
		switch {
		case a.key != b.key || a.scheme != b.scheme:
			return namesClash, fmt.Sprintf("parts %d and %d belong to different volume sets (%q, %q)", i, i+1, names[i-1], names[i])
		case b.num <= a.num:
			return namesClash, fmt.Sprintf("part %d is volume %q and part %d is volume %q: the parts are not in volume order", i, names[i-1], i+1, names[i])
		case b.num != a.num+1:
			return namesClash, fmt.Sprintf("part %d is volume %q and part %d is volume %q: a volume is missing between them", i, names[i-1], i+1, names[i])
		}
	}
	return namesBind, ""
}

func quoteNames(names []string) string {
	q := make([]string, 0, 3)
	for _, n := range names {
		if len(q) == 3 {
			q = append(q, "...")
			break
		}
		q = append(q, fmt.Sprintf("%q", n))
	}
	return strings.Join(q, ", ")
}
