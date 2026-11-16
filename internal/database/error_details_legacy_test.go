package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerMarkers_PreserveLegacyDetails(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	cases := map[string][2]string{
		"bare json string": {`"disk went away"`, "disk went away"},
		"json array":       {`["a","b"]`, `[\"a\",\"b\"]`},
		"plain text":       {`legacy text`, "legacy text"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			raw := c[0]
			got := WithPeerDeferral(&raw, at)
			require.NotNil(t, got)
			assert.True(t, HasPeerDeferral(got))
			assert.Contains(t, *got, c[1], "legacy content must survive")
		})
	}

	t.Run("object with unknown keys", func(t *testing.T) {
		raw := `{"error":"metadata_read_failed","message":"boom"}`
		got := WithPeerDeferral(&raw, at)
		assert.JSONEq(t, `{"error":"metadata_read_failed","message":"boom","peer_deferred_at":"2026-11-12T10:00:00Z"}`, *got)
	})

	t.Run("carry keeps legacy target details", func(t *testing.T) {
		from := WithPeerDeferral(nil, at)
		target := `["x"]`
		got := CarryPeerDeferral(&target, from)
		assert.True(t, HasPeerDeferral(got))
		assert.JSONEq(t, `{"legacy":"[\"x\"]","peer_deferred_at":"2026-11-12T10:00:00Z"}`, *got)
	})
}

func TestPeerReductionMarker(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	assert.False(t, HasPeerReduction(nil))
	marked := WithPeerReduction(strPtr(`{"content_verification":"passed"}`), at)
	assert.True(t, HasPeerReduction(marked))
	assert.Contains(t, *marked, "passed")
	assert.False(t, HasPeerDeferral(marked))

	assert.False(t, HasFailureDetails(nil))
	assert.False(t, HasFailureDetails(marked))
	assert.True(t, HasFailureDetails(strPtr(`{"error_type":"missing_segments"}`)))
}
