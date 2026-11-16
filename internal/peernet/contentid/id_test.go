package contentid

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseID(t *testing.T) {
	valid := strings.Repeat("ab", 32)
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"valid lower", valid, false},
		{"valid upper", strings.ToUpper(valid), false},
		{"empty", "", true},
		{"short", valid[:62], true},
		{"long", valid + "00", true},
		{"non hex", strings.Repeat("zz", 32), true},
		{"whitespace", " " + valid[:62] + " ", true},
		{"prefixed", "0x" + valid[:62], true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseID(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, id.IsZero())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, valid, id.String())
		})
	}
}

func TestIDStringRoundTrip(t *testing.T) {
	var id ID
	for i := range id {
		id[i] = byte(i)
	}
	got, err := ParseID(id.String())
	require.NoError(t, err)
	assert.Equal(t, id, got)
	assert.Len(t, id.String(), 64)
	assert.False(t, id.IsZero())
	assert.True(t, ID{}.IsZero())
}
