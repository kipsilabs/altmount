package bundle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartsOf(t *testing.T) {
	f := volumeFile(threeVolumes)
	parts := PartsOf(&f)
	if assert.Len(t, parts, 3) {
		assert.Len(t, parts[0], 4)
		assert.Len(t, parts[2], 3)
		assert.Equal(t, f.Size, Usable(parts[0])+Usable(parts[1])+Usable(parts[2]))
	}
	f.Parts = nil
	assert.Len(t, PartsOf(&f), 1, "no parts: the table is one posted file")
	assert.Empty(t, PartsOf(&File{}))
}
