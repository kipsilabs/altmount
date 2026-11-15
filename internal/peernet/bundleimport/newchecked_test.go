package bundleimport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An importer is never built without a collaborator it cannot work without:
// a nil State or Library would panic at first use.
func TestNewCheckedRefusesAMissingRequiredCollaborator(t *testing.T) {
	imp, err := NewChecked(Config{})
	require.Error(t, err)
	assert.Nil(t, imp)
	for _, name := range []string{"Library", "Stager", "Prober", "State", "Headers", "HealthEnabled"} {
		assert.Contains(t, err.Error(), name)
	}
	e := newEnv(t)
	cfg := e.imp.cfg
	cfg.Prober = e.prober
	_, err = NewChecked(cfg)
	require.NoError(t, err, "optional collaborators (Index, Fetcher, Recorder) may be nil")
}
