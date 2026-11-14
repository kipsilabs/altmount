package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInjectedPasswordQueueMetadata(t *testing.T) {
	assert.Nil(t, queueMetadataForPassword(""), "no password, no injection, no metadata (the queue row stays as it was)")
	m := queueMetadataForPassword("typed-secret")
	require.NotNil(t, m)
	assert.Contains(t, *m, "_altmount_pw_injected")
	assert.False(t, strings.Contains(*m, "typed-secret"), "the password is never copied into the metadata")
}
