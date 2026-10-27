package api

import (
	"encoding/json"
	"testing"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/stretchr/testify/require"
)

func TestConfigResponsePreservesDeleteGuardSettings(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SABnzbd.DeleteGuardMaxFolders = -1
	cfg.SABnzbd.DeleteGuardWindowMinutes = 25
	data, err := json.Marshal(ToConfigAPIResponse(cfg, ""))
	require.NoError(t, err)
	var roundtrip config.Config
	require.NoError(t, json.Unmarshal(data, &roundtrip))
	require.Equal(t, -1, roundtrip.SABnzbd.DeleteGuardMaxFolders)
	require.Equal(t, 25, roundtrip.SABnzbd.DeleteGuardWindowMinutes)
}
