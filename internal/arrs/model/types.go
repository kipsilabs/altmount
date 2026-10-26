package model

import (
	"fmt"

	"github.com/kipsilabs/altmount/internal/config"
)

var (
	ErrPathMatchFailed         = fmt.Errorf("path match failed")
	ErrEpisodeAlreadySatisfied = fmt.Errorf("item already satisfied by another file in ARR")
	ErrInstanceNotFound        = fmt.Errorf("instance not found")
)

// ConfigInstance represents an arrs instance from configuration
type ConfigInstance struct {
	APIVersion string `json:"api_version,omitempty"`
	Name       string `json:"name"`
	Type       string `json:"type"` // "radarr", "sonarr", "lidarr", "readarr", "whisparr", or "sportarr"
	URL        string `json:"url"`
	APIKey     string `json:"api_key"`
	Category   string `json:"category"`
	Enabled    bool   `json:"enabled"`
}

// ConfigManager interface defines methods needed for configuration management
type ConfigManager interface {
	GetConfig() *config.Config
	GetConfigGetter() config.ConfigGetter
	UpdateConfig(config *config.Config) error
	SaveConfig() error
}
