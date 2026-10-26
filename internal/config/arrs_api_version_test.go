package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSonarrAPIVersionRoundTrip(t *testing.T) {
	for _, version := range []string{"", "auto", "v3", "v5"} {
		t.Run("version_"+version, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Arrs.SonarrInstances = []ArrsInstanceConfig{{Name: "tv", URL: "http://sonarr", APIKey: "secret", APIVersion: version}}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, codec := range []struct {
				name      string
				marshal   func(any) ([]byte, error)
				unmarshal func([]byte, any) error
			}{
				{"json", json.Marshal, json.Unmarshal}, {"yaml", yaml.Marshal, yaml.Unmarshal},
			} {
				data, err := codec.marshal(cfg.Arrs)
				if err != nil {
					t.Fatal(err)
				}
				var restored ArrsConfig
				if err := codec.unmarshal(data, &restored); err != nil {
					t.Fatal(err)
				}
				if got := restored.SonarrInstances[0].APIVersion; got != version {
					t.Fatalf("%s version = %q, want %q", codec.name, got, version)
				}
			}
		})
	}
	var old ArrsInstanceConfig
	if err := yaml.Unmarshal([]byte("name: old\nurl: http://sonarr\napi_key: secret\n"), &old); err != nil {
		t.Fatal(err)
	}
	if old.APIVersion != "" {
		t.Fatalf("old config must default to auto, got %q", old.APIVersion)
	}
}

func TestArrsAPIVersionValidation(t *testing.T) {
	for _, kind := range []string{"sonarr", "radarr", "lidarr", "readarr", "whisparr", "sportarr"} {
		for _, version := range []string{"", "auto", "v3", "v5", "v4", "V5"} {
			t.Run(kind+"_"+version, func(t *testing.T) {
				cfg := DefaultConfig()
				instances := []ArrsInstanceConfig{{Name: "test", APIVersion: version}}
				switch kind {
				case "sonarr":
					cfg.Arrs.SonarrInstances = instances
				case "radarr":
					cfg.Arrs.RadarrInstances = instances
				case "lidarr":
					cfg.Arrs.LidarrInstances = instances
				case "readarr":
					cfg.Arrs.ReadarrInstances = instances
				case "whisparr":
					cfg.Arrs.WhisparrInstances = instances
				case "sportarr":
					cfg.Arrs.SportarrInstances = instances
				}
				err := cfg.Validate()
				valid := version == "" || kind == "sonarr" && (version == "auto" || version == "v3" || version == "v5")
				if valid && err != nil {
					t.Fatal(err)
				}
				if !valid && (err == nil || !strings.Contains(err.Error(), "api_version")) {
					t.Fatalf("want api_version validation error, got %v", err)
				}
			})
		}
	}
}
