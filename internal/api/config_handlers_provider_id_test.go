package api

import (
	"testing"

	"github.com/javi11/altmount/internal/config"
)

func TestNextProviderIDSkipsCollisions(t *testing.T) {
	existing := []config.ProviderConfig{
		{ID: "provider_2"},
	}

	got := nextProviderID(existing)

	if got == "provider_2" {
		t.Fatalf("nextProviderID() = %q, collides with an existing provider", got)
	}
	for _, p := range existing {
		if got == p.ID {
			t.Fatalf("nextProviderID() = %q, collides with existing provider %q", got, p.ID)
		}
	}
}

func TestNextProviderIDWithNoProviders(t *testing.T) {
	if got := nextProviderID(nil); got != "provider_1" {
		t.Fatalf("nextProviderID(nil) = %q, want %q", got, "provider_1")
	}
}
