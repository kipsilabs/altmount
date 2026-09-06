package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProviderStatusResponseDoesNotExposeAuthenticationUsername(t *testing.T) {
	encoded, err := json.Marshal(ProviderStatusResponse{ID: "provider-primary", Host: "news.example.test"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), "username") {
		t.Fatalf("provider status response exposes an authentication field: %s", encoded)
	}
}
