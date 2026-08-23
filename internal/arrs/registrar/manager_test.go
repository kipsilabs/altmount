package registrar

import (
	"strings"
	"testing"
)

func TestIsAltmountDownloadClient(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{AltmountDownloadClientName, true}, // exact registered name
		{"Altmount", true},                 // common manual name
		{"altmount", true},                 // lowercase
		{"AltMount (SABnzbd)", true},
		{"My AltMount SAB", true},
		{"", false},
		{"qBittorrent", false},
		{"SABnzbd", false},
		{"NZBGet", false},
	}
	for _, tt := range tests {
		if got := IsAltmountDownloadClient(tt.name); got != tt.want {
			t.Errorf("IsAltmountDownloadClient(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestRedactWebhookURLForLog(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "removes sensitive query values",
			url:  "https://altmount.example/base?api_key=api-key-secret&apikey=apikey-secret&token=token-secret#fragment-secret",
			want: "https://altmount.example/base/api/arrs/webhook?apikey=***",
		},
		{
			name: "removes userinfo",
			url:  "https://admin:password-secret@altmount.example/base",
			want: "https://altmount.example/base/api/arrs/webhook?apikey=***",
		},
		{
			name: "preserves IPv6 endpoint and URL base path",
			url:  "https://user:password@[2001:db8::1]:8443/sabnzbd?secret=query#secret-fragment",
			want: "https://[2001:db8::1]:8443/sabnzbd/api/arrs/webhook?apikey=***",
		},
		{
			name: "rejects relative URL",
			url:  "/sabnzbd?secret=query#secret-fragment",
			want: "invalid-url/api/arrs/webhook?apikey=***",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactWebhookURLForLog(tt.url)
			if got != tt.want {
				t.Fatalf("RedactWebhookURLForLog(%q) = %q, want %q", tt.url, got, tt.want)
			}
			for _, secret := range []string{"api-key-secret", "apikey-secret", "token-secret", "fragment-secret", "admin", "password-secret", "secret", "user"} {
				if strings.Contains(got, secret) {
					t.Errorf("redacted URL contains secret %q: %q", secret, got)
				}
			}
		})
	}
}

func TestRedactWebhookURLForLogInvalidURL(t *testing.T) {
	got := RedactWebhookURLForLog("https://%invalid")
	if strings.Contains(got, "%invalid") {
		t.Fatalf("redacted URL contains invalid input: %q", got)
	}
	if got != "invalid-url/api/arrs/webhook?apikey=***" {
		t.Fatalf("RedactWebhookURLForLog returned %q for invalid URL", got)
	}
}
