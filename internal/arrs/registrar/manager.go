package registrar

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/javi11/altmount/internal/arrs/clients"
	"github.com/javi11/altmount/internal/arrs/instances"
	"golift.io/starr"
	"golift.io/starr/lidarr"
	"golift.io/starr/radarr"
	"golift.io/starr/readarr"
	"golift.io/starr/sonarr"
)

// AltmountDownloadClientName is the name AltMount registers itself under as a
// SABnzbd-compatible download client in Radarr/Sonarr/Lidarr/etc. Other code
// (e.g. the queue cleanup worker) imports this to distinguish AltMount's own
// queue items from those owned by other download clients.
const AltmountDownloadClientName = "AltMount (SABnzbd)"

// IsAltmountDownloadClient reports whether a download client name belongs to
// AltMount. AltMount auto-registers under AltmountDownloadClientName, but users
// frequently add the SABnzbd client manually under a different name (e.g.
// "Altmount"), so queue cleanup matches case-insensitively on the "altmount"
// token rather than requiring the exact registered name — otherwise it would
// never recognize, and never clean up, items owned by a renamed client.
func IsAltmountDownloadClient(name string) bool {
	return strings.Contains(strings.ToLower(name), "altmount")
}

type Manager struct {
	instances *instances.Manager
	clients   *clients.Manager
}

func NewManager(instances *instances.Manager, clients *clients.Manager) *Manager {
	return &Manager{
		instances: instances,
		clients:   clients,
	}
}

// EnsureWebhookRegistration ensures that the AltMount webhook is registered in all enabled ARR instances
func (m *Manager) EnsureWebhookRegistration(ctx context.Context, altmountURL string, apiKey string) error {
	allInstances := m.instances.GetAllInstances()
	webhookName := "AltMount Webhook"
	webhookURL := fmt.Sprintf("%s/api/arrs/webhook?apikey=%s", altmountURL, apiKey)
	// Redact all caller-controlled URL credentials when logging; the real webhookURL is still used for registration.
	redactedWebhookURL := RedactWebhookURLForLog(altmountURL)

	slog.InfoContext(ctx, "Ensuring webhook registration in ARR instances", "webhook_url", redactedWebhookURL)

	for _, instance := range allInstances {
		if !instance.Enabled {
			continue
		}

		slog.DebugContext(ctx, "Checking webhook for instance", "instance", instance.Name, "type", instance.Type)

		switch instance.Type {
		case "radarr", "whisparr":
			client, err := m.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Radarr client for webhook check", "instance", instance.Name, "error", err)
				continue
			}

			notifications, err := client.GetNotificationsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Radarr notifications", "instance", instance.Name, "error", err)
				continue
			}

			var existing *radarr.NotificationOutput
			for _, n := range notifications {
				if n.Name == webhookName {
					existing = n
					break
				}
			}

			if existing != nil {
				// Check if update is needed
				currentURL := ""
				for _, f := range existing.Fields {
					if f.Name == "url" {
						if val, ok := f.Value.(string); ok {
							currentURL = val
						}
						break
					}
				}

				if currentURL != webhookURL || !existing.OnGrab || !existing.OnDownload {
					slog.InfoContext(ctx, "Updating Radarr webhook configuration (enabling Grab and Import notifications)", "instance", instance.Name)
					notif := &radarr.NotificationInput{
						ID:                          existing.ID,
						Name:                        webhookName,
						Implementation:              "Webhook",
						ConfigContract:              "WebhookSettings",
						OnGrab:                      true,
						OnDownload:                  true,
						OnUpgrade:                   true,
						OnRename:                    true,
						OnMovieDelete:               true,
						OnMovieFileDelete:           true,
						OnMovieFileDeleteForUpgrade: true,
						Fields: []*starr.FieldInput{
							{Name: "url", Value: webhookURL},
							{Name: "method", Value: "1"}, // 1 = POST
						},
					}
					_, err := client.UpdateNotificationContext(ctx, notif)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Radarr webhook", "instance", instance.Name, "error", err)
					}
				}
			} else {
				notif := &radarr.NotificationInput{
					Name:                        webhookName,
					Implementation:              "Webhook",
					ConfigContract:              "WebhookSettings",
					OnGrab:                      true,
					OnDownload:                  true, // OnImport
					OnUpgrade:                   true,
					OnRename:                    true,
					OnMovieDelete:               true,
					OnMovieFileDelete:           true,
					OnMovieFileDeleteForUpgrade: true,
					Fields: []*starr.FieldInput{
						{Name: "url", Value: webhookURL},
						{Name: "method", Value: "1"}, // 1 = POST
					},
				}
				_, err := client.AddNotificationContext(ctx, notif)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Radarr webhook", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount webhook to Radarr", "instance", instance.Name)
				}
			}

		case "sonarr":
			client, err := m.clients.GetOrCreateSonarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Sonarr client for webhook check", "instance", instance.Name, "error", err)
				continue
			}

			notifications, err := client.GetNotificationsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Sonarr notifications", "instance", instance.Name, "error", err)
				continue
			}

			var existing *sonarr.NotificationOutput
			for _, n := range notifications {
				if n.Name == webhookName {
					existing = n
					break
				}
			}

			if existing != nil {
				// Check if update is needed
				currentURL := ""
				for _, f := range existing.Fields {
					if f.Name == "url" {
						if val, ok := f.Value.(string); ok {
							currentURL = val
						}
						break
					}
				}

				if currentURL != webhookURL || !existing.OnGrab || !existing.OnDownload {
					slog.InfoContext(ctx, "Updating Sonarr webhook configuration (enabling Grab and Import notifications)", "instance", instance.Name)
					notif := &sonarr.NotificationInput{
						ID:                            existing.ID,
						Name:                          webhookName,
						Implementation:                "Webhook",
						ConfigContract:                "WebhookSettings",
						OnGrab:                        true,
						OnDownload:                    true,
						OnUpgrade:                     true,
						OnRename:                      true,
						OnSeriesDelete:                true,
						OnEpisodeFileDelete:           true,
						OnEpisodeFileDeleteForUpgrade: true,
						Fields: []*starr.FieldInput{
							{Name: "url", Value: webhookURL},
							{Name: "method", Value: "1"}, // 1 = POST
						},
					}
					_, err := client.UpdateNotificationContext(ctx, notif)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Sonarr webhook", "instance", instance.Name, "error", err)
					}
				}
			} else {
				notif := &sonarr.NotificationInput{
					Name:                          webhookName,
					Implementation:                "Webhook",
					ConfigContract:                "WebhookSettings",
					OnGrab:                        true,
					OnDownload:                    true, // OnImport
					OnUpgrade:                     true,
					OnRename:                      true,
					OnSeriesDelete:                true,
					OnEpisodeFileDelete:           true,
					OnEpisodeFileDeleteForUpgrade: true,
					Fields: []*starr.FieldInput{
						{Name: "url", Value: webhookURL},
						{Name: "method", Value: "1"}, // 1 = POST
					},
				}
				_, err := client.AddNotificationContext(ctx, notif)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Sonarr webhook", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount webhook to Sonarr", "instance", instance.Name)
				}
			}

		case "lidarr":
			client, err := m.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Lidarr client for webhook check", "instance", instance.Name, "error", err)
				continue
			}

			notifications, err := client.GetNotificationsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Lidarr notifications", "instance", instance.Name, "error", err)
				continue
			}

			var existing *lidarr.NotificationOutput
			for _, n := range notifications {
				if n.Name == webhookName {
					existing = n
					break
				}
			}

			if existing != nil {
				// Check if update is needed
				currentURL := ""
				for _, f := range existing.Fields {
					if f.Name == "url" {
						if val, ok := f.Value.(string); ok {
							currentURL = val
						}
						break
					}
				}

				if currentURL != webhookURL || !existing.OnGrab || !existing.OnReleaseImport {
					slog.InfoContext(ctx, "Updating Lidarr webhook configuration (enabling Grab and Import notifications)", "instance", instance.Name)
					notif := &lidarr.NotificationInput{
						ID:              existing.ID,
						Name:            webhookName,
						Implementation:  "Webhook",
						ConfigContract:  "WebhookSettings",
						OnGrab:          true,
						OnReleaseImport: true,
						OnUpgrade:       true,
						OnRename:        true,
						Fields: []*starr.FieldInput{
							{Name: "url", Value: webhookURL},
							{Name: "method", Value: "1"}, // 1 = POST
						},
					}
					_, err := client.UpdateNotificationContext(ctx, notif)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Lidarr webhook", "instance", instance.Name, "error", err)
					}
				}
			} else {
				notif := &lidarr.NotificationInput{
					Name:            webhookName,
					Implementation:  "Webhook",
					ConfigContract:  "WebhookSettings",
					OnGrab:          true,
					OnReleaseImport: true,
					OnUpgrade:       true,
					OnRename:        true,
					Fields: []*starr.FieldInput{
						{Name: "url", Value: webhookURL},
						{Name: "method", Value: "1"}, // 1 = POST
					},
				}
				_, err := client.AddNotificationContext(ctx, notif)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Lidarr webhook", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount webhook to Lidarr", "instance", instance.Name)
				}
			}

		case "readarr":
			client, err := m.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Readarr client for webhook check", "instance", instance.Name, "error", err)
				continue
			}

			notifications, err := client.GetNotificationsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Readarr notifications", "instance", instance.Name, "error", err)
				continue
			}

			var existing *readarr.NotificationOutput
			for _, n := range notifications {
				if n.Name == webhookName {
					existing = n
					break
				}
			}

			if existing != nil {
				// Check if update is needed
				currentURL := ""
				for _, f := range existing.Fields {
					if f.Name == "url" {
						if val, ok := f.Value.(string); ok {
							currentURL = val
						}
						break
					}
				}

				if currentURL != webhookURL || !existing.OnGrab || !existing.OnReleaseImport {
					slog.InfoContext(ctx, "Updating Readarr webhook configuration (enabling Grab and Import notifications)", "instance", instance.Name)
					notif := &readarr.NotificationInput{
						ID:                         existing.ID,
						Name:                       webhookName,
						Implementation:             "Webhook",
						ConfigContract:             "WebhookSettings",
						OnGrab:                     true,
						OnReleaseImport:            true,
						OnUpgrade:                  true,
						OnRename:                   true,
						OnAuthorDelete:             true,
						OnBookDelete:               true,
						OnBookFileDelete:           true,
						OnBookFileDeleteForUpgrade: true,
						Fields: []*starr.FieldInput{
							{Name: "url", Value: webhookURL},
							{Name: "method", Value: "1"}, // 1 = POST
						},
					}
					_, err := client.UpdateNotificationContext(ctx, notif)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Readarr webhook", "instance", instance.Name, "error", err)
					}
				}
			} else {
				notif := &readarr.NotificationInput{
					Name:                       webhookName,
					Implementation:             "Webhook",
					ConfigContract:             "WebhookSettings",
					OnGrab:                     true,
					OnReleaseImport:            true,
					OnUpgrade:                  true,
					OnRename:                   true,
					OnAuthorDelete:             true,
					OnBookDelete:               true,
					OnBookFileDelete:           true,
					OnBookFileDeleteForUpgrade: true,
					Fields: []*starr.FieldInput{
						{Name: "url", Value: webhookURL},
						{Name: "method", Value: "1"}, // 1 = POST
					},
				}
				_, err := client.AddNotificationContext(ctx, notif)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Readarr webhook", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount webhook to Readarr", "instance", instance.Name)
				}
			}
		}
	}

	return nil
}

// RedactWebhookURLForLog returns useful endpoint context without exposing URL
// credentials, query parameters, or fragments in logs.
func RedactWebhookURLForLog(altmountURL string) string {
	parsed, err := url.Parse(altmountURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "invalid-url/api/arrs/webhook?apikey=***"
	}

	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""

	return strings.TrimRight(parsed.String(), "/") + "/api/arrs/webhook?apikey=***"
}

// EnsureDownloadClientRegistration ensures that AltMount is registered as a SABnzbd download client in all enabled ARR instances
func (m *Manager) EnsureDownloadClientRegistration(ctx context.Context, altmountHost string, altmountPort int, urlBase string, apiKey string) error {
	allInstances := m.instances.GetAllInstances()
	clientName := AltmountDownloadClientName

	slog.InfoContext(ctx, "Ensuring AltMount download client registration in ARR instances",
		"host", altmountHost,
		"port", altmountPort,
		"url_base", urlBase)

	for _, instance := range allInstances {
		if !instance.Enabled {
			continue
		}

		slog.DebugContext(ctx, "Checking download client for instance", "instance", instance.Name, "type", instance.Type)

		switch instance.Type {
		case "radarr", "whisparr":
			client, err := m.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Radarr client for download client check", "instance", instance.Name, "error", err)
				continue
			}

			clients, err := client.GetDownloadClientsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Radarr download clients", "instance", instance.Name, "error", err)
				continue
			}

			var existing *radarr.DownloadClientOutput
			for _, c := range clients {
				if c.Name == clientName {
					existing = c
					break
				}
			}

			if existing != nil {
				// Update if API key or Host changed
				currentKey := ""
				currentHost := ""
				for _, f := range existing.Fields {
					if f.Name == "apiKey" {
						if val, ok := f.Value.(string); ok {
							currentKey = val
						}
					}
					if f.Name == "host" {
						if val, ok := f.Value.(string); ok {
							currentHost = val
						}
					}
				}

				if currentKey != apiKey || currentHost != altmountHost {
					slog.InfoContext(ctx, "Updating Radarr download client API key/Host", "instance", instance.Name)
					category := instance.Category
					if category == "" {
						slog.WarnContext(ctx, "No category found in configuration for instance, using empty string", "instance", instance.Name)
					}
					dc := &radarr.DownloadClientInput{
						ID:                       existing.ID,
						Name:                     clientName,
						Implementation:           "SABnzbd",
						ConfigContract:           "SABnzbdSettings",
						Enable:                   true,
						RemoveCompletedDownloads: true,
						RemoveFailedDownloads:    true,
						Priority:                 1,
						Protocol:                 "Usenet",
						Fields: []*starr.FieldInput{
							{Name: "host", Value: altmountHost},
							{Name: "port", Value: altmountPort},
							{Name: "urlBase", Value: urlBase},
							{Name: "apiKey", Value: apiKey},
							{Name: "movieCategory", Value: category},
							{Name: "useSsl", Value: false},
						},
					}
					_, err := client.UpdateDownloadClientContext(ctx, dc, true)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update download client", "instance", instance.Name, "error", err)
					}
				}
			} else {
				category := instance.Category
				if category == "" {
					slog.WarnContext(ctx, "No category found in configuration for instance, using empty string", "instance", instance.Name)
				}
				dc := &radarr.DownloadClientInput{
					Name:                     clientName,
					Implementation:           "SABnzbd",
					ConfigContract:           "SABnzbdSettings",
					Enable:                   true,
					RemoveCompletedDownloads: true,
					RemoveFailedDownloads:    true,
					Priority:                 1,
					Protocol:                 "Usenet",
					Fields: []*starr.FieldInput{
						{Name: "host", Value: altmountHost},
						{Name: "port", Value: altmountPort},
						{Name: "urlBase", Value: urlBase},
						{Name: "apiKey", Value: apiKey},
						{Name: "movieCategory", Value: category},
						{Name: "useSsl", Value: false},
					},
				}
				_, err := client.AddDownloadClientContext(ctx, dc)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add download client to "+instance.Type, "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount download client to "+instance.Type, "instance", instance.Name, "category", category)
				}
			}

		case "sonarr":
			client, err := m.clients.GetOrCreateSonarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Sonarr client for download client check", "instance", instance.Name, "error", err)
				continue
			}

			clients, err := client.GetDownloadClientsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Sonarr download clients", "instance", instance.Name, "error", err)
				continue
			}

			var existing *sonarr.DownloadClientOutput
			for _, c := range clients {
				if c.Name == clientName {
					existing = c
					break
				}
			}

			if existing != nil {
				// Update if API key or Host changed
				currentKey := ""
				currentHost := ""
				for _, f := range existing.Fields {
					if f.Name == "apiKey" {
						if val, ok := f.Value.(string); ok {
							currentKey = val
						}
					}
					if f.Name == "host" {
						if val, ok := f.Value.(string); ok {
							currentHost = val
						}
					}
				}

				if currentKey != apiKey || currentHost != altmountHost {
					slog.InfoContext(ctx, "Updating Sonarr download client API key/Host", "instance", instance.Name)
					category := instance.Category
					if category == "" {
						slog.WarnContext(ctx, "No category found in configuration for instance, using empty string", "instance", instance.Name)
					}
					dc := &sonarr.DownloadClientInput{
						ID:                       existing.ID,
						Name:                     clientName,
						Implementation:           "SABnzbd",
						ConfigContract:           "SABnzbdSettings",
						Enable:                   true,
						RemoveCompletedDownloads: true,
						RemoveFailedDownloads:    true,
						Priority:                 1,
						Protocol:                 "Usenet",
						Fields: []*starr.FieldInput{
							{Name: "host", Value: altmountHost},
							{Name: "port", Value: altmountPort},
							{Name: "urlBase", Value: urlBase},
							{Name: "apiKey", Value: apiKey},
							{Name: "tvCategory", Value: category},
							{Name: "useSsl", Value: false},
						},
					}
					_, err := client.UpdateDownloadClientContext(ctx, dc, true)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update download client", "instance", instance.Name, "error", err)
					}
				}
			} else {
				category := instance.Category
				if category == "" {
					slog.WarnContext(ctx, "No category found in configuration for instance, using empty string", "instance", instance.Name)
				}
				dc := &sonarr.DownloadClientInput{
					Name:                     clientName,
					Implementation:           "SABnzbd",
					ConfigContract:           "SABnzbdSettings",
					Enable:                   true,
					RemoveCompletedDownloads: true,
					RemoveFailedDownloads:    true,
					Priority:                 1,
					Protocol:                 "Usenet",
					Fields: []*starr.FieldInput{
						{Name: "host", Value: altmountHost},
						{Name: "port", Value: altmountPort},
						{Name: "urlBase", Value: urlBase},
						{Name: "apiKey", Value: apiKey},
						{Name: "tvCategory", Value: category},
						{Name: "useSsl", Value: false},
					},
				}
				_, err := client.AddDownloadClientContext(ctx, dc)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Sonarr download client", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount download client to Sonarr", "instance", instance.Name)
				}
			}

		case "lidarr":
			client, err := m.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Lidarr client for download client check", "instance", instance.Name, "error", err)
				continue
			}

			clients, err := client.GetDownloadClientsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Lidarr download clients", "instance", instance.Name, "error", err)
				continue
			}

			var existing *lidarr.DownloadClientOutput
			for _, c := range clients {
				if c.Name == clientName {
					existing = c
					break
				}
			}

			if existing != nil {
				// Update if API key or Host changed
				currentKey := ""
				currentHost := ""
				for _, f := range existing.Fields {
					if f.Name == "apiKey" {
						if val, ok := f.Value.(string); ok {
							currentKey = val
						}
					}
					if f.Name == "host" {
						if val, ok := f.Value.(string); ok {
							currentHost = val
						}
					}
				}

				if currentKey != apiKey || currentHost != altmountHost {
					slog.InfoContext(ctx, "Updating Lidarr download client API key/Host", "instance", instance.Name)
					category := instance.Category
					if category == "" {
						category = ""
					}
					dc := &lidarr.DownloadClientInput{
						ID:                       existing.ID,
						Name:                     clientName,
						Implementation:           "SABnzbd",
						ConfigContract:           "SABnzbdSettings",
						Enable:                   true,
						RemoveCompletedDownloads: true,
						RemoveFailedDownloads:    true,
						Priority:                 1,
						Protocol:                 "Usenet",
						Fields: []*starr.FieldInput{
							{Name: "host", Value: altmountHost},
							{Name: "port", Value: altmountPort},
							{Name: "urlBase", Value: urlBase},
							{Name: "apiKey", Value: apiKey},
							{Name: "musicCategory", Value: category},
							{Name: "useSsl", Value: false},
						},
					}
					_, err := client.UpdateDownloadClientContext(ctx, dc, true)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Lidarr download client", "instance", instance.Name, "error", err)
					}
				}
			} else {
				category := instance.Category
				if category == "" {
					category = ""
				}
				dc := &lidarr.DownloadClientInput{
					Name:                     clientName,
					Implementation:           "SABnzbd",
					ConfigContract:           "SABnzbdSettings",
					Enable:                   true,
					RemoveCompletedDownloads: true,
					RemoveFailedDownloads:    true,
					Priority:                 1,
					Protocol:                 "Usenet",
					Fields: []*starr.FieldInput{
						{Name: "host", Value: altmountHost},
						{Name: "port", Value: altmountPort},
						{Name: "urlBase", Value: urlBase},
						{Name: "apiKey", Value: apiKey},
						{Name: "musicCategory", Value: category},
						{Name: "useSsl", Value: false},
					},
				}
				_, err := client.AddDownloadClientContext(ctx, dc)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Lidarr download client", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount download client to Lidarr", "instance", instance.Name)
				}
			}

		case "readarr":
			client, err := m.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to create Readarr client for download client check", "instance", instance.Name, "error", err)
				continue
			}

			clients, err := client.GetDownloadClientsContext(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to get Readarr download clients", "instance", instance.Name, "error", err)
				continue
			}

			var existing *readarr.DownloadClientOutput
			for _, c := range clients {
				if c.Name == clientName {
					existing = c
					break
				}
			}

			if existing != nil {
				// Update if API key or Host changed
				currentKey := ""
				currentHost := ""
				for _, f := range existing.Fields {
					if f.Name == "apiKey" {
						if val, ok := f.Value.(string); ok {
							currentKey = val
						}
					}
					if f.Name == "host" {
						if val, ok := f.Value.(string); ok {
							currentHost = val
						}
					}
				}

				if currentKey != apiKey || currentHost != altmountHost {
					slog.InfoContext(ctx, "Updating Readarr download client API key/Host", "instance", instance.Name)
					category := instance.Category
					if category == "" {
						category = ""
					}
					dc := &readarr.DownloadClientInput{
						ID:             existing.ID,
						Name:           clientName,
						Implementation: "SABnzbd",
						ConfigContract: "SABnzbdSettings",
						Enable:         true,
						Priority:       1,
						Protocol:       "Usenet",
						Fields: []*starr.FieldInput{
							{Name: "host", Value: altmountHost},
							{Name: "port", Value: altmountPort},
							{Name: "urlBase", Value: urlBase},
							{Name: "apiKey", Value: apiKey},
							{Name: "musicCategory", Value: category},
							{Name: "bookCategory", Value: category},
							{Name: "useSsl", Value: false},
						},
					}
					_, err := client.UpdateDownloadClientContext(ctx, dc, true)
					if err != nil {
						slog.ErrorContext(ctx, "Failed to update Readarr download client", "instance", instance.Name, "error", err)
					}
				}
			} else {
				category := instance.Category
				if category == "" {
					category = ""
				}
				dc := &readarr.DownloadClientInput{
					Name:           clientName,
					Implementation: "SABnzbd",
					ConfigContract: "SABnzbdSettings",
					Enable:         true,
					Priority:       1,
					Protocol:       "Usenet",
					Fields: []*starr.FieldInput{
						{Name: "host", Value: altmountHost},
						{Name: "port", Value: altmountPort},
						{Name: "urlBase", Value: urlBase},
						{Name: "apiKey", Value: apiKey},
						{Name: "musicCategory", Value: category},
						{Name: "bookCategory", Value: category},
						{Name: "useSsl", Value: false},
					},
				}
				_, err := client.AddDownloadClientContext(ctx, dc)
				if err != nil {
					slog.ErrorContext(ctx, "Failed to add Readarr download client", "instance", instance.Name, "error", err)
				} else {
					slog.InfoContext(ctx, "Added AltMount download client to Readarr", "instance", instance.Name)
				}
			}
		}
	}

	return nil
}

// TestDownloadClientRegistration tests the connection from ARR instances back to AltMount
func (m *Manager) TestDownloadClientRegistration(ctx context.Context, altmountHost string, altmountPort int, urlBase string, apiKey string) (map[string]string, error) {
	allInstances := m.instances.GetAllInstances()
	results := make(map[string]string)

	for _, instance := range allInstances {
		if !instance.Enabled {
			continue
		}

		var testErr error
		switch instance.Type {
		case "radarr", "whisparr":
			client, err := m.clients.GetOrCreateRadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				results[instance.Name] = fmt.Sprintf("Failed to create client: %v", err)
				continue
			}

			category := instance.Category
			if category == "" {
				category = "movies"
			}

			dc := &radarr.DownloadClientInput{
				Name:                     "AltMount Test",
				Implementation:           "SABnzbd",
				ConfigContract:           "SABnzbdSettings",
				Enable:                   true,
				RemoveCompletedDownloads: true,
				RemoveFailedDownloads:    true,
				Priority:                 1,
				Protocol:                 "Usenet",
				Fields: []*starr.FieldInput{
					{Name: "host", Value: altmountHost},
					{Name: "port", Value: altmountPort},
					{Name: "urlBase", Value: urlBase},
					{Name: "apiKey", Value: apiKey},
					{Name: "movieCategory", Value: category},
					{Name: "useSsl", Value: false},
				},
			}
			testErr = client.TestDownloadClientContext(ctx, dc)

		case "sonarr":
			client, err := m.clients.GetOrCreateSonarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				results[instance.Name] = fmt.Sprintf("Failed to create client: %v", err)
				continue
			}

			category := instance.Category
			if category == "" {
				category = "tv"
			}

			dc := &sonarr.DownloadClientInput{
				Name:                     "AltMount Test",
				Implementation:           "SABnzbd",
				ConfigContract:           "SABnzbdSettings",
				Enable:                   true,
				RemoveCompletedDownloads: true,
				RemoveFailedDownloads:    true,
				Priority:                 1,
				Protocol:                 "Usenet",
				Fields: []*starr.FieldInput{
					{Name: "host", Value: altmountHost},
					{Name: "port", Value: altmountPort},
					{Name: "urlBase", Value: urlBase},
					{Name: "apiKey", Value: apiKey},
					{Name: "tvCategory", Value: category},
					{Name: "useSsl", Value: false},
				},
			}
			testErr = client.TestDownloadClientContext(ctx, dc)

		case "lidarr":
			client, err := m.clients.GetOrCreateLidarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				results[instance.Name] = fmt.Sprintf("Failed to create client: %v", err)
				continue
			}

			category := instance.Category
			if category == "" {
				category = ""
			}

			dc := &lidarr.DownloadClientInput{
				Name:                     "AltMount Test",
				Implementation:           "SABnzbd",
				ConfigContract:           "SABnzbdSettings",
				Enable:                   true,
				RemoveCompletedDownloads: true,
				RemoveFailedDownloads:    true,
				Priority:                 1,
				Protocol:                 "Usenet",
				Fields: []*starr.FieldInput{
					{Name: "host", Value: altmountHost},
					{Name: "port", Value: altmountPort},
					{Name: "urlBase", Value: urlBase},
					{Name: "apiKey", Value: apiKey},
					{Name: "musicCategory", Value: category},
					{Name: "useSsl", Value: false},
				},
			}
			testErr = client.TestDownloadClientContext(ctx, dc)

		case "readarr":
			client, err := m.clients.GetOrCreateReadarrClient(instance.Name, instance.URL, instance.APIKey)
			if err != nil {
				results[instance.Name] = fmt.Sprintf("Failed to create client: %v", err)
				continue
			}

			category := instance.Category
			if category == "" {
				category = ""
			}

			dc := &readarr.DownloadClientInput{
				Name:           "AltMount Test",
				Implementation: "SABnzbd",
				ConfigContract: "SABnzbdSettings",
				Enable:         true,
				Priority:       1,
				Protocol:       "Usenet",
				Fields: []*starr.FieldInput{
					{Name: "host", Value: altmountHost},
					{Name: "port", Value: altmountPort},
					{Name: "urlBase", Value: urlBase},
					{Name: "apiKey", Value: apiKey},
					{Name: "musicCategory", Value: category},
					{Name: "bookCategory", Value: category},
					{Name: "useSsl", Value: false},
				},
			}
			testErr = client.TestDownloadClientContext(ctx, dc)
		}

		if testErr != nil {
			results[instance.Name] = testErr.Error()
		} else {
			results[instance.Name] = "OK"
		}
	}

	return results, nil
}
