package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

// V5's ProviderResource.Tags is a non-nullable HashSet<int>. Explicit null
// fails MVC model validation before the provider's connection test is reached.
func TestNativeDownloadClientTags(t *testing.T) {
	for _, operation := range []string{"create", "update", "test"} {
		for _, tc := range []struct {
			name string
			tags []int
		}{{"nil", nil}, {"empty", []int{}}, {"assigned", []int{7, 11}}} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				tags := tc.tags
				called := 0
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called++
					var payload struct {
						Tags []int `json:"tags"`
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Tags == nil {
						http.Error(w, `{"errors":{"Tags":["The Tags field is required."]}}`, http.StatusBadRequest)
						return
					}
					require.Equal(t, append([]int{}, tags...), payload.Tags)
					w.Header().Set("Content-Type", "application/json")
					if operation == "test" {
						w.WriteHeader(http.StatusNoContent)
						return
					}
					_, _ = w.Write([]byte(`{"id":3}`))
				}))
				defer srv.Close()
				client := newNativeSonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
				input := &sonarr.DownloadClientInput{
					ID: 3, Name: "AltMount", Implementation: "SABnzbd",
					ConfigContract: "SABnzbdSettings", Enable: true,
					Priority: 1, Protocol: "Usenet", Tags: tags,
					Fields: []*starr.FieldInput{{Name: "host", Value: "altmount"}},
				}
				var err error
				switch operation {
				case "create":
					_, err = client.AddDownloadClientContext(context.Background(), input)
				case "update":
					_, err = client.UpdateDownloadClientContext(context.Background(), input, true)
				case "test":
					err = client.TestDownloadClientContext(context.Background(), input)
				}
				require.NoError(t, err)
				require.Equal(t, 1, called)
				require.Equal(t, tags, input.Tags, "serialization must not mutate caller tags")
			})
		}
	}
}

func TestNativeDownloadClientTestPreservesRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"Connection failed"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	client := newNativeSonarr(&starr.Config{URL: srv.URL, Client: srv.Client()})
	err := client.TestDownloadClientContext(context.Background(), &sonarr.DownloadClientInput{Name: "AltMount"})
	var requestErr *starr.ReqError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, http.StatusBadRequest, requestErr.Code)
}
