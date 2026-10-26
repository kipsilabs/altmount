// Endpoint implementations adapted from golift.io/starr v1.2.0.
/*
MIT License

Copyright (c) 2019-2024 Go Lift - Building Strong Go Tools
Copyright (c) 2018 David Newhall II

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package clients

// These explicit native endpoints share starr DTOs where Sonarr v5 retains the schema.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"

	"golift.io/starr"
	"golift.io/starr/sonarr"
)

func (s *nativeSonarr) GetSeriesEpisodesContext(ctx context.Context, getEpisode *sonarr.GetEpisode) ([]*sonarr.Episode, error) {
	var output []*sonarr.Episode

	params := make(url.Values)

	if getEpisode.SeriesID > 0 {
		params.Set("seriesId", starr.Str(getEpisode.SeriesID))
	}

	if getEpisode.SeasonNumber > 0 {
		params.Set("seasonNumber", starr.Str(getEpisode.SeasonNumber))
	}

	for _, id := range getEpisode.EpisodeIDs {
		params.Add("episodeIds", starr.Str(id))
	}

	if getEpisode.EpisodeFileID > 0 {
		params.Set("episodeFileId", starr.Str(getEpisode.EpisodeFileID))
	}

	if getEpisode.IncludeImages {
		params.Add("includeSubresources", "images")
	}

	req := starr.Request{URI: "/v5/episode", Query: params}

	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) GetEpisodeByIDContext(ctx context.Context, episodeID int64) (*sonarr.Episode, error) {
	var output sonarr.Episode

	req := starr.Request{URI: path.Join("/v5/episode", starr.Str(episodeID))}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) MonitorEpisodeContext(ctx context.Context, episodeIDs []int64, monitor bool) ([]*sonarr.Episode, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(&struct {
		E []int64 `json:"episodeIds"`
		M bool    `json:"monitored"`
	}{E: episodeIDs, M: monitor}); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/episode", err)
	}

	var output []*sonarr.Episode

	req := starr.Request{URI: path.Join("/v5/episode", "monitor"), Body: &body}
	if err := s.PutInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Put(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) GetRootFoldersContext(ctx context.Context) ([]*sonarr.RootFolder, error) {
	var output []*sonarr.RootFolder

	req := starr.Request{URI: "/v5/rootfolder"}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) GetEpisodeFilesContext(ctx context.Context, episodeFileIDs ...int64) ([]*sonarr.EpisodeFile, error) {
	req := starr.Request{URI: "/v5/episodefile", Query: make(url.Values)}
	for _, id := range episodeFileIDs {
		req.Query.Add("episodeFileIds", starr.Str(id))
	}

	var output []*sonarr.EpisodeFile
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) GetSeriesEpisodeFilesContext(ctx context.Context, seriesID int64) ([]*sonarr.EpisodeFile, error) {
	var output []*sonarr.EpisodeFile

	req := starr.Request{URI: "/v5/episodefile", Query: make(url.Values)}
	req.Query.Add("seriesId", starr.Str(seriesID))

	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) DeleteEpisodeFileContext(ctx context.Context, episodeFileID int64) error {
	req := starr.Request{URI: path.Join("/v5/episodefile", starr.Str(episodeFileID))}
	if err := s.DeleteAny(ctx, req); err != nil {
		return fmt.Errorf("api.Delete(%s): %w", &req, err)
	}

	return nil
}

func (s *nativeSonarr) GetCommandsContext(ctx context.Context) ([]*sonarr.CommandResponse, error) {
	var output []*sonarr.CommandResponse

	req := starr.Request{URI: "/v5/command"}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) SendCommandContext(ctx context.Context, cmd *sonarr.CommandRequest) (*sonarr.CommandResponse, error) {
	var output sonarr.CommandResponse

	if cmd == nil || cmd.Name == "" {
		return &output, nil
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(cmd); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/command", err)
	}

	req := starr.Request{URI: "/v5/command", Body: &body}
	if err := s.PostInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Post(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetCommandStatusContext(ctx context.Context, commandID int64) (*sonarr.CommandResponse, error) {
	var output sonarr.CommandResponse

	if commandID == 0 {
		return &output, nil
	}

	req := starr.Request{URI: path.Join("/v5/command", starr.Str(commandID))}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetDownloadClientsContext(ctx context.Context) ([]*sonarr.DownloadClientOutput, error) {
	var output []*sonarr.DownloadClientOutput

	req := starr.Request{URI: "/v5/downloadclient"}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) AddDownloadClientContext(ctx context.Context,
	client *sonarr.DownloadClientInput,
) (*sonarr.DownloadClientOutput, error) {
	var (
		output sonarr.DownloadClientOutput
		body   bytes.Buffer
	)

	client.ID = 0
	if err := json.NewEncoder(&body).Encode(client); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/downloadclient", err)
	}

	req := starr.Request{URI: "/v5/downloadclient", Body: &body, Query: url.Values{"skipTesting": []string{"true"}}}
	if err := s.PostInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Post(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) TestDownloadClientContext(ctx context.Context, client *sonarr.DownloadClientInput) error {
	var output interface{} // any ok

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(client); err != nil {
		return fmt.Errorf("json.Marshal(%s): %w", "/v5/downloadclient", err)
	}

	req := starr.Request{URI: path.Join("/v5/downloadclient", "test"), Body: &body}
	if err := s.PostInto(ctx, req, &output); err != nil {
		return fmt.Errorf("api.Post(%s): %w", &req, err)
	}

	return nil
}

func (s *nativeSonarr) UpdateDownloadClientContext(ctx context.Context,
	client *sonarr.DownloadClientInput,
	force bool,
) (*sonarr.DownloadClientOutput, error) {
	var output sonarr.DownloadClientOutput

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(client); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/downloadclient", err)
	}

	req := starr.Request{
		URI:   path.Join("/v5/downloadclient", starr.Str(client.ID)),
		Body:  &body,
		Query: url.Values{"skipTesting": []string{starr.Str(force)}},
	}
	if err := s.PutInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Put(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetSystemStatusContext(ctx context.Context) (*sonarr.SystemStatus, error) {
	var output sonarr.SystemStatus

	req := starr.Request{URI: path.Join("/v5/system", "status")}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetAllSeriesContext(ctx context.Context) ([]*sonarr.Series, error) {
	var output []*sonarr.Series
	if err := s.GetInto(ctx, starr.Request{URI: "/v5/series"}, &output); err != nil {
		return nil, err
	}
	return output, nil
}

func (s *nativeSonarr) GetSeriesByIDContext(ctx context.Context, seriesID int64) (*sonarr.Series, error) {
	var output sonarr.Series

	req := starr.Request{URI: path.Join("/v5/series", starr.Str(seriesID))}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetNotificationsContext(ctx context.Context) ([]*sonarr.NotificationOutput, error) {
	var output []*sonarr.NotificationOutput

	req := starr.Request{URI: "/v5/connection"}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return output, nil
}

func (s *nativeSonarr) AddNotificationContext(ctx context.Context, client *sonarr.NotificationInput) (*sonarr.NotificationOutput, error) {
	var output sonarr.NotificationOutput

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(client); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/connection", err)
	}

	req := starr.Request{URI: "/v5/connection", Body: &body}
	if err := s.PostInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Post(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) UpdateNotificationContext(ctx context.Context,
	client *sonarr.NotificationInput,
) (*sonarr.NotificationOutput, error) {
	var output sonarr.NotificationOutput

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(client); err != nil {
		return nil, fmt.Errorf("json.Marshal(%s): %w", "/v5/connection", err)
	}

	req := starr.Request{URI: path.Join("/v5/connection", starr.Str(client.ID)), Body: &body}
	if err := s.PutInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Put(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) GetHistoryPageContext(ctx context.Context, params *starr.PageReq) (*sonarr.History, error) {
	var output sonarr.History

	query := params.Params()
	if series := query.Get("seriesId"); series != "" {
		query.Del("seriesId")
		query.Set("seriesIds", series)
	}
	req := starr.Request{URI: "/v5/history", Query: query}
	if err := s.GetInto(ctx, req, &output); err != nil {
		return nil, fmt.Errorf("api.Get(%s): %w", &req, err)
	}

	return &output, nil
}

func (s *nativeSonarr) FailContext(ctx context.Context, historyID int64) error {
	if historyID < 1 {
		return fmt.Errorf("%w: invalid history ID: %d", starr.ErrRequestError, historyID)
	}

	var output interface{} // any ok

	// Strangely uses a POST without a payload.
	req := starr.Request{URI: path.Join("/v5/history", "failed", starr.Str(historyID))}
	if err := s.PostInto(ctx, req, &output); err != nil {
		return fmt.Errorf("api.Post(%s): %w", &req, err)
	}

	return nil
}
