package model

type MovieMetadata struct {
	Id     int64 `json:"id,omitempty"`
	TmdbId int64 `json:"tmdbId,omitempty"`
}

type MovieFileMetadata struct {
	Id        int64  `json:"id,omitempty"`
	SceneName string `json:"sceneName,omitempty"`
}

type SeriesMetadata struct {
	Id     int64 `json:"id,omitempty"`
	TvdbId int64 `json:"tvdbId,omitempty"`
}

type EpisodeFileMetadata struct {
	Id        int64  `json:"id,omitempty"`
	SceneName string `json:"sceneName,omitempty"`
}

type ArtistMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type AlbumMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type TrackFileMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type AuthorMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type BookMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type BookFileMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type EpisodeMetadata struct {
	Id int64 `json:"id,omitempty"`
}

type WebhookMetadata struct {
	DownloadID   string               `json:"downloadId,omitempty"`
	EventType    string               `json:"eventType,omitempty"`
	InstanceName string               `json:"instanceName,omitempty"`
	Movie        *MovieMetadata       `json:"movie,omitempty"`
	MovieFile    *MovieFileMetadata   `json:"movieFile,omitempty"`
	Series       *SeriesMetadata      `json:"series,omitempty"`
	EpisodeFile  *EpisodeFileMetadata `json:"episodeFile,omitempty"`
	Episodes     []EpisodeMetadata    `json:"episodes,omitempty"`
	Artist       *ArtistMetadata      `json:"artist,omitempty"`
	Album        *AlbumMetadata       `json:"album,omitempty"`
	TrackFile    *TrackFileMetadata   `json:"trackFile,omitempty"`
	Author       *AuthorMetadata      `json:"author,omitempty"`
	Book         *BookMetadata        `json:"book,omitempty"`
	BookFile     *BookFileMetadata    `json:"bookFile,omitempty"`
}
