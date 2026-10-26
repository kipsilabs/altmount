package clients

import (
	"context"

	"github.com/kipsilabs/altmount/internal/arrs/model"
	"golift.io/starr"
	"golift.io/starr/sonarr"
)

// SonarrClient fixes one selected API adapter for the lifetime of an operation.
type SonarrClient interface {
	starr.APIer
	APIVersion() string
	QueueGroups(context.Context) ([]model.SonarrDownloadGroup, error)
	RemoveDownload(context.Context, model.SonarrDownloadGroup, model.SonarrRemovalOptions) error
	GetQueueContext(context.Context, int, int) (*sonarr.Queue, error)
	GetQueue(int, int) (*sonarr.Queue, error)
	DeleteQueueContext(context.Context, int64, *starr.QueueDeleteOpts) error
	GetSystemStatus() (*sonarr.SystemStatus, error)
	GetSeriesEpisodesContext(ctx context.Context, getEpisode *sonarr.GetEpisode) ([]*sonarr.Episode, error)
	GetEpisodeByIDContext(ctx context.Context, episodeID int64) (*sonarr.Episode, error)
	MonitorEpisodeContext(ctx context.Context, episodeIDs []int64, monitor bool) ([]*sonarr.Episode, error)
	GetRootFoldersContext(ctx context.Context) ([]*sonarr.RootFolder, error)
	GetEpisodeFilesContext(ctx context.Context, episodeFileIDs ...int64) ([]*sonarr.EpisodeFile, error)
	GetSeriesEpisodeFilesContext(ctx context.Context, seriesID int64) ([]*sonarr.EpisodeFile, error)
	DeleteEpisodeFileContext(ctx context.Context, episodeFileID int64) error
	GetCommandsContext(ctx context.Context) ([]*sonarr.CommandResponse, error)
	SendCommandContext(ctx context.Context, cmd *sonarr.CommandRequest) (*sonarr.CommandResponse, error)
	GetCommandStatusContext(ctx context.Context, commandID int64) (*sonarr.CommandResponse, error)
	GetDownloadClientsContext(ctx context.Context) ([]*sonarr.DownloadClientOutput, error)
	AddDownloadClientContext(ctx context.Context,
		client *sonarr.DownloadClientInput,
	) (*sonarr.DownloadClientOutput, error)
	TestDownloadClientContext(ctx context.Context, client *sonarr.DownloadClientInput) error
	UpdateDownloadClientContext(ctx context.Context,
		client *sonarr.DownloadClientInput,
		force bool,
	) (*sonarr.DownloadClientOutput, error)
	GetSystemStatusContext(ctx context.Context) (*sonarr.SystemStatus, error)
	GetAllSeriesContext(ctx context.Context) ([]*sonarr.Series, error)
	GetSeriesByIDContext(ctx context.Context, seriesID int64) (*sonarr.Series, error)
	GetNotificationsContext(ctx context.Context) ([]*sonarr.NotificationOutput, error)
	AddNotificationContext(ctx context.Context, client *sonarr.NotificationInput) (*sonarr.NotificationOutput, error)
	UpdateNotificationContext(ctx context.Context,
		client *sonarr.NotificationInput,
	) (*sonarr.NotificationOutput, error)
	GetHistoryPageContext(ctx context.Context, params *starr.PageReq) (*sonarr.History, error)
	FailContext(ctx context.Context, historyID int64) error
}
