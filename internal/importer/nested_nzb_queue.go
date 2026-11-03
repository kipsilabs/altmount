package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
)

func (s *Service) enqueueNestedNzbs(ctx context.Context, parent *database.ImportQueueItem, expansion *nestedNzbJobsError) error {
	stage, err := os.MkdirTemp(filepath.Dir(parent.NzbPath), ".nested-nzbs-*")
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			os.RemoveAll(stage)
		}
	}()
	fields := map[string]json.RawMessage{}
	if parent.Metadata != nil && *parent.Metadata != "" {
		if err := json.Unmarshal([]byte(*parent.Metadata), &fields); err != nil {
			return NewNonRetryableError("invalid wrapper metadata", err)
		}
		if fields == nil {
			fields = map[string]json.RawMessage{}
		}
	}
	delete(fields, "extracted_files")
	children := make([]*database.ImportQueueItem, 0, len(expansion.Jobs))
	for i, job := range expansion.Jobs {
		chain, err := expansion.Chain.follow(job.Data)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(chain)
		if err != nil {
			return err
		}
		fields[nestedNzbChainKey] = raw
		raw, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		metadata := string(raw)
		name := nzbtrim.TrimNzbExtension(sanitizeFilename(filepath.Base(strings.ReplaceAll(job.Name, `\`, "/"))))
		if name == "" || name == "." || name == ".." {
			name = "release"
		}
		dir := filepath.Join(stage, fmt.Sprint(i))
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		path := filepath.Join(dir, name+".nzb")
		if err := os.WriteFile(path, job.Data, 0600); err != nil {
			return err
		}
		size, err := s.CalculateFileSizeOnly(path)
		if err != nil {
			return err
		}
		downloadID := uuid.NewString()
		children = append(children, &database.ImportQueueItem{
			NzbPath: path, DownloadID: &downloadID, RelativePath: parent.RelativePath,
			Category: parent.Category, Priority: parent.Priority, MaxRetries: parent.MaxRetries,
			Status: database.QueueStatusPending, BatchID: parent.BatchID, Metadata: &metadata,
			FileSize: &size, TargetPath: parent.TargetPath, Indexer: parent.Indexer,
			SkipArrNotification: parent.SkipArrNotification, SkipPostImportLinks: parent.SkipPostImportLinks,
		})
	}
	if err := s.database.Repository.ExpandNzbWrapper(ctx, parent, children); err != nil {
		return err
	}
	// A concurrent expansion leaves all staged IDs unset.
	keep = children[0].ID != 0
	if keep {
		first := children[0]
		oldPath := parent.NzbPath
		parent.NzbPath, parent.Metadata, parent.FileSize = first.NzbPath, first.Metadata, first.FileSize
		parent.RetryCount, parent.StoragePath, parent.CompletedAt = 0, nil, nil
		os.Remove(oldPath)
	} else {
		stored, err := s.database.Repository.GetQueueItem(ctx, parent.ID)
		if err != nil {
			return err
		}
		if stored == nil {
			return fmt.Errorf("NZB wrapper no longer exists")
		}
		*parent = *stored
	}
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}
