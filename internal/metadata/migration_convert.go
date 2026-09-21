package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// GroupResult reports what happened to one release group.
type GroupResult struct {
	Key           string   `json:"key"`
	StoreRef      string   `json:"store_ref"`
	Faithful      bool     `json:"faithful"`
	FilesMigrated int      `json:"files_migrated"`
	FilesFailed   int      `json:"files_failed"`
	BytesBefore   int64    `json:"bytes_before"`
	BytesAfter    int64    `json:"bytes_after"`
	Failures      []string `json:"failures,omitempty"`
}

// MigrateGroup converts one release group to the v3 store-backed format.
//
// Ordering is load-bearing: the .nzbz is written and read back before any meta
// is repointed at it, so a meta never names a store that is missing or corrupt.
// Each meta is then rewritten atomically by WriteFileMetadataV3, which also
// increments the store's reference count exactly once, leaving the count equal
// to the number of migrated metas.
//
// A per-file failure leaves that file as v1 and is recorded; the rest of the
// group still migrates. If every file fails, the freshly written store is
// removed so no orphan is left behind.
//
// The group's metas are loaded here rather than at scan time: legacy metas
// carry their segments inline, so hydrating the whole library at once is the
// allocation pattern documented at service.go:405.
func (ms *MetadataService) MigrateGroup(ctx context.Context, g LegacyGroup, storeDir, defaultGroup string) (GroupResult, error) {
	res := GroupResult{Key: g.Key}
	for _, lm := range g.Files {
		res.BytesBefore += lm.SizeBytes
	}

	// Hydrate this group's metas now and let them fall out of scope when the
	// call returns, so peak memory is one release rather than the whole library.
	// A group hydrated by the caller (tests, or a re-used group) is left alone.
	if len(g.Files) > 0 && g.Files[0].Meta == nil {
		loaded, err := ms.LoadGroupMetas(g)
		if err != nil {
			return res, err
		}
		g = loaded
	}

	store, index, faithful, err := buildGroupStore(g, defaultGroup)
	if err != nil {
		return res, fmt.Errorf("build store for %q: %w", g.Key, err)
	}
	res.Faithful = faithful

	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return res, fmt.Errorf("create store dir %q: %w", storeDir, err)
	}
	storeRef := storeHashPath(storeDir, g.Key, store)
	if err := ms.store.WriteStore(storeRef, store); err != nil {
		return res, fmt.Errorf("write store %q: %w", storeRef, err)
	}
	// Read back from disk, bypassing the cache WriteStore just populated, so
	// this proves the bytes really landed and decompress.
	if _, err := ms.store.ReadStoreUncached(storeRef); err != nil {
		_ = os.Remove(storeRef)
		return res, fmt.Errorf("store integrity check failed for %q: %w", storeRef, err)
	}
	res.StoreRef = storeRef

	for _, lm := range g.Files {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return res, ctxErr
		}

		// Keep the exact legacy bytes so a file that fails conversion or
		// verification can be put back precisely as it was.
		originalBytes, readErr := os.ReadFile(lm.MetaPath)
		if readErr != nil {
			res.FilesFailed++
			res.Failures = append(res.Failures, fmt.Sprintf("%s: read original: %v", lm.VirtualPath, readErr))
			slog.WarnContext(ctx, "Failed to read legacy metadata before migrating; skipping file",
				"virtual_path", lm.VirtualPath, "error", readErr)
			continue
		}

		if err := ms.WriteFileMetadataV3(ctx, lm.VirtualPath, lm.Meta, index, storeRef); err != nil {
			res.FilesFailed++
			res.Failures = append(res.Failures, fmt.Sprintf("%s: %v", lm.VirtualPath, err))
			slog.WarnContext(ctx, "Failed to migrate metadata file; leaving it in the legacy format",
				"virtual_path", lm.VirtualPath, "error", err)
			continue
		}

		// Prove this specific file still resolves to exactly the segments it
		// had before, and undo it if not. This is what makes the migration
		// trustworthy on metadata shapes no test anticipated.
		if verifyErr := verifyMigratedFile(ms, lm.VirtualPath, lm.Meta); verifyErr != nil {
			res.FilesFailed++
			res.Failures = append(res.Failures,
				fmt.Sprintf("%s: verification failed, restored legacy metadata: %v", lm.VirtualPath, verifyErr))
			slog.ErrorContext(ctx, "Migrated metadata did not verify; restoring the legacy file",
				"virtual_path", lm.VirtualPath, "error", verifyErr)
			if restoreErr := restoreLegacyMeta(lm.MetaPath, originalBytes); restoreErr != nil {
				slog.ErrorContext(ctx, "Failed to restore legacy metadata after a failed verification",
					"virtual_path", lm.VirtualPath, "error", restoreErr)
				res.Failures = append(res.Failures,
					fmt.Sprintf("%s: RESTORE FAILED: %v", lm.VirtualPath, restoreErr))
			}
			// The failed v3 write already refreshed the lite cache; drop that
			// entry so readers see the restored legacy file.
			ms.liteCache.Remove(lm.VirtualPath)
			continue
		}

		res.FilesMigrated++
		// Measure the meta this service just wrote, not lm.MetaPath: a dry run
		// converts into a throwaway root while lm.MetaPath still names the
		// untouched legacy file, which would project "before + store".
		if info, statErr := os.Stat(ms.metaFilePath(lm.VirtualPath)); statErr == nil {
			res.BytesAfter += info.Size()
		}
	}

	if res.FilesMigrated == 0 {
		if removeErr := os.Remove(storeRef); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.WarnContext(ctx, "Failed to remove orphaned store after a fully failed group",
				"store_ref", storeRef, "error", removeErr)
		}
		res.StoreRef = ""
		return res, nil
	}

	if info, statErr := os.Stat(storeRef); statErr == nil {
		res.BytesAfter += info.Size()
	}
	return res, nil
}
