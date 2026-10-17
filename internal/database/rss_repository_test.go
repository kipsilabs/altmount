package database

import (
	"context"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func testRSSReceipts(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	found, err := db.HasRSSImport(ctx, "rss-new")
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, db.RecordRSSImport(ctx, "rss-new"))
	require.NoError(t, db.RecordRSSImport(ctx, "rss-new"))
	found, err = db.HasRSSImport(ctx, "rss-new")
	require.NoError(t, err)
	require.True(t, found)
	item := &ImportQueueItem{NzbPath: "/test.nzb", DownloadID: ptrString("rss-recovery"), Status: QueueStatusPending, Priority: QueuePriorityNormal, MaxRetries: 3}
	repo := NewQueueRepository(db.conn, db.dialect.d)
	require.NoError(t, repo.AddToQueue(ctx, item))
	found, err = db.HasRSSImport(ctx, "rss-recovery")
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, db.RecordRSSImport(ctx, "rss-recovery"))
	removed, err := db.RemoveIncompleteRSSImport(ctx, "rss-recovery")
	require.NoError(t, err)
	require.True(t, removed)
	active := &ImportQueueItem{NzbPath: "/active.nzb", DownloadID: ptrString("rss-active"), Status: QueueStatusProcessing, Priority: QueuePriorityNormal, MaxRetries: 3}
	require.NoError(t, repo.AddToQueue(ctx, active))
	removed, err = db.RemoveIncompleteRSSImport(ctx, "rss-active")
	require.NoError(t, err)
	require.False(t, removed)

	found, err = db.HasRSSImport(ctx, "rss-recovery")
	require.NoError(t, err)
	require.True(t, found)
	_, err = db.conn.ExecContext(ctx, db.dialect.q(`INSERT INTO import_history (nzb_name, file_name, virtual_path, download_id) VALUES (?, ?, ?, ?)`), "n", "f", "/f", "rss-history")
	require.NoError(t, err)
	found, err = db.HasRSSImport(ctx, "rss-history")
	require.NoError(t, err)
	require.True(t, found)
}

func TestRSSReceiptsSQLite(t *testing.T) {
	db, err := NewDB(Config{DatabasePath: filepath.Join(t.TempDir(), "rss.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	testRSSReceipts(t, db)
}

func TestRSSReceiptsPostgres(t *testing.T) {
	conn := openPostgresLatest(t)
	testRSSReceipts(t, &DB{conn: conn, dialect: dialectHelper{d: DialectPostgres}})
}

func ptrString(value string) *string { return &value }
