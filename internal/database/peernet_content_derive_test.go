package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gk(c byte) string { return hexID(c) }

func TestPeernetContent_SchemeAndGroupRoundTrip(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "c/a.mkv", FileID: hexID('a'), ReleaseID: hexID('1'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('d')},
		{FilePath: "c/b.mkv", FileID: hexID('b'), ReleaseID: hexID('2'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('e')},
		{FilePath: "c/c.mkv", FileID: hexID('c')},
	}))
	a, ok, err := r.GetByPath(ctx, "c/a.mkv")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, PeernetSchemeNZB, a.ReleaseScheme)
	assert.Equal(t, gk('d'), a.GroupKey)
	b, _, _ := r.GetByPath(ctx, "c/b.mkv")
	assert.Equal(t, PeernetSchemeFiles, b.ReleaseScheme)
	c, _, _ := r.GetByPath(ctx, "c/c.mkv")
	assert.Equal(t, 0, c.ReleaseScheme)
	assert.Empty(t, c.GroupKey)

	page, err := r.ListPage(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, page, 3)
	assert.Equal(t, PeernetSchemeFiles, page[1].ReleaseScheme)
	byRel, err := r.FindByReleaseID(ctx, hexID('2'))
	require.NoError(t, err)
	require.Len(t, byRel, 1)
	assert.Equal(t, PeernetSchemeFiles, byRel[0].ReleaseScheme)
}

func TestPeernetContent_RejectsInconsistentScheme(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	bad := []PeernetContentEntry{
		{FilePath: "x", FileID: hexID('a'), ReleaseScheme: PeernetSchemeFiles},        // scheme without id
		{FilePath: "x", FileID: hexID('a'), ReleaseID: hexID('1'), ReleaseScheme: 3},  // unknown scheme
		{FilePath: "x", FileID: hexID('a'), GroupKey: "not-hex"},                      // bad group key
		{FilePath: "x", FileID: hexID('a'), ReleaseID: hexID('1'), ReleaseScheme: -1}, // negative
	}
	for i, e := range bad {
		assert.Error(t, r.Upsert(ctx, e), "case %d", i)
	}
	// A release ID with no scheme is the original (nzb) kind.
	require.NoError(t, r.Upsert(ctx, PeernetContentEntry{FilePath: "x", FileID: hexID('a'), ReleaseID: hexID('1')}))
	got, _, _ := r.GetByPath(ctx, "x")
	assert.Equal(t, PeernetSchemeNZB, got.ReleaseScheme)
}

func TestPeernetContent_CountsBySchemeAndReason(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "a", FileID: hexID('a'), ReleaseID: hexID('1'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('d')},
		{FilePath: "b", FileID: hexID('b'), ReleaseID: hexID('2'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('e')},
		{FilePath: "c", FileID: hexID('c'), ReleaseID: hexID('2'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('e')},
		{FilePath: "d", FileID: hexID('d'), GroupKey: gk('f')},
		{FilePath: "e", FileID: hexID('e')},
	}))
	c, err := r.Counts(ctx)
	require.NoError(t, err)
	assert.Equal(t, PeernetContentCounts{Files: 5, WithReleaseID: 3, WithNZBReleaseID: 1, WithDerivedReleaseID: 2, NoSourceGroup: 1}, c)
}

func TestPeernetContent_GroupSummariesKeysetAndExcludesNZB(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "n/1", FileID: hexID('1'), ReleaseID: hexID('9'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('a')},
		{FilePath: "n/2", FileID: hexID('2'), GroupKey: gk('a')}, // old-format sibling of an nzb file
		{FilePath: "o/1", FileID: hexID('3'), GroupKey: gk('b')},
		{FilePath: "o/2", FileID: hexID('4'), GroupKey: gk('b')},
		{FilePath: "o/3", FileID: hexID('5'), ReleaseID: hexID('7'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('b')},
		{FilePath: "p/1", FileID: hexID('6')}, // no group
	}))
	first, err := r.GroupSummaries(ctx, "", 1)
	require.NoError(t, err)
	assert.Equal(t, []PeernetGroupSummary{{Key: gk('a'), Files: 1}}, first)
	rest, err := r.GroupSummaries(ctx, first[0].Key, 10)
	require.NoError(t, err)
	assert.Equal(t, []PeernetGroupSummary{{Key: gk('b'), Files: 3}}, rest)

	members, err := r.GroupMembers(ctx, []string{gk('a'), gk('b')})
	require.NoError(t, err)
	require.Len(t, members, 4, "the nzb-scheme file is not a member of a derived group")
	assert.Equal(t, "n/2", members[0].FilePath)
	assert.Equal(t, "o/1", members[1].FilePath)
	none, err := r.GroupMembers(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestPeernetContent_ApplyDerivedGuards(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "o/1", FileID: hexID('3'), GroupKey: gk('b')},
		{FilePath: "o/2", FileID: hexID('4'), GroupKey: gk('b')},
		{FilePath: "o/3", FileID: hexID('5'), ReleaseID: hexID('7'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('b')},
	}))
	n, err := r.ApplyDerived(ctx, []PeernetDerivedUpdate{
		{FilePath: "o/1", FileID: hexID('3'), GroupKey: gk('b'), ReleaseID: hexID('d')},
		{FilePath: "o/2", FileID: hexID('f'), GroupKey: gk('b'), ReleaseID: hexID('d')}, // file changed meanwhile
		{FilePath: "o/3", FileID: hexID('5'), GroupKey: gk('b'), ReleaseID: hexID('d')}, // nzb row is never touched
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	got, _, _ := r.GetByPath(ctx, "o/1")
	assert.Equal(t, hexID('d'), got.ReleaseID)
	assert.Equal(t, PeernetSchemeFiles, got.ReleaseScheme)
	two, _, _ := r.GetByPath(ctx, "o/2")
	assert.Empty(t, two.ReleaseID)
	three, _, _ := r.GetByPath(ctx, "o/3")
	assert.Equal(t, hexID('7'), three.ReleaseID)
	assert.Equal(t, PeernetSchemeNZB, three.ReleaseScheme)

	// Clearing (empty release ID) returns a row to "no id".
	_, err = r.ApplyDerived(ctx, []PeernetDerivedUpdate{{FilePath: "o/1", FileID: hexID('3'), GroupKey: gk('b')}})
	require.NoError(t, err)
	got, _, _ = r.GetByPath(ctx, "o/1")
	assert.Empty(t, got.ReleaseID)
	assert.Equal(t, 0, got.ReleaseScheme)
}

func TestPeernetContent_StillIndexedAndRefs(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	require.NoError(t, r.UpsertBatch(ctx, []PeernetContentEntry{
		{FilePath: "d/1", FileID: hexID('3'), ReleaseID: hexID('d'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('b')},
		{FilePath: "d/sub/2", FileID: hexID('4'), ReleaseID: hexID('d'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('b')},
		{FilePath: "e/1", FileID: hexID('5'), ReleaseID: hexID('7'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('c')},
		{FilePath: "dd/1", FileID: hexID('6'), GroupKey: gk('9')},
	}))
	live, err := r.StillIndexed(ctx, []string{hexID('d'), hexID('e')})
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{hexID('d'): true}, live)

	refs, err := r.RefsByPrefix(ctx, "d", 100)
	require.NoError(t, err)
	assert.Len(t, refs, 2, "exact directory match: dd is not under d")
	ref, ok, err := r.RefByPath(ctx, "e/1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, PeernetContentRef{GroupKey: gk('c'), ReleaseID: hexID('7'), ReleaseScheme: PeernetSchemeNZB}, ref)

	_, err = r.RefsByPrefix(ctx, "", 10)
	assert.Error(t, err)
}

func TestPeernetContent_VideoFilesOfReleases(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	var rows []PeernetContentEntry
	for i := range 20 {
		rows = append(rows, PeernetContentEntry{FilePath: fmt.Sprintf("big/e%02d.MKV", i), FileID: hexID(byte('a' + i%6)), ReleaseID: hexID('1'), ReleaseScheme: PeernetSchemeNZB})
	}
	rows = append(rows,
		PeernetContentEntry{FilePath: "small/m.mkv", FileID: hexID('b'), ReleaseID: hexID('2'), ReleaseScheme: PeernetSchemeFiles},
		PeernetContentEntry{FilePath: "small/m.nfo", FileID: hexID('c'), ReleaseID: hexID('2'), ReleaseScheme: PeernetSchemeFiles},
		PeernetContentEntry{FilePath: "arch/x.r00", FileID: hexID('d'), ReleaseID: hexID('3'), ReleaseScheme: PeernetSchemeFiles},
	)
	require.NoError(t, r.UpsertBatch(ctx, rows))
	got, err := r.VideoFilesOfReleases(ctx, []string{hexID('1'), hexID('2'), hexID('3')}, 16)
	require.NoError(t, err)
	assert.Len(t, got[hexID('1')], 17, "one more than the cap so the caller can tell it overflowed")
	assert.Equal(t, []string{hexID('b')}, got[hexID('2')])
	assert.Empty(t, got[hexID('3')], "a release with no video file has no label files")

	many := make([]string, PeernetMaxPathsPerQuery+1)
	for i := range many {
		many[i] = fmt.Sprintf("%064x", i+1)
	}
	_, err = r.VideoFilesOfReleases(ctx, many, 16)
	assert.ErrorIs(t, err, ErrPeernetTooManyIDs)
}

func TestPeernetContent_GroupCountsAndClearDerivedGroup(t *testing.T) {
	r := newPeernetTestRepo(t)
	ctx := context.Background()
	var rows []PeernetContentEntry
	for i := range 25 {
		rows = append(rows, PeernetContentEntry{FilePath: fmt.Sprintf("big/%02d.mkv", i), FileID: fmt.Sprintf("%064x", i+1),
			ReleaseID: hexID('d'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('b')})
	}
	rows = append(rows,
		PeernetContentEntry{FilePath: "n/1", FileID: hexID('1'), ReleaseID: hexID('7'), ReleaseScheme: PeernetSchemeNZB, GroupKey: gk('b')},
		PeernetContentEntry{FilePath: "other/1", FileID: hexID('2'), ReleaseID: hexID('e'), ReleaseScheme: PeernetSchemeFiles, GroupKey: gk('c')})
	require.NoError(t, r.UpsertBatch(ctx, rows))

	counts, err := r.GroupCounts(ctx, []string{gk('b'), gk('c'), gk('a')})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{gk('b'): 25, gk('c'): 1}, counts, "nzb-scheme rows are not counted; empty groups are absent")

	old, err := r.ClearDerivedGroup(ctx, gk('b'), 10) // several short transactions
	require.NoError(t, err)
	assert.Equal(t, []string{hexID('d')}, old)
	left, _ := r.FindByReleaseID(ctx, hexID('d'))
	assert.Empty(t, left)
	nzb, _, _ := r.GetByPath(ctx, "n/1")
	assert.Equal(t, hexID('7'), nzb.ReleaseID, "an nzb-scheme row is never cleared")
	other, _, _ := r.GetByPath(ctx, "other/1")
	assert.Equal(t, hexID('e'), other.ReleaseID, "another group is untouched")
}
