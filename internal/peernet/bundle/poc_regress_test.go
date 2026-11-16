package bundle_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/bundle"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

func mid(i int) string { return fmt.Sprintf("part%d@poc.example", i) }

// seal fills the identities and returns the Verify result.
func seal(t *testing.T, b *bundle.Bundle) error {
	b.Title, b.PostedUnix = "Poc.Release", 1700000000
	var sum int64
	for i := range b.Files {
		b.Files[i].FileID = contentid.ID{byte(i + 1)}
		sum += b.Files[i].Size
	}
	b.TotalSize = sum
	// provisional ReleaseID so ToLocal's Validate passes
	b.ReleaseID = contentid.ID{9}
	loc, err := bundle.ToLocal(b, time.Now())
	if err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	ids := []contentid.ID{}
	for i, lf := range loc.Files {
		id, err := contentid.FileID(lf.Meta)
		if err != nil {
			return err
		}
		b.Files[i].FileID = id
		ids = append(ids, id)
	}
	sortFiles(b)
	ids = ids[:0]
	for i := range b.Files {
		ids = append(ids, b.Files[i].FileID)
	}
	if b.Scheme == bundle.SchemeFiles {
		b.ReleaseID = bundle.FilesReleaseID(ids)
	} else {
		store := &metapb.NzbStore{}
		for _, f := range loc.NZBFiles {
			fe := &metapb.NzbFileEntry{}
			for _, id := range f {
				fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
			}
			store.Files = append(store.Files, fe)
		}
		b.ReleaseID, err = contentid.ReleaseID(store)
		if err != nil {
			return err
		}
	}
	wire, id, err := bundle.Pack(b)
	if err != nil {
		return fmt.Errorf("pack: %w", err)
	}
	_, err = bundle.Verify(wire, id, b.ReleaseID)
	return err
}

func sortFiles(b *bundle.Bundle) {
	for i := 0; i < len(b.Files); i++ {
		for j := i + 1; j < len(b.Files); j++ {
			if string(b.Files[j].FileID[:]) < string(b.Files[i].FileID[:]) {
				b.Files[i], b.Files[j] = b.Files[j], b.Files[i]
			}
		}
	}
}

// Q1: files scheme, a file made of 100 one-segment parts that each take 10
// bytes from the middle of a 700 KB article. Header binding at accept would
// be optional for the attacker, so the format itself must refuse it: a part
// that is neither the first nor the last must read nearly all of its articles.
func TestQ1FilesSchemeSingleSegmentPartsAreRefused(t *testing.T) {
	n := 100
	pool := []string{}
	segs := []bundle.Segment{}
	parts := []int{}
	for i := 0; i < n; i++ {
		pool = append(pool, mid(i))
		segs = append(segs, bundle.Segment{Ref: int64(i), Size: 700000, Start: 1000, End: 1009})
		parts = append(parts, 1)
	}
	b := &bundle.Bundle{Scheme: bundle.SchemeFiles, Pool: pool,
		Files: []bundle.File{{Name: "Movie.mkv", Size: int64(n) * 10, Segments: segs, Parts: parts}}}
	if err := seal(t, b); !errors.Is(err, bundle.ErrPartialRange) {
		t.Fatalf("want ErrPartialRange, got %v", err)
	}
}

// Q2: the same 100 windows as ONE part must stay refused.
func TestQ2OnePartOfPartialSegmentsIsRefused(t *testing.T) {
	n := 100
	pool := []string{}
	segs := []bundle.Segment{}
	for i := 0; i < n; i++ {
		pool = append(pool, mid(i))
		segs = append(segs, bundle.Segment{Ref: int64(i), Size: 700000, Start: 1000, End: 1009})
	}
	b := &bundle.Bundle{Scheme: bundle.SchemeFiles, Pool: pool,
		Files: []bundle.File{{Name: "Movie.mkv", Size: int64(n) * 10, Segments: segs, Parts: []int{n}}}}
	if err := seal(t, b); err == nil {
		t.Fatal("a single part of 100 partial segments must be refused")
	}
}

// Q3: nzb scheme, 100 entries; the file takes one arbitrary 10-byte window of
// one article of each, in reverse NZB order.
func TestQ3NZBReorderedWindowsAreRefused(t *testing.T) {
	entries := 100
	pool := []string{}
	rel := []int{}
	for e := 0; e < entries; e++ {
		rel = append(rel, 3)
		for k := 0; k < 3; k++ {
			pool = append(pool, mid(e*3+k))
		}
	}
	segs := []bundle.Segment{}
	parts := []int{}
	for e := entries - 1; e >= 0; e-- {
		segs = append(segs, bundle.Segment{Ref: int64(e*3 + 1), Size: 700000, Start: 5, End: 14})
		parts = append(parts, 1)
	}
	b := &bundle.Bundle{Scheme: bundle.SchemeNZB, Pool: pool, ReleaseFiles: rel,
		Files: []bundle.File{{Name: "Movie.mkv", Size: int64(entries) * 10, Segments: segs, Parts: parts}}}
	if err := seal(t, b); !errors.Is(err, bundle.ErrPartialRange) {
		t.Fatalf("want ErrPartialRange, got %v", err)
	}
}

// An honest stored multi-volume layout still validates: each volume is read
// from just after its archive headers to just before its end block.
func TestHonestVolumePartsStillValidate(t *testing.T) {
	const art = 700000
	pool := []string{}
	segs := []bundle.Segment{}
	parts := []int{}
	var size int64
	for v := 0; v < 4; v++ {
		for k := 0; k < 3; k++ {
			ref := int64(len(pool))
			pool = append(pool, mid(int(ref)))
			s := bundle.Segment{Ref: ref, Size: art, Start: 0, End: art - 1}
			if k == 0 {
				s.Start = 300 // headers of the volume
			}
			if k == 2 {
				s.End = art - 1 - 24 // end-of-archive block
			}
			segs = append(segs, s)
			size += s.End - s.Start + 1
		}
		parts = append(parts, 3)
	}
	b := &bundle.Bundle{Scheme: bundle.SchemeFiles, Pool: pool, Files: []bundle.File{{Name: "Movie.mkv", Size: size, Segments: segs, Parts: parts}}}
	if err := seal(t, b); err != nil {
		t.Fatalf("an honest layout must validate: %v", err)
	}
}
