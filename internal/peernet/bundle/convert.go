package bundle

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// LocalFile is one file ready for the metadata service. Name is a sanitised
// base name: the caller derives the virtual path locally and never from
// anything the peer sent.
type LocalFile struct {
	Name string
	Meta *metapb.FileMetadata
}

// Local is a bundle converted to the local on-disk model.
type Local struct {
	Scheme      Scheme
	ReleaseID   contentid.ID
	Title       string
	PostedUnix  int64
	NZBPassword string
	// NZBFiles (SchemeNZB) is the full release segment list per NZB file, for
	// computing the Release ID of the local import later. It lacks subjects,
	// posters, groups and encoded sizes, so it cannot regenerate an NZB.
	NZBFiles [][]string
	Files    []LocalFile
}

// Field-by-field mapping, bundle to local metadata.
//
//	shared:   FileSize, SegmentData (id, size, start, end), ReleaseDate,
//	          OriginalFilename, Encryption=AES + AesKey/AesIv, NestedSources and
//	          SharedOuterSources (segments, key, iv, inner offset/length/volume
//	          size), ClipBoundaries, Par2Files (name, size, segments)
//	derived:  Status (HEALTHY, as a fresh import), CreatedAt/ModifiedAt (now),
//	          placeholder ids of gaps
//	dropped:  SourceNzbPath, StoreRef, NzbdavId, Password, Salt, KnownHoles,
//	          HoleProviderFingerprint, SegmentRefs/Runs (inline segments are
//	          used instead), segment-store references
//
// The result is the v1 inline layout: the readers need no .nzbz store entry.

func gapSalt(id contentid.ID, tag string) string { return hex.EncodeToString(id[:8]) + tag }

func segmentData(pool []string, segs []Segment, salt string) []*metapb.SegmentData {
	if len(segs) == 0 {
		return nil
	}
	out := make([]*metapb.SegmentData, len(segs))
	for i, s := range segs {
		id := ""
		if s.Ref == GapRef {
			id = holes.PlaceholderID(i+1, salt)
		} else {
			id = pool[s.Ref]
		}
		out[i] = &metapb.SegmentData{Id: id, SegmentSize: s.Size, StartOffset: s.Start, EndOffset: s.End}
	}
	return out
}

// buildMeta renders one (validated) file as on-disk metadata without local
// timestamps. Verify uses it to recompute the File ID with contentid.
func buildMeta(b *Bundle, f *File) *metapb.FileMetadata {
	m := &metapb.FileMetadata{
		FileSize:         f.Size,
		Status:           metapb.FileStatus_FILE_STATUS_HEALTHY,
		ReleaseDate:      f.ReleaseDate,
		OriginalFilename: f.Name,
		SegmentData:      segmentData(b.Pool, f.Segments, gapSalt(f.FileID, "")),
	}
	if len(f.AESKey) > 0 {
		m.Encryption = metapb.Encryption_AES
		m.AesKey = append([]byte(nil), f.AESKey...)
		m.AesIv = append([]byte(nil), f.AESIV...)
	}
	for i, o := range f.Outer {
		sh := &metapb.NestedSegmentSource{
			Segments:        segmentData(b.Pool, o.Segments, gapSalt(f.FileID, fmt.Sprintf("o%d", i))),
			InnerVolumeSize: o.VolumeSize,
		}
		if len(o.AESKey) > 0 {
			sh.AesKey = append([]byte(nil), o.AESKey...)
			sh.AesIv = append([]byte(nil), o.AESIV...)
		}
		m.SharedOuterSources = append(m.SharedOuterSources, sh)
	}
	for _, e := range f.Extents {
		m.NestedSources = append(m.NestedSources, &metapb.NestedSegmentSource{
			SharedOuterSourceIndex: int32(e.Outer + 1),
			InnerOffset:            e.InnerOffset,
			InnerLength:            e.InnerLength,
		})
	}
	for _, c := range f.Clips {
		m.ClipBoundaries = append(m.ClipBoundaries, &metapb.ClipBoundary{ByteLen: c.ByteLen, Delta_90K: c.Delta90k})
	}
	for i, p := range f.Par2 {
		m.Par2Files = append(m.Par2Files, &metapb.Par2FileReference{
			Filename:    p.Name,
			FileSize:    p.Size,
			SegmentData: segmentData(b.Pool, p.Segments, gapSalt(f.FileID, fmt.Sprintf("p%d", i))),
		})
	}
	return m
}

// ToLocal validates b and converts it to local metadata messages, ready for
// MetadataService.WriteFileMetadata. now stamps CreatedAt and ModifiedAt.
// Virtual paths are never taken from the bundle.
func ToLocal(b *Bundle, now time.Time) (*Local, error) {
	if err := Validate(b); err != nil {
		return nil, err
	}
	loc := &Local{Scheme: b.Scheme, ReleaseID: b.ReleaseID, Title: b.Title, PostedUnix: b.PostedUnix, NZBPassword: b.NZBPassword}
	if b.Scheme == SchemeNZB {
		pos := 0
		for _, n := range b.ReleaseFiles {
			loc.NZBFiles = append(loc.NZBFiles, append([]string{}, b.Pool[pos:pos+n]...))
			pos += n
		}
	}
	for i := range b.Files {
		f := &b.Files[i]
		m := buildMeta(b, f)
		m.CreatedAt, m.ModifiedAt = now.Unix(), now.Unix()
		loc.Files = append(loc.Files, LocalFile{Name: f.Name, Meta: m})
	}
	return loc, nil
}
