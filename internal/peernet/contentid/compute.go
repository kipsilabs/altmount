package contentid

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"strings"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

const (
	releaseDomain = "altmount-peernet/release/v1"
	fileDomain    = "altmount-peernet/file/v1"
)

var (
	// ErrNoSegments means there were no real segment IDs to identify.
	ErrNoSegments = errors.New("contentid: no real segments")
	// ErrEmptyMessageID means a segment carried an empty message-ID.
	ErrEmptyMessageID = errors.New("contentid: empty message-id")
	// ErrUnresolved means the metadata still holds segment references into a
	// store instead of resolved message-IDs.
	ErrUnresolved = errors.New("contentid: metadata segments are not resolved")
	// ErrBadSharedSource means a nested source points at a shared outer source
	// that does not exist.
	ErrBadSharedSource = errors.New("contentid: bad shared outer source index")
)

// canonical strips one pair of angle brackets. Placeholder detection happens
// on the raw value (IsPlaceholderID tolerates brackets).
func canonical(id string) string {
	id = strings.TrimPrefix(id, "<")
	return strings.TrimSuffix(id, ">")
}

type idWriter struct{ h hash.Hash }

func newWriter(domain string) *idWriter {
	w := &idWriter{h: sha256.New()}
	w.u32(uint32(len(domain)))
	_, _ = w.h.Write([]byte(domain))
	return w
}

func (w *idWriter) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, _ = w.h.Write(b[:])
}

func (w *idWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, _ = w.h.Write(b[:])
}

func (w *idWriter) sum() ID {
	var id ID
	copy(id[:], w.h.Sum(nil))
	return id
}

// section writes a count-prefixed list of canonical real IDs and returns how
// many were written.
func (w *idWriter) section(ids []string) {
	w.u64(uint64(len(ids)))
	for _, id := range ids {
		w.u32(uint32(len(id)))
		_, _ = w.h.Write([]byte(id))
	}
}

// realIDs filters placeholders and canonicalises. next yields raw IDs.
func realIDs(n int, at func(i int) (string, bool)) ([]string, error) {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		raw, ok := at(i)
		if !ok || canonical(raw) == "" {
			return nil, ErrEmptyMessageID
		}
		if holes.IsPlaceholderID(raw) {
			continue
		}
		out = append(out, canonical(raw))
	}
	return out, nil
}

// ReleaseID computes the Release ID of an NZB store. See the package
// documentation for the exact layout.
func ReleaseID(store *metapb.NzbStore) (ID, error) {
	if store == nil {
		return ID{}, ErrNoSegments
	}
	w := newWriter(releaseDomain)
	w.u64(uint64(len(store.Files)))
	total := 0
	for fi, f := range store.Files {
		var segs []*metapb.NzbSeg
		if f != nil {
			segs = f.Segments
		}
		ids, err := realIDs(len(segs), func(i int) (string, bool) {
			if segs[i] == nil {
				return "", false
			}
			return segs[i].Id, true
		})
		if err != nil {
			return ID{}, fmt.Errorf("file %d: %w", fi, err)
		}
		total += len(ids)
		w.section(ids)
	}
	if total == 0 {
		return ID{}, ErrNoSegments
	}
	return w.sum(), nil
}

func segmentDataIDs(segs []*metapb.SegmentData) ([]string, error) {
	return realIDs(len(segs), func(i int) (string, bool) {
		if segs[i] == nil {
			return "", false
		}
		return segs[i].Id, true
	})
}

// FileID computes the File ID of a fully resolved FileMetadata (as returned
// by metadata.MetadataService.ReadFileMetadata). The input is not modified.
func FileID(m *metapb.FileMetadata) (ID, error) {
	if m == nil {
		return ID{}, ErrNoSegments
	}
	if len(m.SegmentData) == 0 && (len(m.SegmentRefs) > 0 || len(m.SegmentRuns) > 0) {
		return ID{}, fmt.Errorf("%w: main segments", ErrUnresolved)
	}

	w := newWriter(fileDomain)
	total := 0

	main, err := segmentDataIDs(m.SegmentData)
	if err != nil {
		return ID{}, fmt.Errorf("main segments: %w", err)
	}
	total += len(main)
	w.section(main)

	w.u64(uint64(len(m.NestedSources)))
	for i, ns := range m.NestedSources {
		segs, err := nestedSegments(m, ns)
		if err != nil {
			return ID{}, fmt.Errorf("nested source %d: %w", i, err)
		}
		ids, err := segmentDataIDs(segs)
		if err != nil {
			return ID{}, fmt.Errorf("nested source %d: %w", i, err)
		}
		total += len(ids)
		w.section(ids)
	}

	if total == 0 {
		return ID{}, ErrNoSegments
	}
	return w.sum(), nil
}

// nestedSegments returns the outer segments of ns, following a shared outer
// source reference without mutating the metadata.
func nestedSegments(m *metapb.FileMetadata, ns *metapb.NestedSegmentSource) ([]*metapb.SegmentData, error) {
	if ns == nil {
		return nil, nil
	}
	if ns.SharedOuterSourceIndex > 0 {
		idx := int(ns.SharedOuterSourceIndex) - 1
		if idx >= len(m.SharedOuterSources) {
			return nil, fmt.Errorf("%w: %d of %d", ErrBadSharedSource, ns.SharedOuterSourceIndex, len(m.SharedOuterSources))
		}
		shared := m.SharedOuterSources[idx]
		if shared == nil {
			return nil, nil
		}
		if len(shared.Segments) == 0 && len(shared.SegmentRefs) > 0 {
			return nil, ErrUnresolved
		}
		return shared.Segments, nil
	}
	if len(ns.Segments) == 0 && len(ns.SegmentRefs) > 0 {
		return nil, ErrUnresolved
	}
	return ns.Segments, nil
}
