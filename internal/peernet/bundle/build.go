package bundle

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/peernet/contentid"
)

// FileInput is one local file of a release handed to the builder.
type FileInput struct {
	// Name is the original file name; when empty Meta.OriginalFilename is
	// used. It is sanitised to a base name.
	Name string
	// Meta is the file's metadata with its segments resolved (inline
	// SegmentData, nested sources with segments or shared outer sources), as
	// MetadataService.ReadFileMetadata returns it. It is not modified.
	Meta *metapb.FileMetadata
	// PasswordFromNZB says that any encryption key in Meta derives from a
	// password that is already in the NZB. The publisher alone can know this.
	// It only matters for a file that carries key material: when it is false
	// such a file is not shareable and neither is the release.
	PasswordFromNZB bool
	// PatchedLocally marks a file that was healed by local PAR2 patches. Its
	// articles do not match what the network would fetch, so it is not
	// shareable.
	PatchedLocally bool
}

// ReleaseInput is everything the builder needs. It is plain data: no I/O.
type ReleaseInput struct {
	Scheme   Scheme
	Title    string
	PostedAt time.Time
	// NZBPassword is the password found in the NZB, if any. It travels as an
	// opaque string and is never used by the receiver at import.
	NZBPassword string
	// NZBFiles (SchemeNZB only) lists the message-IDs of every NZB file in NZB
	// order, including files that were not imported. Gap placeholders are
	// skipped.
	NZBFiles [][]string
	Files    []FileInput
}

// ShareReason says why a release cannot be shared.
type ShareReason string

const (
	ReasonRclone          ShareReason = "rclone_encrypted"
	ReasonOtherEncryption ShareReason = "unsupported_encryption"
	ReasonPasswordNotNZB  ShareReason = "password_not_from_nzb"
	ReasonLocalPatch      ShareReason = "healed_by_local_patch"
)

// NotShareableError reports the first file that makes the release not
// shareable. errors.Is(err, ErrNotShareable) is true for it.
type NotShareableError struct {
	File   string
	Reason ShareReason
}

func (e *NotShareableError) Error() string {
	return fmt.Sprintf("%v: file %q: %s", ErrNotShareable, e.File, e.Reason)
}

// Is makes errors.Is(err, ErrNotShareable) match.
func (e *NotShareableError) Is(target error) bool { return target == ErrNotShareable }

func hasKeyMaterial(m *metapb.FileMetadata) bool {
	if len(m.AesKey) > 0 {
		return true
	}
	for _, ns := range m.NestedSources {
		if len(ns.AesKey) > 0 {
			return true
		}
	}
	for _, ns := range m.SharedOuterSources {
		if len(ns.AesKey) > 0 {
			return true
		}
	}
	return false
}

func checkShareable(fi *FileInput, name string) error {
	m := fi.Meta
	switch m.Encryption {
	case metapb.Encryption_NONE, metapb.Encryption_AES:
	case metapb.Encryption_RCLONE:
		return &NotShareableError{File: name, Reason: ReasonRclone}
	default:
		return &NotShareableError{File: name, Reason: ReasonOtherEncryption}
	}
	switch {
	case fi.PatchedLocally:
		return &NotShareableError{File: name, Reason: ReasonLocalPatch}
	case hasKeyMaterial(m) && !fi.PasswordFromNZB:
		return &NotShareableError{File: name, Reason: ReasonPasswordNotNZB}
	}
	return nil
}

type preparedFile struct {
	in   *FileInput
	name string
	id   contentid.ID
}

func mapIDError(err error) error {
	switch {
	case errors.Is(err, contentid.ErrEmptyMessageID):
		return fmt.Errorf("%w: %v", ErrBadMessageID, err)
	case errors.Is(err, contentid.ErrUnresolved):
		return fmt.Errorf("%w: %v", ErrUnresolved, err)
	case errors.Is(err, contentid.ErrBadSharedSource):
		return fmt.Errorf("%w: %v", ErrOutOfRange, err)
	default:
		return fmt.Errorf("%w: %v", ErrInvalidField, err)
	}
}

// splitExt returns name without a short trailing extension, and the extension.
func splitExt(name string) (string, string) {
	if i := strings.LastIndexByte(name, '.'); i > 0 && len(name)-i <= maxExtLen {
		return name[:i], name[i:]
	}
	return name, ""
}

// uniqueName returns name, or "stem (n).ext" for the smallest n >= 2 that is
// free, and records it in taken. Comparison ignores case.
func uniqueName(name string, taken map[string]struct{}) string {
	cand := name
	stem, ext := splitExt(name)
	for n := 2; ; n++ {
		if _, dup := taken[strings.ToLower(cand)]; !dup {
			taken[strings.ToLower(cand)] = struct{}{}
			return cand
		}
		cand = SanitizeFilename(fmt.Sprintf("%s (%d)%s", stem, n, ext))
	}
}

// poolBuilder maps message-IDs to pool indices.
type poolBuilder struct {
	nzb  bool
	idx  map[string]int64
	pool []string
}

func (p *poolBuilder) ref(raw string) (int64, error) {
	if holes.IsPlaceholderID(raw) {
		return 0, fmt.Errorf("%w: file has a known hole", ErrNotShareable)
	}
	id, err := CanonicalMessageID(raw)
	if err != nil {
		return 0, err
	}
	if i, ok := p.idx[id]; ok {
		return i, nil
	}
	if p.nzb {
		return 0, fmt.Errorf("%w: %q", ErrSegmentNotInNZB, id)
	}
	i := int64(len(p.pool))
	p.idx[id] = i
	p.pool = append(p.pool, id)
	return i, nil
}

func (p *poolBuilder) table(in []*metapb.SegmentData) ([]Segment, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]Segment, len(in))
	for i, s := range in {
		if s == nil {
			return nil, fmt.Errorf("%w: nil segment", ErrInvalidField)
		}
		r, err := p.ref(s.Id)
		if err != nil {
			return nil, err
		}
		out[i] = Segment{Ref: r, Size: s.SegmentSize, Start: s.StartOffset, End: s.EndOffset}
	}
	return out, nil
}

// outerKey identifies an outer source by content, to share identical ones.
func outerKey(ns *metapb.NestedSegmentSource) string {
	h := sha256.New()
	var b [8]byte
	w := func(v int64) {
		binary.BigEndian.PutUint64(b[:], uint64(v))
		h.Write(b[:])
	}
	w(int64(len(ns.Segments)))
	for _, s := range ns.Segments {
		w(int64(len(s.Id)))
		h.Write([]byte(s.Id))
		w(s.SegmentSize)
		w(s.StartOffset)
		w(s.EndOffset)
	}
	w(int64(len(ns.AesKey)))
	h.Write(ns.AesKey)
	w(int64(len(ns.AesIv)))
	h.Write(ns.AesIv)
	w(ns.InnerVolumeSize)
	return string(h.Sum(nil))
}

// effectiveOuter returns the outer source a nested source reads through,
// following a shared reference. Indices were checked by contentid.FileID.
func effectiveOuter(m *metapb.FileMetadata, ns *metapb.NestedSegmentSource) *metapb.NestedSegmentSource {
	if ns.SharedOuterSourceIndex > 0 {
		sh := m.SharedOuterSources[ns.SharedOuterSourceIndex-1]
		eff := &metapb.NestedSegmentSource{Segments: sh.Segments, AesKey: sh.AesKey, AesIv: sh.AesIv, InnerVolumeSize: sh.InnerVolumeSize}
		if ns.InnerVolumeSize != 0 {
			eff.InnerVolumeSize = ns.InnerVolumeSize
		}
		return eff
	}
	return ns
}

func (p *poolBuilder) file(pf preparedFile) (File, error) {
	m := pf.in.Meta
	if (len(m.AesKey) > 0) != (m.Encryption == metapb.Encryption_AES) {
		return File{}, fmt.Errorf("%w: encryption type and key disagree", ErrBadEncryption)
	}
	f := File{Name: pf.name, Size: m.FileSize, ReleaseDate: m.ReleaseDate, FileID: pf.id,
		AESKey: append([]byte(nil), m.AesKey...), AESIV: append([]byte(nil), m.AesIv...)}
	if len(f.AESKey) == 0 {
		f.AESKey, f.AESIV = nil, nil
	}
	var err error
	if f.Segments, err = p.table(m.SegmentData); err != nil {
		return File{}, err
	}
	index := map[string]int{}
	for _, ns := range m.NestedSources {
		if ns == nil {
			return File{}, fmt.Errorf("%w: nil nested source", ErrInvalidField)
		}
		eff := effectiveOuter(m, ns)
		k := outerKey(eff)
		oi, ok := index[k]
		if !ok {
			segs, err := p.table(eff.Segments)
			if err != nil {
				return File{}, err
			}
			o := OuterSource{Segments: segs, VolumeSize: eff.InnerVolumeSize}
			if len(eff.AesKey) > 0 {
				o.AESKey, o.AESIV = append([]byte(nil), eff.AesKey...), append([]byte(nil), eff.AesIv...)
			}
			oi = len(f.Outer)
			index[k] = oi
			f.Outer = append(f.Outer, o)
		}
		f.Extents = append(f.Extents, Extent{Outer: oi, InnerOffset: ns.InnerOffset, InnerLength: ns.InnerLength})
	}
	for _, c := range m.ClipBoundaries {
		if c == nil {
			return File{}, fmt.Errorf("%w: nil clip", ErrBadClip)
		}
		f.Clips = append(f.Clips, Clip{ByteLen: c.ByteLen, Delta90k: c.Delta_90K})
	}
	taken := map[string]struct{}{}
	for _, pr := range m.Par2Files {
		if pr == nil {
			return File{}, fmt.Errorf("%w: nil par2 reference", ErrInvalidField)
		}
		segs, err := p.table(pr.SegmentData)
		if err != nil {
			return File{}, err
		}
		f.Par2 = append(f.Par2, Par2File{Name: uniqueName(SanitizeFilename(pr.Filename), taken), Size: pr.FileSize, Segments: segs})
	}
	return f, nil
}

func nzbPool(files [][]string) (*poolBuilder, []int, error) {
	if len(files) == 0 || len(files) > MaxReleaseFiles {
		if len(files) == 0 {
			return nil, nil, fmt.Errorf("%w: the nzb scheme needs the release segment list", ErrBadPool)
		}
		return nil, nil, fmt.Errorf("%w: %d release files", ErrLimit, len(files))
	}
	p := &poolBuilder{nzb: true, idx: map[string]int64{}}
	counts := make([]int, len(files))
	for i, f := range files {
		for _, raw := range f {
			if holes.IsPlaceholderID(raw) {
				continue
			}
			id, err := CanonicalMessageID(raw)
			if err != nil {
				return nil, nil, err
			}
			if len(p.pool) >= MaxSegments {
				return nil, nil, fmt.Errorf("%w: release segments", ErrLimit)
			}
			if _, ok := p.idx[id]; !ok {
				p.idx[id] = int64(len(p.pool))
			}
			p.pool = append(p.pool, id)
			counts[i]++
		}
	}
	return p, counts, nil
}

// releaseIDOfList computes the nzb-scheme Release ID from a pool and counts.
func releaseIDOfList(pool []string, counts []int) (contentid.ID, error) {
	store := &metapb.NzbStore{}
	pos := 0
	for _, n := range counts {
		fe := &metapb.NzbFileEntry{}
		for _, id := range pool[pos : pos+n] {
			fe.Segments = append(fe.Segments, &metapb.NzbSeg{Id: id})
		}
		pos += n
		store.Files = append(store.Files, fe)
	}
	return contentid.ReleaseID(store)
}

// FromLocal builds a bundle from a finished local import. It returns a
// *NotShareableError (errors.Is ErrNotShareable) when any file cannot be
// shared, and a typed validation error when the local metadata is itself
// inconsistent. Files are ordered by File ID and named deterministically, so
// the result does not depend on the order of in.Files. Nothing in the input
// is modified or retained.
func FromLocal(in ReleaseInput) (*Bundle, error) {
	if in.Scheme != SchemeNZB && in.Scheme != SchemeFiles {
		return nil, fmt.Errorf("%w: %d", ErrUnknownScheme, in.Scheme)
	}
	title := SanitizeTitle(in.Title)
	if title == "" {
		return nil, fmt.Errorf("%w: title is empty after sanitising", ErrInvalidField)
	}
	if in.PostedAt.Unix() < 1 {
		return nil, fmt.Errorf("%w: no post date", ErrInvalidField)
	}
	switch {
	case len(in.Files) == 0:
		return nil, fmt.Errorf("%w: no files", ErrInvalidField)
	case len(in.Files) > MaxFiles:
		return nil, fmt.Errorf("%w: %d files", ErrLimit, len(in.Files))
	}
	prepared := make([]preparedFile, len(in.Files))
	for i := range in.Files {
		fi := &in.Files[i]
		if fi.Meta == nil {
			return nil, fmt.Errorf("%w: file %d has no metadata", ErrInvalidField, i)
		}
		name := fi.Name
		if name == "" {
			name = fi.Meta.OriginalFilename
		}
		name = SanitizeFilename(name)
		if err := checkShareable(fi, name); err != nil {
			return nil, err
		}
		id, err := contentid.FileID(fi.Meta)
		if err != nil {
			return nil, fmt.Errorf("file %q: %w", name, mapIDError(err))
		}
		prepared[i] = preparedFile{in: fi, name: name, id: id}
	}
	sort.SliceStable(prepared, func(a, b int) bool { return string(prepared[a].id[:]) < string(prepared[b].id[:]) })
	taken := map[string]struct{}{}
	for i := range prepared {
		if i > 0 && prepared[i].id == prepared[i-1].id {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateFile, prepared[i].id)
		}
		prepared[i].name = uniqueName(prepared[i].name, taken)
	}

	b := &Bundle{Scheme: in.Scheme, Title: title, PostedUnix: in.PostedAt.Unix(), NZBPassword: in.NZBPassword}
	p := &poolBuilder{idx: map[string]int64{}}
	if in.Scheme == SchemeNZB {
		var counts []int
		var err error
		if p, counts, err = nzbPool(in.NZBFiles); err != nil {
			return nil, err
		}
		b.ReleaseFiles = counts
	}
	ids := make([]contentid.ID, len(prepared))
	for i, pf := range prepared {
		f, err := p.file(pf)
		if err != nil {
			return nil, fmt.Errorf("file %q: %w", pf.name, err)
		}
		b.Files = append(b.Files, f)
		ids[i] = pf.id
		b.TotalSize += f.Size
	}
	b.Pool = p.pool
	switch in.Scheme {
	case SchemeNZB:
		rid, err := releaseIDOfList(b.Pool, b.ReleaseFiles)
		if err != nil {
			return nil, mapIDError(err)
		}
		b.ReleaseID = rid
	default:
		b.ReleaseID = FilesReleaseID(ids)
	}
	if err := Validate(b); err != nil {
		return nil, err
	}
	return b, nil
}
