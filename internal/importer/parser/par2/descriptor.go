package par2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/javi11/nzbparser"
)

// MaxIndexSegments is the upper bound for treating a PAR2 file as an index file
// (no recovery blocks). Recovery block files have many more segments.
const MaxIndexSegments = 5

// nzbSegmentLoader adapts []nzbparser.NzbSegment into usenet.SegmentLoader.
// Each raw NZB segment maps with Start=0, End=Bytes-1 (all data is usable).
type nzbSegmentLoader struct {
	segs   nzbparser.NzbSegments
	groups []string
}

func (l nzbSegmentLoader) GetSegment(index int) (usenet.Segment, []string, bool) {
	if index < 0 || index >= len(l.segs) {
		return usenet.Segment{}, nil, false
	}
	seg := l.segs[index]
	return usenet.Segment{
		Id:    seg.ID,
		Start: 0,
		End:   int64(seg.Bytes) - 1,
		Size:  int64(seg.Bytes),
	}, l.groups, true
}

// FirstSegmentData holds cached data from the first segment of an NZB file
// This is passed from the parser to avoid redundant fetches
type FirstSegmentData struct {
	File     *nzbparser.NzbFile
	RawBytes []byte // Up to 16KB for PAR2 detection
}

// GetFileDescriptors extracts file descriptors from PAR2 files in the NZB
// Similar to C# GetPar2FileDescriptorsStep.GetPar2FileDescriptors
// Uses cached first segment data and streams through the PAR2 file
func GetFileDescriptors(
	ctx context.Context,
	firstSegmentCache []*FirstSegmentData,
	poolManager pool.Manager,
	store usenet.SegmentStore,
) (map[[16]byte]*FileDescriptor, error) {
	descriptors := make(map[[16]byte]*FileDescriptor)

	if poolManager == nil || !poolManager.HasPool() {
		slog.DebugContext(ctx, "No pool manager available for PAR2 extraction")
		return descriptors, nil
	}

	// Read all small PAR2 files (index files) and merge their descriptors.
	// Index files never contain recovery blocks so they have few segments (≤ MaxIndexSegments).
	// Recovery block files have many segments and are skipped.
	// Merging handles releases with multiple PAR2 sets (e.g. main archive + sample).
	for _, cachedData := range firstSegmentCache {
		if cachedData == nil || cachedData.File == nil || len(cachedData.File.Segments) == 0 {
			continue
		}
		if !HasMagicBytes(cachedData.RawBytes) {
			continue
		}
		if len(cachedData.File.Segments) > MaxIndexSegments {
			continue // Skip large recovery block files
		}
		fileDescriptors, err := readFileDescriptors(ctx, cachedData.File, poolManager, store)
		if err != nil {
			slog.DebugContext(ctx, "Failed to read PAR2 file descriptors, skipping",
				"error", err, "segments", len(cachedData.File.Segments))
			continue
		}
		mergeFileDescriptors(descriptors, fileDescriptors)
	}

	return descriptors, nil
}

// mergeFileDescriptors adds descs to descriptors by Hash16k. The same file seen
// again in another PAR2 file is ignored; a different file with the same first
// 16KB and length is kept on SameHead.
func mergeFileDescriptors(descriptors map[[16]byte]*FileDescriptor, descs []FileDescriptor) {
	for i := range descs {
		desc := &descs[i]
		first, exists := descriptors[desc.Hash16k]
		if !exists {
			descriptors[desc.Hash16k] = desc
			continue
		}
		if first.FileID == desc.FileID || first.Length != desc.Length {
			continue
		}
		known := slices.ContainsFunc(first.SameHead, func(d *FileDescriptor) bool { return d.FileID == desc.FileID })
		if !known {
			first.SameHead = append(first.SameHead, desc)
		}
	}
}

// readFileDescriptors streams through a PAR2 file and extracts all file descriptors
// Similar to C# Par2.ReadFileDescriptions
// This function reads ALL segments of the PAR2 file sequentially to find all FileDesc packets
func readFileDescriptors(
	ctx context.Context,
	par2File *nzbparser.NzbFile,
	poolManager pool.Manager,
	store usenet.SegmentStore,
) ([]FileDescriptor, error) {
	var descriptors []FileDescriptor

	if len(par2File.Segments) == 0 {
		return descriptors, fmt.Errorf("PAR2 file has no segments")
	}

	// Create context with timeout (30s per segment, capped at 90s ceiling).
	// Capping prevents runaway waits on large index files where the real cost
	// is dominated by latency, not sequential segment fetches.
	timeout := min(time.Second*30*time.Duration(len(par2File.Segments)), 90*time.Second)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Build segment loader and compute total size
	loader := nzbSegmentLoader{segs: par2File.Segments, groups: par2File.Groups}
	var totalSize int64
	for _, seg := range par2File.Segments {
		totalSize += int64(seg.Bytes)
	}

	// Create UsenetReader (provides retry, prefetch, and metrics for free)
	rg := usenet.GetSegmentsInRange(ctx, 0, totalSize-1, loader)
	r, err := usenet.NewUsenetReader(ctx, poolManager.GetPool, rg, 5, poolManager, "", store,
		usenet.WithImportProfile(poolManager))
	if err != nil {
		return descriptors, fmt.Errorf("failed to create usenet reader: %w", err)
	}
	defer r.Close()

	// Create packet reader for streaming across all segments
	packetReader := NewPacketReader(r)

	// Read packets until we hit an error or reach the end
	// Since we're now reading all segments, we may have many more packets
	// Increase limit to accommodate larger PAR2 files with many FileDesc packets
	maxPackets := 1000 // Limit the number of packets to process
	packetCount := 0
	// Once FileDesc packets stop appearing, PAR2 index files typically have no
	// more ahead. Break after a window of non-FileDesc packets past the last
	// descriptor to avoid draining the entire index unnecessarily.
	const noNewDescWindow = 50
	packetsSinceLastDesc := 0
	var lastError error

	for packetCount < maxPackets {
		select {
		case <-ctx.Done():
			return descriptors, ctx.Err()
		default:
		}

		// Read packet header
		header, err := packetReader.ReadHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			if len(descriptors) == 0 {
				return descriptors, fmt.Errorf("corrupted PAR2 file: failed to read packet header: %w", err)
			}
			slog.DebugContext(ctx, "Corrupted packet header encountered, returning partial PAR2 descriptors", "error", err, "descriptors_found", len(descriptors))
			break
		}

		packetCount++

		// Check if this is a FileDesc packet
		if header.Type == PacketTypeFileDesc {
			// Read and parse the file descriptor
			desc, err := packetReader.ReadFileDescriptor(header)
			if err != nil {
				slog.DebugContext(ctx, "Failed to read file descriptor from corrupted packet", "error", err)
				lastError = err
				continue
			}

			descriptors = append(descriptors, *desc)
			packetsSinceLastDesc = 0
		} else {
			// Skip non-FileDesc packets
			if err := packetReader.SkipPacketBody(header); err != nil {
				if len(descriptors) == 0 {
					return descriptors, fmt.Errorf("corrupted PAR2 file: failed to skip packet body: %w", err)
				}
				slog.DebugContext(ctx, "Corrupted packet body encountered, returning partial PAR2 descriptors", "error", err, "descriptors_found", len(descriptors))
				break
			}
			if len(descriptors) > 0 {
				packetsSinceLastDesc++
				if packetsSinceLastDesc >= noNewDescWindow {
					slog.DebugContext(ctx, "No new FileDesc packets in window, ending PAR2 scan early",
						"descriptors_found", len(descriptors),
						"window", noNewDescWindow)
					break
				}
			}
		}
	}

	if len(descriptors) == 0 && lastError != nil {
		return descriptors, fmt.Errorf("corrupted PAR2 file: failed to extract any file descriptors: %w", lastError)
	}

	return descriptors, nil
}
