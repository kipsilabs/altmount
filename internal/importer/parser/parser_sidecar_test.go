package parser

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v5"
	"github.com/javi11/nzbparser"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
)

// Sidecars posted with the video's yEnc name must retain their own subject
// name, so later import filename deduplication cannot rename the real video.
func TestParseNzbSidecarDoesNotConsumeVideoFilename(t *testing.T) {
	const videoName = "Release.Movie.2024.mkv"
	for _, sidecarFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "sidecar first", false: "video first"}[sidecarFirst], func(t *testing.T) {
			fp := fakepool.New()
			sidecar := nzbparser.NzbFile{Filename: videoName + ".nfo", Segments: nzbparser.NzbSegments{{Number: 1, ID: "nfo", Bytes: 200}}}
			video := nzbparser.NzbFile{Filename: videoName + ".txt", Segments: nzbparser.NzbSegments{{Number: 1, ID: "video", Bytes: 2000}}}
			fp.SetBehavior("nfo", fakepool.SegmentBehavior{Bytes: []byte("release information"), YEnc: nntppool.YEncMeta{FileName: videoName, FileSize: 19, PartSize: 19}})
			// A misleading sidecar subject must not rename actual MPEG-PS media.
			fp.SetBehavior("video", fakepool.SegmentBehavior{Bytes: append([]byte{0, 0, 1, 0xBA}, make([]byte, 1020)...), YEnc: nntppool.YEncMeta{FileName: videoName, FileSize: 1024, PartSize: 1024}})
			files := nzbparser.NzbFiles{sidecar, video}
			if !sidecarFirst {
				files = nzbparser.NzbFiles{video, sidecar}
			}
			parsed, err := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2)).ParseNzb(context.Background(), &nzbparser.Nzb{Files: files}, "release.nzb", nil, ParseOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(parsed.Files) != 2 {
				t.Fatalf("got %d files, want sidecar and video", len(parsed.Files))
			}
			names := make(map[string]int)
			for _, f := range parsed.Files {
				names[f.Filename]++
				if f.Filename == videoName && f.Size != 1024 {
					t.Errorf("video filename belongs to file of size %d, want 1024", f.Size)
				}
			}
			if names[videoName] != 1 || names[videoName+".nfo"] != 1 {
				t.Errorf("filenames = %v, want distinct video and sidecar names", names)
			}
		})
	}
}
