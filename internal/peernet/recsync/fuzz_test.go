package recsync

import (
	"bytes"
	"strings"
	"testing"
)

func FuzzDecodePage(f *testing.F) {
	good, _ := EncodePage(Page{Epoch: strings.Repeat("ab", 16), HighestSeq: 7, More: true,
		Records: [][]byte{rec(10, 1), rec(300, 2)}})
	empty, _ := EncodePage(Page{Epoch: strings.Repeat("0", 16)})
	f.Add(good)
	f.Add(empty)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := DecodePage(b)
		if err != nil {
			return
		}
		// Anything accepted must re-encode to the same bytes and honour the caps.
		if len(p.Records) > PageRecords {
			t.Fatalf("%d records", len(p.Records))
		}
		for _, r := range p.Records {
			if len(r) == 0 || len(r) > 2048 {
				t.Fatalf("record length %d", len(r))
			}
		}
		again, err := EncodePage(p)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("not canonical: %v", err)
		}
	})
}

func FuzzDecodePush(f *testing.F) {
	good, _ := EncodePush([][]byte{rec(10, 1), rec(300, 2)})
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x01, 0x2a, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		recs, err := DecodePush(b)
		if err != nil {
			return
		}
		if len(recs) == 0 || len(recs) > MaxPushRecords {
			t.Fatalf("%d records", len(recs))
		}
		again, err := EncodePush(recs)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("not canonical: %v", err)
		}
	})
}

func FuzzDecodePushResult(f *testing.F) {
	f.Add(EncodePushResult(PushResult{Accepted: 1, Duplicate: 2, Rejected: 3}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := DecodePushResult(b)
		if err != nil {
			return
		}
		if !bytes.Equal(EncodePushResult(r), b) {
			t.Fatal("not canonical")
		}
	})
}
