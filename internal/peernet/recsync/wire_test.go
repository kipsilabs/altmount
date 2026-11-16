package recsync

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func rec(n int, fill byte) []byte { return bytes.Repeat([]byte{fill}, n) }

func TestPageRoundTrip(t *testing.T) {
	in := Page{Epoch: strings.Repeat("ab", 16), HighestSeq: 1 << 40, More: true,
		Records: [][]byte{rec(10, 1), rec(2048, 2), rec(1, 3)}}
	b, err := EncodePage(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodePage(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Epoch != in.Epoch || out.HighestSeq != in.HighestSeq || !out.More || len(out.Records) != 3 ||
		!bytes.Equal(out.Records[1], in.Records[1]) {
		t.Fatalf("round trip mismatch: %+v", out)
	}
	empty, err := EncodePage(Page{Epoch: in.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if p, err := DecodePage(empty); err != nil || len(p.Records) != 0 || p.More || p.HighestSeq != 0 {
		t.Fatalf("empty page: %+v %v", p, err)
	}
}

func TestPageEncodeRejectsInvalid(t *testing.T) {
	good := strings.Repeat("ab", 16)
	cases := map[string]Page{
		"empty epoch":      {},
		"bad epoch chars":  {Epoch: strings.Repeat("A", 16)},
		"short epoch":      {Epoch: "ab"},
		"negative seq":     {Epoch: good, HighestSeq: -1},
		"empty record":     {Epoch: good, Records: [][]byte{{}}},
		"oversize record":  {Epoch: good, Records: [][]byte{rec(2049, 1)}},
		"too many records": {Epoch: good, Records: make([][]byte, PageRecords+1)},
	}
	for name, p := range cases {
		for i := range p.Records {
			if p.Records[i] == nil {
				p.Records[i] = rec(1, 1)
			}
		}
		if _, err := EncodePage(p); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}

func TestDecodePageRejectsNonCanonicalAndHostile(t *testing.T) {
	good, err := EncodePage(Page{Epoch: strings.Repeat("ab", 16), HighestSeq: 5, Records: [][]byte{rec(4, 9)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePage(good); err != nil {
		t.Fatal(err)
	}
	var unknown []byte
	unknown = append(unknown, good...)
	unknown = protowire.AppendTag(unknown, 9, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)

	var dupVersion []byte
	dupVersion = protowire.AppendTag(dupVersion, 1, protowire.VarintType)
	dupVersion = protowire.AppendVarint(dupVersion, 1)
	dupVersion = append(dupVersion, good...)

	var badVersion []byte
	badVersion = protowire.AppendTag(badVersion, 1, protowire.VarintType)
	badVersion = protowire.AppendVarint(badVersion, 2)
	badVersion = append(badVersion, good[2:]...)

	var tooMany []byte
	tooMany = append(tooMany, good[:len(good)-6]...) // drop the record
	for i := 0; i < PageRecords+1; i++ {
		tooMany = protowire.AppendTag(tooMany, 5, protowire.BytesType)
		tooMany = protowire.AppendBytes(tooMany, rec(1, 1))
	}
	huge := protowire.AppendTag(nil, 5, protowire.BytesType)
	huge = protowire.AppendVarint(huge, 1<<40)

	cases := map[string][]byte{
		"empty":           nil,
		"truncated":       good[:len(good)-1],
		"trailing":        append(append([]byte{}, good...), 0xff),
		"unknown field":   unknown,
		"duplicate field": dupVersion,
		"bad version":     badVersion,
		"too many":        tooMany,
		"huge length":     huge,
		"wrong type":      protowire.AppendTag(nil, 1, protowire.BytesType),
		"oversize body":   make([]byte, MaxPageBytes+1),
	}
	for name, b := range cases {
		if _, err := DecodePage(b); !errors.Is(err, ErrMalformedWire) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestPushRoundTripAndLimits(t *testing.T) {
	b, err := EncodePush([][]byte{rec(5, 1), rec(6, 2)})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := DecodePush(b)
	if err != nil || len(recs) != 2 || !bytes.Equal(recs[1], rec(6, 2)) {
		t.Fatalf("push: %v %v", recs, err)
	}
	if _, err := EncodePush(nil); err == nil {
		t.Error("empty push encoded")
	}
	many := make([][]byte, MaxPushRecords+1)
	for i := range many {
		many[i] = rec(1, 1)
	}
	if _, err := EncodePush(many); err == nil {
		t.Error("oversize push encoded")
	}
	var over []byte
	over = protowire.AppendTag(over, 1, protowire.VarintType)
	over = protowire.AppendVarint(over, 1)
	for range many {
		over = protowire.AppendTag(over, 5, protowire.BytesType)
		over = protowire.AppendBytes(over, rec(1, 1))
	}
	if _, err := DecodePush(over); !errors.Is(err, ErrMalformedWire) {
		t.Errorf("decoded %d records over the cap: %v", len(many), err)
	}
	// A page is not a push (it carries an epoch).
	page, _ := EncodePage(Page{Epoch: strings.Repeat("ab", 16)})
	if _, err := DecodePush(page); !errors.Is(err, ErrMalformedWire) {
		t.Errorf("page decoded as push: %v", err)
	}
}

func TestPushResultRoundTrip(t *testing.T) {
	in := PushResult{Accepted: 3, Duplicate: 4, Rejected: 5}
	out, err := DecodePushResult(EncodePushResult(in))
	if err != nil || out != in {
		t.Fatalf("got %+v %v", out, err)
	}
	if _, err := DecodePushResult([]byte{0xff}); !errors.Is(err, ErrMalformedWire) {
		t.Errorf("err = %v", err)
	}
	if _, err := DecodePushResult(append(EncodePushResult(in), 0)); !errors.Is(err, ErrMalformedWire) {
		t.Errorf("trailing byte accepted: %v", err)
	}
}
