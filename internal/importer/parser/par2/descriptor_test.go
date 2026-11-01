package par2

import "testing"

func TestMergeFileDescriptors(t *testing.T) {
	hash := [16]byte{1}
	descs := map[[16]byte]*FileDescriptor{}
	mergeFileDescriptors(descs, []FileDescriptor{
		{FileID: [16]byte{10}, Hash16k: hash, Length: 100, Name: "a.part01.rar"},
		{FileID: [16]byte{11}, Hash16k: hash, Length: 100, Name: "a.part02.rar"},
		{FileID: [16]byte{12}, Hash16k: hash, Length: 50, Name: "shorter.rar"},
		{FileID: [16]byte{13}, Hash16k: [16]byte{2}, Length: 100, Name: "other.rar"},
	})
	// The same set read from a second PAR2 file adds nothing.
	mergeFileDescriptors(descs, []FileDescriptor{
		{FileID: [16]byte{10}, Hash16k: hash, Length: 100, Name: "a.part01.rar"},
		{FileID: [16]byte{11}, Hash16k: hash, Length: 100, Name: "a.part02.rar"},
	})

	first := descs[hash]
	if first == nil || first.Name != "a.part01.rar" {
		t.Fatalf("first = %+v, want a.part01.rar", first)
	}
	if len(first.SameHead) != 1 || first.SameHead[0].Name != "a.part02.rar" {
		t.Fatalf("SameHead = %+v, want only a.part02.rar", first.SameHead)
	}
	if descs[[16]byte{2}] == nil {
		t.Fatal("other hash lost")
	}
}
