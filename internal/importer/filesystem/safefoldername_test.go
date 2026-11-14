package filesystem

import "testing"

// The result of SafeFolderName is itself a safe folder name: applying the
// function again must not change it (a later NzbFolderPath pass strips the
// same extensions).
func TestSafeFolderNameIsAFixedPoint(t *testing.T) {
	for _, in := range []string{"Movie.mkv.", "Movie.mkv ", "Movie.mkv.mkv.", "A.mp4. .", "x.ts.iso.", "Plain Title", "Dots...", " a  b "} {
		once, ok := SafeFolderName(in)
		if !ok {
			continue
		}
		twice, ok2 := SafeFolderName(once)
		if !ok2 || twice != once {
			t.Errorf("SafeFolderName(%q) = %q, again = %q (ok=%v)", in, once, twice, ok2)
		}
	}
}
