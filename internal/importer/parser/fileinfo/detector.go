package fileinfo

import (
	"bytes"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// Video file extensions (common video formats)
	videoExtensions = map[string]bool{
		".webm": true, ".m4v": true, ".3gp": true, ".nsv": true, ".ty": true, ".strm": true,
		".rm": true, ".rmvb": true, ".m3u": true, ".ifo": true, ".mov": true, ".qt": true,
		".divx": true, ".xvid": true, ".bivx": true, ".nrg": true, ".pva": true, ".wmv": true,
		".asf": true, ".asx": true, ".ogm": true, ".ogv": true, ".m2v": true, ".avi": true,
		".bin": true, ".dat": true, ".dvr-ms": true, ".mpg": true, ".mpeg": true, ".mp4": true,
		".avc": true, ".vp3": true, ".svq3": true, ".nuv": true, ".viv": true, ".dv": true,
		".fli": true, ".flv": true, ".wpl": true, ".img": true, ".iso": true, ".vob": true,
		".mkv": true, ".mk3d": true, ".ts": true, ".wtv": true, ".m2ts": true,
	}

	// RAR file pattern: .rar, .r00/.r01…, the old-style rollover continuation
	// volumes .s00…/.t00…/… up to .z99 (the extension letter rolls r→s→…→z after
	// .r99), and .partNN.rar.
	rarPattern = regexp.MustCompile(`(?i)\.(?:rar|r\d+|[r-z]\d{2})$|\.part\d+\.rar$`)

	// 7z file pattern: .7z or .7z.001, .7z.002, etc.
	sevenZipPattern = regexp.MustCompile(`(?i)\.7z(\.(\d+))?$`)

	// Multipart MKV pattern: .mkv.001, .mkv.002, etc.
	multipartMkvPattern = regexp.MustCompile(`(?i)\.mkv\.(\d+)$`)
)

// HasRar4Magic checks if the data contains RAR 4.x magic bytes
func HasRar4Magic(data []byte) bool {
	return len(data) >= len(Rar4Magic) && bytes.Equal(data[:len(Rar4Magic)], Rar4Magic)
}

// HasRar5Magic checks if the data contains RAR 5.x magic bytes
func HasRar5Magic(data []byte) bool {
	return len(data) >= len(Rar5Magic) && bytes.Equal(data[:len(Rar5Magic)], Rar5Magic)
}

// HasRarMagic checks if the data contains any RAR magic bytes (4.x or 5.x)
func HasRarMagic(data []byte) bool {
	return HasRar4Magic(data) || HasRar5Magic(data)
}

// Has7zMagic checks if the data contains 7-Zip magic bytes
func Has7zMagic(data []byte) bool {
	return len(data) >= len(SevenZipMagic) && bytes.Equal(data[:len(SevenZipMagic)], SevenZipMagic)
}

// IsVideoFile checks if the filename is a video file based on extension
func IsVideoFile(filename string) bool {
	if filename == "" {
		return false
	}
	ext := strings.ToLower(filepath.Ext(filename))
	return videoExtensions[ext]
}

// IsRarFile checks if the filename is a RAR file based on extension pattern
func IsRarFile(filename string) bool {
	if filename == "" {
		return false
	}
	return rarPattern.MatchString(filename)
}

// Is7zFile checks if the filename is a 7z file based on extension pattern
func Is7zFile(filename string) bool {
	if filename == "" {
		return false
	}

	return sevenZipPattern.MatchString(filename)
}

// IsMultipartMkv checks if the filename is a multipart MKV file
func IsMultipartMkv(filename string) bool {
	if filename == "" {
		return false
	}
	return multipartMkvPattern.MatchString(filename)
}

// IsImportantFileType checks if the filename is an important file type
// (video, RAR, 7z, or multipart MKV)
func IsImportantFileType(filename string) bool {
	return IsVideoFile(filename) ||
		IsRarFile(filename) ||
		Is7zFile(filename) ||
		IsMultipartMkv(filename)
}

// audioExtensions lists file extensions eligible for audio content
// verification. Every entry was verified empirically to produce a
// mimetype.Detect-recognized signature within the first 512 bytes.
var audioExtensions = map[string]bool{
	".mp3": true, ".flac": true, ".ogg": true, ".aac": true,
	".m4a": true, ".wma": true, ".wav": true, ".aiff": true,
}

// verifiableVideoExtensions lists video-container extensions eligible for
// content signature verification. This is deliberately narrower than
// videoExtensions (which also covers Kodi-style "might be playable"
// extensions such as .iso, .strm, .ifo, .m3u, and raw/ambiguous streams
// like .bin/.dat): those formats either carry no magic number in the first
// 512 bytes (e.g. ISO 9660's CD001 sits at offset 32769) or are plain
// text/XML, so IsRecognizedMediaContainer can never confirm them and a
// false "not recognized" verdict there would destroy a healthy file. Every
// entry here was verified empirically against IsRecognizedMediaContainer.
var verifiableVideoExtensions = map[string]bool{
	".mkv": true, ".mk3d": true, ".webm": true, ".mp4": true, ".m4v": true,
	".mov": true, ".qt": true, ".avi": true, ".flv": true, ".ogv": true,
	".ogm": true, ".wmv": true, ".asf": true, ".3gp": true, ".ts": true,
	".m2ts": true, ".mpg": true, ".mpeg": true, ".vob": true,
}

// IsKnownMediaExtension identifies clear media filenames without reading content.
// It excludes ambiguous extensions such as .bin and .dat that may name archive parts.
func IsKnownMediaExtension(filename string) bool {
	if filename == "" {
		return false
	}
	ext := strings.ToLower(filepath.Ext(filename))
	return verifiableVideoExtensions[ext] || audioExtensions[ext]
}

// samplePattern matches scene-release sample/proof clips, which are
// legitimately short and would false-positive as truncated/invalid content.
// Matched only against the base filename — a directory named e.g.
// "Samples" elsewhere in the path must not disqualify an otherwise valid
// file (see /data/Samples/Movie.mkv, which is a real release layout).
var samplePattern = regexp.MustCompile(`(?i)sample`)

// IsVerifiableMediaFile reports whether filename is eligible for content
// signature verification: a video or audio file that is not a sample clip.
// PAR2, subtitle, .nfo, and other non-media sidecars are never eligible.
func IsVerifiableMediaFile(filename string) bool {
	if filename == "" {
		return false
	}
	if samplePattern.MatchString(filepath.Base(filename)) {
		return false
	}
	return IsKnownMediaExtension(filename)
}

// HasValidExtensionLength checks if the extension length is between 2 and 4 characters
// (considered a valid/common extension length)
func HasValidExtensionLength(filename string) bool {
	if filename == "" {
		return false
	}
	ext := filepath.Ext(filename)
	if ext == "" {
		return false
	}
	// Remove the leading dot
	extLen := len(ext) - 1
	return extLen >= 2 && extLen <= 4
}
