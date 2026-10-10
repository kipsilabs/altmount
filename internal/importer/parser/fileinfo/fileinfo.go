package fileinfo

import (
	"crypto/md5"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kipsilabs/altmount/internal/importer/parser/par2"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
)

var (
	obfMD5Pattern     = regexp.MustCompile(`^[a-f0-9]{32}$`)
	obfLongHexPattern = regexp.MustCompile(`^[a-f0-9.]{40,}$`)
	obfHexPattern     = regexp.MustCompile(`[a-f0-9]{30}`)
	obfAbcXyzPattern  = regexp.MustCompile(`^abc\.xyz`)

	// Multi-volume archive suffixes are never obfuscated: .partNN.rar, .rNN, .NNN
	archiveVolumePattern = regexp.MustCompile(`(?i)(?:\.part\d+\.rar|\.r\d{2,3}|\.\d{2,3})$`)
)

// GetFileInfos extracts file information from NZB files with first segment data
// Similar to C# GetFileInfosStep.GetFileInfos
func GetFileInfos(
	files []*NzbFileWithFirstSegment,
	par2Descriptors map[[16]byte]*par2.FileDescriptor,
	nzbFilename string,
) []*FileInfo {
	// Strip .nzb extension for use as last-resort filename stem
	nzbStem := nzbtrim.TrimNzbExtension(nzbFilename)

	fileInfos := make([]*FileInfo, 0, len(files))
	for _, file := range files {
		info := getFileInfo(file, par2Descriptors, nzbStem)
		fileInfos = append(fileInfos, info)
	}

	return fileInfos
}

// getFileInfo extracts information for a single file
func getFileInfo(
	file *NzbFileWithFirstSegment,
	hashToDescMap map[[16]byte]*par2.FileDescriptor,
	nzbFilenameStem string,
) *FileInfo {
	par2Filename := ""
	par2FileSize := int64(0)

	if len(hashToDescMap) > 0 {
		// Gap 1: PAR2 Hash16k is MD5 of the first 16384 bytes, zero-padded if file is shorter.
		// Without zero-padding, md5.Sum(file.First16KB) produces a wrong hash for files < 16KB.
		padded := make([]byte, 16384)
		copy(padded, file.First16KB)
		md5Hash := md5.Sum(padded)

		desc, ok := hashToDescMap[md5Hash]
		if ok {
			par2Filename = desc.Name
			par2FileSize = int64(desc.Length)
		}
	}

	subjectFilename := file.NzbFile.Filename

	headerFilename := ""
	if file.Headers != nil {
		headerFilename = file.Headers.FileName
	}

	// Select best filename using priority system (PAR2 > subject > yEnc header > subject header)
	filename := selectBestFilename(par2Filename, subjectFilename, headerFilename, file.SubjectHeader)
	// Media extensions score higher than sidecars, but a sidecar must not consume
	// the video's name when an encoder reused it in the yEnc header.
	fromSidecar := false
	if isSidecarFilename(par2Filename) {
		filename, fromSidecar = par2Filename, true
	} else if par2Filename == "" && isSafeSubjectSidecar(file) {
		filename, fromSidecar = subjectFilename, true
	}
	fromPar2 := par2Filename != "" && filename == par2Filename

	// Gap 4: Correct extension based on magic bytes when filename appears obfuscated.
	// This handles files that were uploaded with a wrong or missing extension.
	if !fromSidecar {
		filename = correctExtensionFromMagicBytes(filename, file.First16KB)
	}

	// Gap 5: Last resort — use NZB filename stem when all other sources are obfuscated/empty.
	// A PAR2-sourced name is authoritative, so trust it even when the obfuscation heuristic
	// flags it (e.g. a short base like "yay.rar"); overriding it would split a multi-volume
	// RAR set whose first volume PAR2 named differently from its .rNN continuations.
	if !fromPar2 && !fromSidecar && nzbFilenameStem != "" && (filename == "" || isProbablyObfuscated(filename)) {
		ext := filepath.Ext(filename)
		// An NZB named after its file ("The.Movie.mkv.nzb") already carries the
		// extension in the stem; don't double it.
		if ext != "" && !strings.EqualFold(filepath.Ext(nzbFilenameStem), ext) {
			filename = nzbFilenameStem + ext
		} else {
			filename = nzbFilenameStem
		}
	}

	// Determine file size (PAR2 has highest priority)
	var fileSize *int64
	if par2FileSize > 0 {
		fileSize = &par2FileSize
	} else if file.Headers != nil && file.Headers.FileSize > 0 {
		size := int64(file.Headers.FileSize)
		fileSize = &size
	}

	// Detect RAR archives (by magic bytes or extension).
	// Also check subjectFilename: the obfuscation override (Gap 5) may reduce a compound
	// extension like ".7z.002" to just ".002", losing the archive-type signal.
	// subjectFilename is the raw NZB filename before any processing, so it is unaffected.
	isRar := HasRarMagic(file.First16KB) || IsRarFile(filename) || IsRarFile(subjectFilename)

	// Gap 3: Detect 7z archives by magic bytes or extension.
	// Same reasoning: check subjectFilename alongside the processed filename so that
	// split-archive parts beyond the first (.7z.002, .7z.003, …) are correctly identified
	// even when the obfuscation override stripped the ".7z" component from the extension.
	is7z := Has7zMagic(file.First16KB) || Is7zFile(filename) || Is7zFile(subjectFilename)

	// Check selected, subject, and header filenames — yEnc headers often omit the .par2 extension
	// (e.g. encoder stores "Movie.mkv" in the yEnc name= field for a "Movie.mkv.vol07+8.par2" segment)
	isPar2Archive := IsPar2File(filename) || IsPar2File(subjectFilename) || IsPar2File(headerFilename)

	return &FileInfo{
		NzbFile:       *file.NzbFile,
		Filename:      filename,
		ReleaseDate:   file.ReleaseDate,
		IsPar2Archive: isPar2Archive,
		FileSize:      fileSize,
		IsRar:         isRar,
		Is7z:          is7z,
		YencHeaders:   file.Headers,
		First16KB:     file.First16KB,
		OriginalIndex: file.OriginalIndex,
	}
}

func isSidecarFilename(filename string) bool {
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".nfo", ".sfv", ".txt", ".srt", ".sub", ".jpg", ".jpeg", ".png", ".nzb", ".md5":
		return true
	}
	return false
}

func isSafeSubjectSidecar(file *NzbFileWithFirstSegment) bool {
	// Unlike a matched PAR2 descriptor, a subject alone is not authoritative:
	// protect real media and archives, and require a known small decoded size.
	return isSidecarFilename(file.NzbFile.Filename) &&
		file.Headers != nil && file.Headers.FileSize > 0 && file.Headers.FileSize <= 32<<20 &&
		len(file.First16KB) > 0 && !IsRecognizedMediaContainer(file.First16KB) &&
		!HasRarMagic(file.First16KB) && !Has7zMagic(file.First16KB) && !par2.HasMagicBytes(file.First16KB)
}

// correctExtensionFromMagicBytes fixes the extension of an obfuscated file based on its magic bytes.
// Only applies when the filename looks obfuscated — clear names are never modified.
func correctExtensionFromMagicBytes(filename string, data []byte) string {
	if !isProbablyObfuscated(filename) {
		return filename
	}
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	if HasRarMagic(data) {
		return base + ".rar"
	}
	if Has7zMagic(data) {
		return base + ".7z"
	}
	return filename
}

// selectBestFilename selects the best filename using priority system
// Priority: PAR2 (3) > Subject (2) > yEnc header (1) > Subject header (0)
// With adjustments for obfuscation, important types, and extension length
func selectBestFilename(par2Filename, subjectFilename, headerFilename, subjectHeader string) string {
	type candidate struct {
		filename string
		priority int
	}

	candidates := []candidate{
		{filename: par2Filename, priority: getFilenamePriority(par2Filename, 3)},
		{filename: subjectFilename, priority: getFilenamePriority(subjectFilename, 2)},
		{filename: headerFilename, priority: getFilenamePriority(headerFilename, 1)},
		{filename: subjectHeader, priority: getFilenamePriority(subjectHeader, 0)},
	}

	// Find candidate with highest priority
	bestCandidate := candidate{filename: "", priority: -5000}
	for _, c := range candidates {
		if c.filename != "" && c.priority > bestCandidate.priority {
			bestCandidate = c
		}
	}

	return bestCandidate.filename
}

// getFilenamePriority calculates priority score for a filename
// Higher score = better filename
func getFilenamePriority(filename string, startingPriority int) int {
	priority := startingPriority

	// Empty filename gets very low priority
	if strings.TrimSpace(filename) == "" {
		return priority - 5000
	}

	// Obfuscated filenames get -1000 penalty
	if isProbablyObfuscated(filename) {
		priority -= 1000
	}

	// Important file types get +50 bonus
	if IsImportantFileType(filename) {
		priority += 50
	}

	// Valid extension length (2-4 chars) gets +10 bonus
	if HasValidExtensionLength(filename) {
		priority += 10
	}

	return priority
}

// IsProbablyObfuscated reports whether a filename is likely obfuscated. It is the
// exported form of isProbablyObfuscated, used by the parser to decide — before any
// network fetch — whether a file's NZB subject name is trustworthy enough to skip
// downloading its first segment.
func IsProbablyObfuscated(filename string) bool {
	return isProbablyObfuscated(filename)
}

// isProbablyObfuscated checks if a filename is likely obfuscated
// Based on SABnzbd's deobfuscation algorithm:
// https://github.com/sabnzbd/sabnzbd/blob/64034c5636563b66360aa9dfc1a0b624f4db5cc3/sabnzbd/deobfuscate_filenames.py#L105
func isProbablyObfuscated(filename string) bool {
	if filename == "" {
		return false
	}

	// Multi-volume archive suffixes (.part01.rar, .r00, .001) are legitimate naming
	// conventions, never obfuscated — guard before the heuristic below strips them.
	if archiveVolumePattern.MatchString(filename) {
		return false
	}

	// Extract base filename without extension
	// Find last dot position
	lastDot := strings.LastIndex(filename, ".")
	baseFilename := filename
	if lastDot > 0 {
		baseFilename = filename[:lastDot]
	}

	// ---
	// First: patterns that are certainly obfuscated

	// 32 hex digits (MD5-like hash)
	// Example: b082fa0beaa644d3aa01045d5b8d0b36.mkv
	if obfMD5Pattern.MatchString(baseFilename) {
		return true
	}

	// 40+ lowercase hex digits and/or dots
	// Example: 0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.6547
	if obfLongHexPattern.MatchString(baseFilename) {
		return true
	}

	// 30+ hex digits with 2+ sets of square brackets
	// Example: [BlaBla] something 5937bc5e32146e.bef89a622e4a23f07b0d3757ad5e8a.a02b264e [More]
	if obfHexPattern.MatchString(baseFilename) {
		bracketCount := strings.Count(baseFilename, "[")
		if bracketCount >= 2 {
			return true
		}
	}

	// Starts with 'abc.xyz' (common obfuscation pattern)
	// Example: abc.xyz.a4c567edbcbf27.BLA
	if obfAbcXyzPattern.MatchString(baseFilename) {
		return true
	}

	// ---
	// Then: patterns that are NOT obfuscated (typical, clear names)

	// Count character types
	decimals := 0
	upperChars := 0
	lowerChars := 0
	spacesDots := 0

	for _, char := range baseFilename {
		switch {
		case char >= '0' && char <= '9':
			decimals++
		case char >= 'A' && char <= 'Z':
			upperChars++
		case char >= 'a' && char <= 'z':
			lowerChars++
		case char == ' ' || char == '.' || char == '_':
			spacesDots++
		}
	}

	// Example: "Great Distro"
	// Has uppercase, lowercase, and space-like separators
	if upperChars >= 2 && lowerChars >= 2 && spacesDots >= 1 {
		return false
	}

	// Example: "this is a download"
	// Multiple spaces/dots/underscores indicate readable name
	if spacesDots >= 3 {
		return false
	}

	// Example: "Beast 2020"
	// Has letters, digits, and separators
	if (upperChars+lowerChars >= 4) && decimals >= 4 && spacesDots >= 1 {
		return false
	}

	// Example: "Catullus"
	// Starts with capital, mostly lowercase (typical proper noun/title)
	if len(baseFilename) > 0 {
		firstChar := rune(baseFilename[0])
		if firstChar >= 'A' && firstChar <= 'Z' && lowerChars > 2 {
			if upperChars == 0 || float64(upperChars)/float64(lowerChars) <= 0.25 {
				return false
			}
		}
	}

	// Finally: default to obfuscated
	return true
}

// IsPar2File checks if a filename is a PAR2 archive
func IsPar2File(filename string) bool {
	return strings.HasSuffix(strings.ToLower(filename), ".par2")
}
