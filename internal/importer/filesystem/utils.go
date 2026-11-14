package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/kipsilabs/altmount/internal/importer/parser"
	"github.com/kipsilabs/altmount/internal/importer/utils/nzbtrim"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// CalculateVirtualDirectory determines the virtual directory path based on NZB file location
func CalculateVirtualDirectory(nzbPath, relativePath string) string {
	if relativePath == "" {
		return "/"
	}

	nzbPath = filepath.Clean(nzbPath)
	relativePath = filepath.Clean(relativePath)

	relPath, err := filepath.Rel(relativePath, nzbPath)
	if err != nil || strings.HasPrefix(relPath, "..") {
		if strings.HasPrefix(relativePath, "/") {
			return filepath.Clean(relativePath)
		}
		return "/" + strings.ReplaceAll(relativePath, string(filepath.Separator), "/")
	}

	relDir := filepath.Dir(relPath)
	if relDir == "." || relDir == "" {
		// If the file is at the root, return root
		// The processor will handle creating a folder if needed (e.g. for archives or multi-file NZBs)
		return "/"
	}

	// Ignore .nzbs folder if present (persistent storage)
	if strings.Contains(relDir, ".nzbs") {
		parts := strings.Split(relDir, string(filepath.Separator))
		filtered := make([]string, 0, len(parts))
		for _, p := range parts {
			if p != ".nzbs" {
				filtered = append(filtered, p)
			}
		}
		relDir = filepath.Join(filtered...)
	}

	if relDir == "." || relDir == "" {
		return "/"
	}

	virtualPath := "/" + strings.ReplaceAll(relDir, string(filepath.Separator), "/")
	return filepath.Clean(virtualPath)
}

// SeparateFiles separates files by type (regular, archive, PAR2) based on NZB type
func SeparateFiles(files []parser.ParsedFile, nzbType parser.NzbType) (regular, archive, par2 []parser.ParsedFile) {
	switch nzbType {
	case parser.NzbTypeRarArchive:
		for _, file := range files {
			if file.IsRarArchive {
				archive = append(archive, file)
			} else if file.IsPar2Archive || IsPar2File(file.Filename) {
				par2 = append(par2, file)
			} else {
				regular = append(regular, file)
			}
		}

	case parser.NzbType7zArchive:
		for _, file := range files {
			if file.IsPar2Archive || IsPar2File(file.Filename) {
				par2 = append(par2, file)
			} else {
				// When the NZB is a 7z archive, all non-par2 files are archive parts.
				// Only the first split part (.7z.001) contains the 7z magic bytes; subsequent
				// parts (.7z.002, .7z.003, …) do not, so per-file Is7zArchive detection is
				// unreliable. Use the NZB-level type instead.
				archive = append(archive, file)
			}
		}

	default:
		// For single file and multi-file types, just separate PAR2 files
		for _, file := range files {
			if file.IsPar2Archive || IsPar2File(file.Filename) {
				par2 = append(par2, file)
			} else {
				regular = append(regular, file)
			}
		}
	}

	return regular, archive, par2
}

// IsPar2File checks if a filename is a PAR2 repair file
func IsPar2File(filename string) bool {
	lower := strings.ToLower(filename)
	return strings.HasSuffix(lower, ".par2")
}

// EnsureDirectoryExists creates directory structure in the metadata filesystem
func EnsureDirectoryExists(virtualDir string, metadataService *metadata.MetadataService) error {
	if virtualDir == "/" {
		return nil
	}

	metadataDir := metadataService.GetMetadataDirectoryPath(virtualDir)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory %s: %w", metadataDir, err)
	}

	return nil
}

// CreateNzbFolder creates a folder named after the NZB file
func CreateNzbFolder(virtualDir, nzbFilename string, metadataService *metadata.MetadataService) (string, error) {
	nzbVirtualDir := NzbFolderPath(virtualDir, nzbFilename)

	if err := EnsureDirectoryExists(nzbVirtualDir, metadataService); err != nil {
		return "", err
	}

	return nzbVirtualDir, nil
}

// NzbFolderPath is the virtual folder CreateNzbFolder would create, without
// creating anything.
func NzbFolderPath(virtualDir, nzbFilename string) string {
	nzbBaseName := nzbtrim.TrimNzbExtension(nzbFilename)
	// Now, also strip the media file extension if it exists
	// Common media extensions: .mkv, .mp4, .avi, .flv, .wmv, .mov, .webm
	// This is not exhaustive, but covers common cases.
	mediaExtensions := []string{".mkv", ".mp4", ".avi", ".flv", ".wmv", ".mov", ".webm", ".ts", ".iso"}

	for _, ext := range mediaExtensions {
		if strings.HasSuffix(strings.ToLower(nzbBaseName), ext) {
			nzbBaseName = strings.TrimSuffix(nzbBaseName, ext)
			break
		}
	}

	nzbVirtualDir := filepath.Join(virtualDir, nzbBaseName)
	return strings.ReplaceAll(nzbVirtualDir, string(filepath.Separator), "/")
}

// CreateDirectoriesForFiles analyzes files and creates their parent directories
func CreateDirectoriesForFiles(virtualDir string, files []parser.ParsedFile, metadataService *metadata.MetadataService) error {
	// Collect unique directory paths
	dirs := make(map[string]bool)

	for _, file := range files {
		normalizedFilename := strings.ReplaceAll(file.Filename, "\\", "/")
		normalizedFilename = filepath.Clean(normalizedFilename)
		normalizedFilename = strings.TrimPrefix(normalizedFilename, "/")

		dir := filepath.ToSlash(filepath.Dir(normalizedFilename))
		name := filepath.Base(normalizedFilename)

		// Check for redundant nesting (e.g. file.mkv/file.mkv)
		// If the last directory component matches the filename, flatten the structure
		// Also check without extension for cases like Movie/Movie.mkv
		nameWithoutExt := strings.TrimSuffix(name, filepath.Ext(name))
		if filepath.Base(dir) == name || filepath.Base(dir) == nameWithoutExt {
			dir = filepath.ToSlash(filepath.Dir(dir))
		}

		// Flatten redundant nesting against parent directory (same-level duplicate names)
		parentDirName := filepath.Base(virtualDir)
		if dir == parentDirName {
			dir = "."
		} else if after, ok := strings.CutPrefix(dir, parentDirName+"/"); ok {
			dir = after
		}

		if dir != "." && dir != "/" {
			virtualPath := filepath.Join(virtualDir, dir)
			virtualPath = strings.ReplaceAll(virtualPath, string(filepath.Separator), "/")
			dirs[virtualPath] = true
		}
	}

	// Create all directories
	for dir := range dirs {
		if err := EnsureDirectoryExists(dir, metadataService); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// DetermineFileLocation determines where a file should be placed in the virtual structure
func DetermineFileLocation(file parser.ParsedFile, baseDir string) (parentPath, filename string) {
	normalizedFilename := strings.ReplaceAll(file.Filename, "\\", "/")
	normalizedFilename = filepath.Clean(normalizedFilename)
	normalizedFilename = strings.TrimPrefix(normalizedFilename, "/")

	dir := filepath.ToSlash(filepath.Dir(normalizedFilename))
	name := filepath.Base(normalizedFilename)

	// Check for redundant nesting (e.g. file.mkv/file.mkv)
	// If the last directory component matches the filename, flatten the structure
	// Also check without extension for cases like Movie/Movie.mkv
	nameWithoutExt := strings.TrimSuffix(name, filepath.Ext(name))
	if filepath.Base(dir) == name || filepath.Base(dir) == nameWithoutExt {
		dir = filepath.ToSlash(filepath.Dir(dir))
	}

	// Flatten redundant nesting against parent directory (same-level duplicate names)
	parentDirName := filepath.Base(baseDir)
	if dir == parentDirName {
		dir = "."
	} else if after, ok := strings.CutPrefix(dir, parentDirName+"/"); ok {
		dir = after
	}

	if dir == "." || dir == "/" {
		return baseDir, name
	}

	virtualPath := filepath.Join(baseDir, dir)
	virtualPath = strings.ReplaceAll(virtualPath, string(filepath.Separator), "/")
	return virtualPath, name
}

// EnsureUniqueVirtualPath returns a path that is safe to write to.
// If a healthy metadata file already exists at virtualPath, appends _1, _2, …
// to the stem (before the extension) until an unused slot is found.
// Non-healthy metadata is treated as available to overwrite, so the original
// path is returned unchanged.
func EnsureUniqueVirtualPath(virtualPath string, ms *metadata.MetadataService) string {
	if !isHealthyMetadata(virtualPath, ms) {
		return virtualPath
	}
	ext := filepath.Ext(virtualPath)
	stem := strings.TrimSuffix(virtualPath, ext)
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s_%d%s", stem, i, ext)
		if !isHealthyMetadata(candidate, ms) {
			return candidate
		}
	}
}

func isHealthyMetadata(virtualPath string, ms *metadata.MetadataService) bool {
	meta, err := ms.ReadFileMetadata(virtualPath)
	return err == nil && meta != nil && meta.Status == metapb.FileStatus_FILE_STATUS_HEALTHY
}

// PathReserver assigns unique virtual paths within a single import batch,
// skipping both paths already claimed by sibling goroutines and healthy
// on-disk metadata.
//
// When many files collide to the same name (e.g. obfuscated Blu-ray BDMV posts
// where every file inherits the release name), re-probing _1.._k for each file
// is O(N^2) — and when that probing runs under a single shared lock it also
// serializes the whole worker pool. PathReserver keeps a per-base "next index"
// high-water mark so each colliding file claims the next free suffix in O(1)
// amortized time, behind a short-lived internal lock. Safe for concurrent use.
type PathReserver struct {
	ms      *metadata.MetadataService
	mu      sync.Mutex
	claimed map[string]struct{} // final paths claimed but not yet on disk
	nextIdx map[string]int      // desired base path -> next suffix index to try
}

// NewPathReserver creates a reserver backed by the given metadata service.
func NewPathReserver(ms *metadata.MetadataService) *PathReserver {
	return &PathReserver{
		ms:      ms,
		claimed: make(map[string]struct{}),
		nextIdx: make(map[string]int),
	}
}

// Reserve claims and returns a unique virtual path for desired, appending
// _1, _2, … only as needed. The caller must Release the returned path once it
// is durably written (or its write has failed).
func (r *PathReserver) Reserve(desired string) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, taken := r.claimed[desired]; !taken && !isHealthyMetadata(desired, r.ms) {
		r.claimed[desired] = struct{}{}
		return desired
	}

	ext := filepath.Ext(desired)
	stem := strings.TrimSuffix(desired, ext)
	i := max(r.nextIdx[desired], 1)
	for ; ; i++ {
		candidate := fmt.Sprintf("%s_%d%s", stem, i, ext)
		if _, taken := r.claimed[candidate]; taken {
			continue
		}
		if isHealthyMetadata(candidate, r.ms) {
			continue
		}
		r.claimed[candidate] = struct{}{}
		// Advance the high-water mark so sibling goroutines start probing past
		// this slot instead of re-scanning _1.._i. The mark is never rewound:
		// a written path is caught by the on-disk check above, and a failed
		// one simply leaves an unused suffix.
		r.nextIdx[desired] = i + 1
		return candidate
	}
}

// Release drops the in-batch claim for path once it is durably on disk.
func (r *PathReserver) Release(path string) {
	r.mu.Lock()
	delete(r.claimed, path)
	r.mu.Unlock()
}

// ReservedFolderNames are folder names the application uses for its own
// bookkeeping inside the library; no peer-supplied title may become one.
var ReservedFolderNames = []string{".nzbs", "corrupted_metadata", "patches", "_migrated", ".peernet-staging"}

// maxSafeFolderNameLen caps a peer-derived folder name in bytes.
const maxSafeFolderNameLen = 200

// SafeFolderName turns an untrusted title into one plain folder name. The media
// extension is stripped first (as NzbFolderPath does later), then control and
// path characters are replaced, leading and trailing dots and spaces removed
// and the length capped. A name that ends up empty, starts with a dot, or
// equals (ignoring case) a reserved internal name or one of extraReserved (for
// example the category folders) is refused. Unlike NzbFolderPath it can never
// yield "." or "..".
func SafeFolderName(title string, extraReserved ...string) (string, bool) {
	name := strings.ToValidUTF8(title, "")
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	if nzbtrim.HasNzbExtension(name) {
		name = nzbtrim.TrimNzbExtension(name)
	}
	lower := strings.ToLower(name)
	for _, ext := range []string{".mkv", ".mp4", ".avi", ".flv", ".wmv", ".mov", ".webm", ".ts", ".iso"} {
		if strings.HasSuffix(lower, ext) {
			name = name[:len(name)-len(ext)]
			break
		}
	}
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || strings.ContainsRune(`<>:"|?*`, r) {
			return ' '
		}
		return r
	}, name)
	name = strings.Join(strings.Fields(name), " ")
	if strings.HasPrefix(name, ".") {
		return "", false
	}
	for len(name) > maxSafeFolderNameLen {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	name = strings.TrimRight(name, ". ")
	if name == "" || strings.HasPrefix(name, ".") {
		return "", false
	}
	for _, r := range append(append([]string{}, ReservedFolderNames...), extraReserved...) {
		if r != "" && strings.EqualFold(name, r) {
			return "", false
		}
	}
	return name, true
}
