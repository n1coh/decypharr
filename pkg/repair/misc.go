package repair

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
)

func fileIsSymlinked(file string) bool {
	info, err := os.Lstat(file)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

func getSymlinkTarget(file string) string {
	if fileIsSymlinked(file) {
		target, err := os.Readlink(file)
		if err != nil {
			return ""
		}
		if !filepath.IsAbs(target) {
			dir := filepath.Dir(file)
			target = filepath.Join(dir, target)
		}
		return target
	}
	return ""
}

func fileIsStrm(file string) bool {
	return strings.HasSuffix(strings.ToLower(file), ".strm")
}

func getStrmURL(file string) string {
	if !fileIsStrm(file) {
		return ""
	}

	content, err := os.ReadFile(file)
	if err != nil {
		return ""
	}

	// Return the URL from the file, trimming any whitespace
	return strings.TrimSpace(string(content))
}

// validateStrmURL checks if the URL in a .strm file is still valid by making a HEAD request
func validateStrmURL(url string) error {
	if url == "" {
		return fmt.Errorf("empty URL")
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Allow up to 3 redirects
			if len(via) >= 3 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	// Make HEAD request to check if URL is accessible
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach URL: %w", err)
	}
	defer resp.Body.Close()

	// Check HTTP status code
	if resp.StatusCode >= 400 {
		return fmt.Errorf("URL returned status %d", resp.StatusCode)
	}

	return nil
}

func fileIsReadable(filePath string) error {
	// First check if file exists and is accessible
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	// Check if it's a regular file
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}

	// Special handling for .strm files
	if fileIsStrm(filePath) {
		// Read the URL from the .strm file
		url := getStrmURL(filePath)
		if url == "" {
			return fmt.Errorf("strm file contains no URL")
		}

		// Validate that the URL is still accessible
		if err := validateStrmURL(url); err != nil {
			return fmt.Errorf("strm URL validation failed: %w", err)
		}

		return nil
	}

	// For non-.strm files, try to read the first 1024 bytes
	err = checkFileStart(filePath)
	if err != nil {
		return err
	}

	return nil
}

func checkFileStart(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	// Read first 1kb
	buffer := make([]byte, 1024)
	_, err = f.Read(buffer)
	if err != nil {
		return err
	}
	return nil
}

func collectFiles(media arr.Content) map[string][]arr.ContentFile {
	uniqueParents := make(map[string][]arr.ContentFile)
	files := media.Files
	for _, file := range files {
		// Check if it's a symlink
		target := getSymlinkTarget(file.Path)
		if target != "" {
			file.IsSymlink = true
			dir, f := filepath.Split(target)
			torrentNamePath := filepath.Clean(dir)
			// Set target path folder/file.mkv
			file.TargetPath = f
			uniqueParents[torrentNamePath] = append(uniqueParents[torrentNamePath], file)
			continue
		}

		// Check if it's a .strm file
		if fileIsStrm(file.Path) {
			strmURL := getStrmURL(file.Path)
			if strmURL != "" {
				file.IsSymlink = false
				// For .strm files, we'll use the URL as a marker
				// The parent folder will be extracted from the file's directory
				dir := filepath.Dir(file.Path)
				fileName := filepath.Base(file.Path)
				file.TargetPath = fileName
				uniqueParents[dir] = append(uniqueParents[dir], file)
			}
		}
	}
	return uniqueParents
}

func (r *Repair) checkTorrentFiles(torrentPath string, files []arr.ContentFile, clients map[string]common.Client, caches map[string]*store.Cache) []arr.ContentFile {
	brokenFiles := make([]arr.ContentFile, 0)

	emptyFiles := make([]arr.ContentFile, 0)

	r.logger.Debug().Msgf("Checking %s", torrentPath)

	// Get the debrid client
	dir := filepath.Dir(torrentPath)
	debridName := r.findDebridForPath(dir, clients)
	if debridName == "" {
		r.logger.Debug().Msgf("No debrid found for %s. Skipping", torrentPath)
		return emptyFiles
	}

	cache, ok := caches[debridName]
	if !ok {
		r.logger.Debug().Msgf("No cache found for %s. Skipping", debridName)
		return emptyFiles
	}
	tor, ok := r.torrentsMap.Load(debridName)
	if !ok {
		r.logger.Debug().Msgf("Could not find torrents for %s. Skipping", debridName)
		return emptyFiles
	}

	torrentsMap := tor.(map[string]store.CachedTorrent)

	// Check if torrent exists
	torrentName := filepath.Clean(filepath.Base(torrentPath))
	torrent, ok := torrentsMap[torrentName]
	if !ok {
		r.logger.Debug().Msgf("Can't find torrent %s in %s. Marking as broken", torrentName, debridName)
		// Return all files as broken
		return files
	}

	// Batch check files
	filePaths := make([]string, len(files))
	for i, file := range files {
		filePaths[i] = file.TargetPath
	}

	brokenFilePaths := cache.GetBrokenFiles(&torrent, filePaths)
	if len(brokenFilePaths) > 0 {
		r.logger.Debug().Msgf("%d broken files found in %s", len(brokenFilePaths), torrentName)

		// Create a set for O(1) lookup
		brokenSet := make(map[string]bool, len(brokenFilePaths))
		for _, brokenPath := range brokenFilePaths {
			brokenSet[brokenPath] = true
		}

		// Filter broken files
		for _, contentFile := range files {
			if brokenSet[contentFile.TargetPath] {
				brokenFiles = append(brokenFiles, contentFile)
			}
		}
	}

	return brokenFiles
}

func (r *Repair) findDebridForPath(dir string, clients map[string]common.Client) string {
	// Check cache first
	if debridName, exists := r.debridPathCache.Load(dir); exists {
		return debridName.(string)
	}

	r.logger.Debug().Str("dir", dir).Int("numClients", len(clients)).Msg("Finding debrid for path")

	// Find debrid client
	for _, client := range clients {
		mountPath := client.GetMountPath()
		if mountPath == "" {
			r.logger.Debug().Str("client", client.Name()).Msg("Client has no mount path")
			continue
		}

		cleanMount := filepath.Clean(mountPath)
		cleanDir := filepath.Clean(dir)

		r.logger.Debug().
			Str("client", client.Name()).
			Str("mountPath", cleanMount).
			Str("dir", cleanDir).
			Msg("Comparing paths")

		// Check if dir starts with mountPath (use HasPrefix for directory matching)
		if strings.HasPrefix(cleanDir, cleanMount) {
			debridName := client.Name()
			r.logger.Debug().Str("debrid", debridName).Msg("Found match using HasPrefix")

			// Cache the result
			r.debridPathCache.Store(dir, debridName)

			return debridName
		}
	}

	r.logger.Debug().Str("dir", dir).Msg("No debrid found for path")

	// Cache empty result to avoid repeated lookups
	r.debridPathCache.Store(dir, "")

	return ""
}
