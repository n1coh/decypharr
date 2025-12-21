package repair

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
)

func fileIsStrm(file string) bool {
	return strings.HasSuffix(strings.ToLower(file), ".strm")
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
	// Ignore 429 (Too Many Requests) - this is a temporary rate limit, not a broken file
	if resp.StatusCode == 429 {
		return nil // Consider file valid despite rate limit
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("URL returned status %d", resp.StatusCode)
	}

	return nil
}

func collectFiles(media arr.Content) map[string][]arr.ContentFile {
	uniqueParents := make(map[string][]arr.ContentFile)
	files := media.Files
	for _, file := range files {
		// For STRM files, we only use the filename from the API
		// No need to read the file from disk or check symlinks
		// Validation will be done by regenerating URL from cache
		if fileIsStrm(file.Path) {
			file.IsSymlink = false
			dir := filepath.Dir(file.Path)
			fileName := filepath.Base(file.Path)
			file.TargetPath = fileName
			uniqueParents[dir] = append(uniqueParents[dir], file)
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
