package wire

import (
	"crypto/md5"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"

	"github.com/cavaliergopher/grab/v3"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// Multi-season detection patterns
var (
	// Pre-compiled patterns for multi-season replacement
	multiSeasonReplacements = []multiSeasonPattern{
		// S01-08 -> S01 (or whatever target season)
		{regexp.MustCompile(`(?i)S(\d{1,2})-\d{1,2}`), "S%02d"},

		// S01-S08 -> S01
		{regexp.MustCompile(`(?i)S(\d{1,2})-S\d{1,2}`), "S%02d"},

		// Season 1-8 -> Season 1
		{regexp.MustCompile(`(?i)Season\.?\s*\d{1,2}-\d{1,2}`), "Season %02d"},

		// Seasons 1-8 -> Season 1
		{regexp.MustCompile(`(?i)Seasons\.?\s*\d{1,2}-\d{1,2}`), "Season %02d"},

		// Complete Series -> Season X
		{regexp.MustCompile(`(?i)Complete\.?Series`), "Season %02d"},

		// All Seasons -> Season X
		{regexp.MustCompile(`(?i)All\.?Seasons?`), "Season %02d"},
	}

	// Also pre-compile other patterns
	seasonPattern     = regexp.MustCompile(`(?i)(?:season\.?\s*|s)(\d{1,2})`)
	qualityIndicators = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|BluRay|WEB-DL|HDTV|x264|x265|HEVC)`)

	multiSeasonIndicators = []*regexp.Regexp{
		regexp.MustCompile(`(?i)complete\.?series`),
		regexp.MustCompile(`(?i)all\.?seasons?`),
		regexp.MustCompile(`(?i)season\.?\s*\d+\s*-\s*\d+`),
		regexp.MustCompile(`(?i)s\d+\s*-\s*s?\d+`),
		regexp.MustCompile(`(?i)seasons?\s*\d+\s*-\s*\d+`),
	}
)

type multiSeasonPattern struct {
	pattern     *regexp.Regexp
	replacement string
}

type SeasonInfo struct {
	SeasonNumber int
	Files        []types.File
	InfoHash     string
	Name         string
}

func (s *Store) replaceMultiSeasonPattern(name string, targetSeason int) string {
	result := name

	// Apply each pre-compiled pattern replacement
	for _, msp := range multiSeasonReplacements {
		if msp.pattern.MatchString(result) {
			replacement := fmt.Sprintf(msp.replacement, targetSeason)
			result = msp.pattern.ReplaceAllString(result, replacement)
			s.logger.Debug().Msgf("Applied pattern replacement: %s -> %s", name, result)
			return result
		}
	}

	// If no multi-season pattern found, try to insert season info intelligently
	return s.insertSeasonIntoName(result, targetSeason)
}

func (s *Store) insertSeasonIntoName(name string, seasonNum int) string {
	// Check if season info already exists
	if seasonPattern.MatchString(name) {
		return name // Already has season info, keep as is
	}

	// Try to find a good insertion point (before quality indicators)
	if loc := qualityIndicators.FindStringIndex(name); loc != nil {
		// Insert season before quality info
		before := strings.TrimSpace(name[:loc[0]])
		after := name[loc[0]:]
		return fmt.Sprintf("%s S%02d %s", before, seasonNum, after)
	}

	// If no quality indicators found, append at the end
	return fmt.Sprintf("%s S%02d", name, seasonNum)
}

func (s *Store) detectMultiSeason(debridTorrent *types.Torrent) (bool, []SeasonInfo, error) {
	torrentName := debridTorrent.Name
	files := debridTorrent.GetFiles()

	s.logger.Debug().Msgf("Analyzing torrent for multi-season: %s", torrentName)

	// Find all seasons present in the files
	seasonsFound := s.findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || s.hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil, nil
	}

	s.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := s.groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := s.generateSeasonSpecificName(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     s.generateSeasonHash(debridTorrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons, nil
}

// generateSeasonSpecificName creates season name preserving all original metadata
func (s *Store) generateSeasonSpecificName(originalName string, seasonNum int) string {
	// Find and replace the multi-season pattern with single season
	seasonName := s.replaceMultiSeasonPattern(originalName, seasonNum)

	s.logger.Debug().Msgf("Generated season name for S%02d: %s", seasonNum, seasonName)

	return seasonName
}

func (s *Store) findAllSeasons(files []types.File) map[int]bool {
	seasons := make(map[int]bool)

	for _, file := range files {
		// Check filename first
		if season := s.extractSeason(file.Name); season > 0 {
			seasons[season] = true
			continue
		}

		// Check full path
		if season := s.extractSeason(file.Path); season > 0 {
			seasons[season] = true
		}
	}

	return seasons
}

// extractSeason pulls season number from a string
func (s *Store) extractSeason(text string) int {
	matches := seasonPattern.FindStringSubmatch(text)
	if len(matches) > 1 {
		if num, err := strconv.Atoi(matches[1]); err == nil && num > 0 && num < 100 {
			return num
		}
	}
	return 0
}

func (s *Store) hasMultiSeasonIndicators(torrentName string) bool {
	for _, pattern := range multiSeasonIndicators {
		if pattern.MatchString(torrentName) {
			return true
		}
	}
	return false
}

// groupFilesBySeason puts files into season buckets
func (s *Store) groupFilesBySeason(files []types.File, knownSeasons map[int]bool) map[int][]types.File {
	groups := make(map[int][]types.File)

	// Initialize groups
	for season := range knownSeasons {
		groups[season] = []types.File{}
	}

	for _, file := range files {
		// Try to find season from filename or path
		season := s.extractSeason(file.Name)
		if season == 0 {
			season = s.extractSeason(file.Path)
		}

		// If we found a season and it's known, add the file
		if season > 0 && knownSeasons[season] {
			groups[season] = append(groups[season], file)
		} else {
			// If no season found, try path-based inference
			inferredSeason := s.inferSeasonFromPath(file.Path, knownSeasons)
			if inferredSeason > 0 {
				groups[inferredSeason] = append(groups[inferredSeason], file)
			} else if len(knownSeasons) == 1 {
				// If only one season exists, default to it
				for season := range knownSeasons {
					groups[season] = append(groups[season], file)
				}
			}
		}
	}

	return groups
}

func (s *Store) inferSeasonFromPath(path string, knownSeasons map[int]bool) int {
	pathParts := strings.Split(path, "/")

	for _, part := range pathParts {
		if season := s.extractSeason(part); season > 0 && knownSeasons[season] {
			return season
		}
	}

	return 0
}

// Helper to get sorted season list for logging
func getSortedSeasons(seasons map[int]bool) []int {
	var result []int
	for season := range seasons {
		result = append(result, season)
	}
	return result
}

// generateSeasonHash creates a unique hash for a season based on original hash
func (s *Store) generateSeasonHash(originalHash string, seasonNumber int) string {
	source := fmt.Sprintf("%s-season-%d", originalHash, seasonNumber)
	hash := md5.Sum([]byte(source))
	return fmt.Sprintf("%x", hash)
}

func grabber(client *grab.Client, url, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	req, err := grab.NewRequest(filename, url)
	if err != nil {
		return err
	}

	// Set byte range if specified
	if byterange != nil {
		byterangeStr := fmt.Sprintf("%d-%d", byterange[0], byterange[1])
		req.HTTPRequest.Header.Set("Range", "bytes="+byterangeStr)
	}

	resp := client.Do(req)

	t := time.NewTicker(time.Second * 2)
	defer t.Stop()

	var lastReported int64
Loop:
	for {
		select {
		case <-t.C:
			current := resp.BytesComplete()
			speed := int64(resp.BytesPerSecond())
			if current != lastReported {
				if progressCallback != nil {
					progressCallback(current-lastReported, speed)
				}
				lastReported = current
			}
		case <-resp.Done:
			break Loop
		}
	}

	// Report final bytes
	if progressCallback != nil {
		progressCallback(resp.BytesComplete()-lastReported, 0)
	}

	return resp.Err()
}

func (s *Store) processDownload(torrent *Torrent, debridTorrent *types.Torrent) (string, error) {
	s.logger.Info().Msgf("Downloading %d files...", len(debridTorrent.Files))
	torrentPath := filepath.Join(torrent.SavePath, utils.RemoveExtension(debridTorrent.OriginalFilename))
	torrentPath = utils.RemoveInvalidChars(torrentPath)
	err := os.MkdirAll(torrentPath, os.ModePerm)
	if err != nil {
		// add the previous error to the error and return
		return "", fmt.Errorf("failed to create directory: %s: %v", torrentPath, err)
	}
	s.downloadFiles(torrent, debridTorrent, torrentPath)
	return torrentPath, nil
}

func (s *Store) downloadFiles(torrent *Torrent, debridTorrent *types.Torrent, parent string) {
	var wg sync.WaitGroup

	totalSize := int64(0)
	for _, file := range debridTorrent.GetFiles() {
		totalSize += file.Size
	}
	debridTorrent.Lock()
	debridTorrent.SizeDownloaded = 0 // Reset downloaded bytes
	debridTorrent.Progress = 0       // Reset progress
	debridTorrent.Unlock()
	progressCallback := func(downloaded int64, speed int64) {
		debridTorrent.Lock()
		defer debridTorrent.Unlock()
		torrent.Lock()
		defer torrent.Unlock()

		// Update total downloaded bytes
		debridTorrent.SizeDownloaded += downloaded
		debridTorrent.Speed = speed

		// Calculate overall progress
		if totalSize > 0 {
			debridTorrent.Progress = float64(debridTorrent.SizeDownloaded) / float64(totalSize) * 100
		}
		s.partialTorrentUpdate(torrent, debridTorrent)
	}
	client := &grab.Client{
		UserAgent: "Decypharr[QBitTorrent]",
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}
	errChan := make(chan error, len(debridTorrent.Files))
	for _, file := range debridTorrent.GetFiles() {
		if file.DownloadLink.Empty() {
			s.logger.Info().Msgf("No download link found for %s", file.Name)
			continue
		}
		wg.Add(1)
		s.downloadSemaphore <- struct{}{}
		go func(file types.File) {
			defer wg.Done()
			defer func() { <-s.downloadSemaphore }()
			filename := file.Name

			err := grabber(
				client,
				file.DownloadLink.DownloadLink,
				filepath.Join(parent, filename),
				file.ByteRange,
				progressCallback,
			)

			if err != nil {
				s.logger.Error().Msgf("Failed to download %s: %v", filename, err)
				errChan <- err
			} else {
				s.logger.Info().Msgf("Downloaded %s", filename)
			}
		}(file)
	}
	wg.Wait()

	close(errChan)
	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		s.logger.Error().Msgf("Errors occurred during download: %v", errors)
		return
	}
	s.logger.Info().Msgf("Downloaded all files for %s", debridTorrent.Name)
}

func (s *Store) processStrm(client common.Client, debridTorrent *types.Torrent, torrentStrmPath string) (string, map[string]string, error) {
	files := debridTorrent.GetFiles()
	if len(files) == 0 {
		return "", nil, fmt.Errorf("no valid files found")
	}

	s.logger.Info().Msgf("Creating .strm files for %d files in torrent %s (ID: %s)", len(files), debridTorrent.Name, debridTorrent.Id)

	// Create strm directory
	err := os.MkdirAll(torrentStrmPath, os.ModePerm)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create directory: %s: %v", torrentStrmPath, err)
	}

	// Map to store original filename -> streaming URL mapping for cache
	strmUrls := make(map[string]string)

	// For each file, create a .strm file containing the HTTP URL
	for _, file := range files {
		s.logger.Debug().Msgf("Processing file: %s (file_id=%s, torrent_id=%s)", file.Name, file.Id, file.TorrentId)

		// Get streaming URL from the debrid client (each provider constructs it appropriately)
		downloadURL := client.GetStreamingURL(debridTorrent, &file)
		if downloadURL == "" {
			s.logger.Warn().Msgf("No streaming URL available for file: %s", file.Name)
			continue
		}

		s.logger.Debug().Msgf("Generated streaming URL: %s", downloadURL)

		// Store the URL in cache mapping (original filename -> streaming URL)
		strmUrls[file.Name] = downloadURL

		// Create strm file path by replacing the original extension with .strm
		strmFileName := utils.RemoveExtension(file.Name) + ".strm"
		strmFilePath := filepath.Join(torrentStrmPath, strmFileName)

		// Write the HTTP URL to the .strm file
		if err := os.WriteFile(strmFilePath, []byte(downloadURL), 0644); err != nil {
			s.logger.Error().Msgf("Failed to create .strm file for %s: %v", file.Name, err)
			continue
		}

		s.logger.Info().Msgf("Created .strm file: %s with file_id=%s", strmFileName, file.Id)
	}

	return torrentStrmPath, strmUrls, nil
}

func (s *Store) processMultiSeasonSymlinks(torrent *Torrent, debridTorrent *types.Torrent, seasons []SeasonInfo, importReq *ImportRequest) error {
	// Get debrid client
	client := s.debrid.Debrid(debridTorrent.Debrid).Client()

	// Get download links for STRM mode
	if err := client.GetFileDownloadLinks(debridTorrent); err != nil {
		return fmt.Errorf("failed to get download links for strm mode: %w", err)
	}

	for _, seasonInfo := range seasons {
		// Create a season-specific debrid torrent
		seasonDebridTorrent := debridTorrent.Copy()

		// Update the season torrent with season-specific data
		seasonDebridTorrent.InfoHash = seasonInfo.InfoHash
		seasonDebridTorrent.Name = seasonInfo.Name

		seasonTorrent := torrent.Copy()
		seasonTorrent.ID = seasonInfo.InfoHash
		seasonTorrent.Name = seasonInfo.Name
		seasonTorrent.Hash = seasonInfo.InfoHash

		torrentFiles := make([]*File, 0)
		size := int64(0)

		// Filter files to only include this season's files
		seasonFiles := make(map[string]types.File)
		for index, file := range seasonInfo.Files {
			seasonFiles[file.Name] = file
			torrentFiles = append(torrentFiles, &File{
				Index: index,
				Name:  file.Path,
				Size:  file.Size,
			})
			size += file.Size
		}
		seasonDebridTorrent.Files = seasonFiles
		seasonTorrent.Files = torrentFiles
		seasonTorrent.Size = size

		// Create season folder path using the extracted season name
		seasonFolderName := seasonInfo.Name

		s.logger.Info().Msgf("Processing season %s with %d files", seasonTorrent.Name, len(seasonInfo.Files))

		// STRM mode for multi-season
		torrentSymlinkPath := filepath.Join(seasonTorrent.SavePath, seasonFolderName)
		torrentSymlinkPath, _, err := s.processStrm(client, seasonDebridTorrent, torrentSymlinkPath)

		if err != nil {
			return err
		}

		if torrentSymlinkPath == "" {
			return fmt.Errorf("no path found for season %d", seasonInfo.SeasonNumber)
		}

		// Update season torrent with final path
		seasonTorrent.TorrentPath = torrentSymlinkPath
		seasonTorrent.ContentPath = torrentSymlinkPath
		seasonTorrent.State = "pausedUP"
		// Add the season torrent to storage
		s.torrents.AddOrUpdate(seasonTorrent)

		s.logger.Info().Str("path", torrentSymlinkPath).Msgf("Successfully created season %d torrent: %s", seasonInfo.SeasonNumber, seasonTorrent.ID)
	}
	s.torrents.Delete(torrent.Hash, "", false)
	s.logger.Info().Msgf("Multi-season processing completed for %s", debridTorrent.Name)
	return nil
}

// processMultiSeasonDownloads handles multi-season torrent downloading
func (s *Store) processMultiSeasonDownloads(torrent *Torrent, debridTorrent *types.Torrent, seasons []SeasonInfo, importReq *ImportRequest) error {
	s.logger.Info().Msgf("Creating separate download records for %d seasons", len(seasons))
	for _, seasonInfo := range seasons {
		// Create a season-specific debrid torrent
		seasonDebridTorrent := debridTorrent.Copy()

		// Update the season torrent with season-specific data
		seasonDebridTorrent.InfoHash = seasonInfo.InfoHash
		seasonDebridTorrent.Name = seasonInfo.Name

		// Filter files to only include this season's files
		seasonFiles := make(map[string]types.File)
		for _, file := range seasonInfo.Files {
			seasonFiles[file.Name] = file
		}
		seasonDebridTorrent.Files = seasonFiles

		// Create a season-specific torrent record
		seasonTorrent := torrent.Copy()
		seasonTorrent.ID = uuid.New().String()
		seasonTorrent.Name = seasonInfo.Name
		seasonTorrent.Hash = seasonInfo.InfoHash
		seasonTorrent.SavePath = torrent.SavePath

		s.logger.Info().Msgf("Downloading season %d with %d files", seasonInfo.SeasonNumber, len(seasonInfo.Files))

		// Generate download links for season files
		client := s.debrid.Debrid(debridTorrent.Debrid).Client()
		if err := client.GetFileDownloadLinks(seasonDebridTorrent); err != nil {
			s.logger.Error().Msgf("Failed to get download links for season %d: %v", seasonInfo.SeasonNumber, err)
			return fmt.Errorf("failed to get download links for season %d: %v", seasonInfo.SeasonNumber, err)
		}

		// Download files for this season
		seasonDownloadPath, err := s.processDownload(seasonTorrent, seasonDebridTorrent)
		if err != nil {
			s.logger.Error().Msgf("Failed to download season %d: %v", seasonInfo.SeasonNumber, err)
			return fmt.Errorf("failed to download season %d: %v", seasonInfo.SeasonNumber, err)
		}

		// Update season torrent with final path
		seasonTorrent.TorrentPath = seasonDownloadPath
		seasonTorrent.ContentPath = seasonDownloadPath
		seasonTorrent.State = "pausedUP"

		// Add the season torrent to storage
		s.torrents.AddOrUpdate(seasonTorrent)
		s.logger.Info().Msgf("Successfully downloaded season %d torrent: %s", seasonInfo.SeasonNumber, seasonTorrent.ID)
	}
	s.logger.Debug().Msgf("Deleting original torrent with hash: %s, category: %s", torrent.Hash, torrent.Category)
	s.torrents.Delete(torrent.Hash, torrent.Category, false)

	s.logger.Info().Msgf("Multi-season download processing completed for %s", debridTorrent.Name)
	return nil
}
