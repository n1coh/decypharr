package wire

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func (s *Store) AddTorrent(ctx context.Context, importReq *ImportRequest) error {
	torrent := createTorrentFromMagnet(importReq)
	debridTorrent, err := debridTypes.Process(ctx, s.debrid, importReq.SelectedDebrid, importReq.Magnet, importReq.Arr, importReq.Action, importReq.DownloadUncached)
	if err != nil {
		var httpErr *utils.HTTPError
		if ok := errors.As(err, &httpErr); ok {
			switch httpErr.Code {
			case "too_many_active_downloads":
				// Handle too much active downloads error
				s.logger.Warn().Msgf("Too many active downloads for %s, adding to queue", importReq.Magnet.Name)

				if err := s.addToQueue(importReq); err != nil {
					s.logger.Error().Err(err).Msgf("Failed to add %s to queue", importReq.Magnet.Name)
					return err
				}
				torrent.State = "queued"
			default:
				// Unhandled error, return it, caller logs it
				return err
			}
		} else {
			// Unhandled error, return it, caller logs it
			return err
		}
	}
	torrent = s.partialTorrentUpdate(torrent, debridTorrent)
	s.torrents.AddOrUpdate(torrent)
	go s.processFiles(torrent, debridTorrent, importReq) // We can send async for file processing not to delay the response
	return nil
}

func (s *Store) processFiles(torrent *Torrent, debridTorrent *types.Torrent, importReq *ImportRequest) {

	if debridTorrent == nil {
		// Early return if debridTorrent is nil
		return
	}

	deb := s.debrid.Debrid(debridTorrent.Debrid)
	client := deb.Client()
	downloadingStatuses := client.GetDownloadingStatus()
	_arr := importReq.Arr
	backoff := time.NewTimer(s.refreshInterval)
	defer backoff.Stop()
	for debridTorrent.Status != "downloaded" {
		s.logger.Debug().Msgf("%s <- (%s) Download Progress: %.2f%%", debridTorrent.Debrid, debridTorrent.Name, debridTorrent.Progress)
		dbT, err := client.CheckStatus(debridTorrent)
		if err != nil {
			s.logger.Error().
				Str("torrent_id", debridTorrent.Id).
				Str("torrent_name", debridTorrent.Name).
				Err(err).
				Msg("Error checking torrent status")
			if dbT != nil && dbT.Id != "" {
				// Delete the torrent if it was not downloaded
				go func() {
					_ = client.DeleteTorrent(dbT.Id)
				}()
			}
			s.logger.Error().Msgf("Error checking status: %v", err)
			s.markTorrentAsFailed(torrent)
			go func() {
				_arr.Refresh()
			}()
			importReq.markAsFailed(err, torrent, debridTorrent)
			return
		}

		debridTorrent = dbT
		torrent = s.partialTorrentUpdate(torrent, debridTorrent)

		// Exit the loop for downloading statuses to prevent memory buildup
		if debridTorrent.Status == "downloaded" || !utils.Contains(downloadingStatuses, debridTorrent.Status) {
			break
		}

		<-backoff.C
		// Reset the backoff timer
		nextInterval := min(s.refreshInterval*2, 30*time.Second)
		backoff.Reset(nextInterval)
	}
	var torrentSymlinkPath string
	debridTorrent.Arr = _arr

	// Check if debrid supports webdav by checking cache
	timer := time.Now()

	onFailed := func(err error) {
		s.markTorrentAsFailed(torrent)
		go func() {
			if deleteErr := client.DeleteTorrent(debridTorrent.Id); deleteErr != nil {
				s.logger.Warn().Err(deleteErr).Msgf("Failed to delete torrent %s", debridTorrent.Id)
			}
		}()
		s.logger.Error().Err(err).Msgf("Error occured while processing torrent %s", debridTorrent.Name)
		importReq.markAsFailed(err, torrent, debridTorrent)
	}

	onSuccess := func(torrentSymlinkPath string) {
		torrent.TorrentPath = torrentSymlinkPath
		s.updateTorrent(torrent, debridTorrent)
		s.logger.Info().Msgf("Adding %s took %s", debridTorrent.Name, time.Since(timer))

		go importReq.markAsCompleted(torrent, debridTorrent) // Mark the import request as completed, send callback if needed
		go func() {
			if err := request.SendDiscordMessage("download_complete", "success", torrent.discordContext()); err != nil {
				s.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
		go func() {
			_arr.Refresh()
		}()
	}

	// Check for multi-season torrent support
	var isMultiSeason bool
	var seasons []SeasonInfo
	var strmFiles map[string]string
	var err error
	if !importReq.SkipMultiSeason {
		isMultiSeason, seasons, err = s.detectMultiSeason(debridTorrent)
		if err != nil {
			s.logger.Warn().Msgf("Error detecting multi-season for %s: %v", debridTorrent.Name, err)
			// Continue with normal processing if detection fails
			isMultiSeason = false
		}
	}

	switch importReq.Action {
	case "symlink":
		// STRM mode - create .strm files with streaming URLs
		s.logger.Debug().Msgf("Post-Download Action: STRM")

		cache := deb.Cache()

		if isMultiSeason {
			s.logger.Info().Msgf("Processing multi-season torrent with %d seasons", len(seasons))

			// Process multi-season with STRM
			err := s.processMultiSeasonSymlinks(torrent, debridTorrent, seasons, importReq)
			if err == nil {
				s.logger.Info().Msgf("Adding %s took %s", debridTorrent.Name, time.Since(timer))

				go importReq.markAsCompleted(torrent, debridTorrent)
				go func() {
					if err := request.SendDiscordMessage("download_complete", "success", torrent.discordContext()); err != nil {
						s.logger.Error().Msgf("Error sending discord message: %v", err)
					}
				}()
				go func() {
					_arr.Refresh()
				}()
				return
			}
		}

		// For STRM mode, we need download links
		s.logger.Info().Msgf("Starting STRM processing for torrent %s (ID: %s)", debridTorrent.Name, debridTorrent.Id)
		s.logger.Debug().Msgf("Getting download links for STRM mode...")
		if err := client.GetFileDownloadLinks(debridTorrent); err != nil {
			s.logger.Error().Msgf("Failed to get download links: %v", err)
			onFailed(err)
			return
		}
		s.logger.Info().Msgf("Successfully retrieved download links for %d files", len(debridTorrent.Files))

		torrentSymlinkPath = filepath.Join(torrent.SavePath, utils.RemoveExtension(debridTorrent.Name))
		s.logger.Info().Msgf("Creating STRM files in directory: %s", torrentSymlinkPath)
		torrentSymlinkPath, strmFiles, err = s.processStrm(client, debridTorrent, torrentSymlinkPath)
		s.logger.Info().Msgf("STRM processing completed with result: path=%s, strm_count=%d, err=%v", torrentSymlinkPath, len(strmFiles), err)

		// Add torrent to cache with STRM information
		if cache != nil {
			s.logger.Info().Msgf("Adding STRM torrent to cache for %s", debridTorrent.Debrid)
			if err := cache.Add(debridTorrent); err != nil {
				s.logger.Error().Msgf("Failed to add STRM torrent to cache: %v", err)
				// Don't fail the entire process if cache update fails
			}

			// Update cache with STRM streaming URLs
			if len(strmFiles) > 0 {
				s.logger.Info().Msgf("Updating cache with %d STRM URLs for torrent %s", len(strmFiles), debridTorrent.Id)
				if err := cache.AddStrmUrls(debridTorrent.Id, strmFiles); err != nil {
					s.logger.Error().Msgf("Failed to add STRM URLs to cache: %v", err)
					// Don't fail the entire process if STRM cache update fails
				}
			}
		}

		if err != nil {
			s.logger.Error().Msgf("Error during file processing: %v", err)
			onFailed(err)
			return
		}
		if torrentSymlinkPath == "" {
			err = fmt.Errorf("path is empty for %s", debridTorrent.Name)
			s.logger.Error().Msgf("Empty path error: %v", err)
			onFailed(err)
			return
		}
		s.logger.Debug().Msgf("File processing successful, calling onSuccess with path: %s", torrentSymlinkPath)
		onSuccess(torrentSymlinkPath)
		return
	case "download":
		// Download action, we will download the torrent to the specified folder
		// Generate download links
		s.logger.Debug().Msgf("Post-Download Action: Download")

		if isMultiSeason {
			s.logger.Info().Msgf("Processing multi-season download with %d seasons", len(seasons))
			err := s.processMultiSeasonDownloads(torrent, debridTorrent, seasons, importReq)
			if err != nil {
				onFailed(err)
				return
			}
			// Multi-season processing completed successfully
			onSuccess(torrent.SavePath)
			return
		}

		if err := client.GetFileDownloadLinks(debridTorrent); err != nil {
			onFailed(err)
			return
		}
		torrentSymlinkPath, err = s.processDownload(torrent, debridTorrent)
		if err != nil {
			onFailed(err)
			return
		}
		if torrentSymlinkPath == "" {
			err = fmt.Errorf("download path is empty for %s", debridTorrent.Name)
			onFailed(err)
			return
		}
		onSuccess(torrentSymlinkPath)
	case "none":
		s.logger.Debug().Msgf("Post-Download Action: None")
		// No action, just update the torrent and mark it as completed
		onSuccess(torrent.TorrentPath)
	default:
		// Action is none, do nothing, fallthrough
	}
}

func (s *Store) markTorrentAsFailed(t *Torrent) *Torrent {
	t.State = "error"
	s.torrents.AddOrUpdate(t)
	go func() {
		if err := request.SendDiscordMessage("download_failed", "error", t.discordContext()); err != nil {
			s.logger.Error().Msgf("Error sending discord message: %v", err)
		}
	}()
	return t
}

func (s *Store) partialTorrentUpdate(t *Torrent, debridTorrent *types.Torrent) *Torrent {
	if debridTorrent == nil {
		return t
	}

	addedOn, err := time.Parse(time.RFC3339, debridTorrent.Added)
	if err != nil {
		addedOn = time.Now()
	}
	totalSize := debridTorrent.Bytes
	progress := (cmp.Or(debridTorrent.Progress, 0.0)) / 100.0
	if math.IsNaN(progress) || math.IsInf(progress, 0) {
		progress = 0
	}
	sizeCompleted := int64(float64(totalSize) * progress)

	var speed int64
	if debridTorrent.Speed != 0 {
		speed = debridTorrent.Speed
	}
	var eta int
	if speed != 0 {
		eta = int((totalSize - sizeCompleted) / speed)
	}
	files := make([]*File, 0, len(debridTorrent.Files))
	for index, file := range debridTorrent.GetFiles() {
		files = append(files, &File{
			Index: index,
			Name:  file.Path,
			Size:  file.Size,
		})
	}
	t.DebridID = debridTorrent.Id
	t.Name = debridTorrent.Name
	t.AddedOn = addedOn.Unix()
	t.Files = files
	t.Debrid = debridTorrent.Debrid
	t.Size = totalSize
	t.TotalSize = totalSize
	t.Completed = sizeCompleted
	t.NumSeeds = debridTorrent.Seeders
	t.Downloaded = sizeCompleted
	t.DownloadedSession = sizeCompleted
	t.Uploaded = sizeCompleted
	t.UploadedSession = sizeCompleted
	t.AmountLeft = totalSize - sizeCompleted
	t.Progress = progress
	t.Eta = eta
	t.Dlspeed = speed
	t.Upspeed = speed
	return t
}

func (s *Store) updateTorrent(t *Torrent, debridTorrent *types.Torrent) *Torrent {
	if debridTorrent == nil {
		return t
	}

	if debridClient := s.debrid.Clients()[debridTorrent.Debrid]; debridClient != nil {
		if debridTorrent.Status != "downloaded" {
			_ = debridClient.UpdateTorrent(debridTorrent)
		}
	}
	t = s.partialTorrentUpdate(t, debridTorrent)
	t.ContentPath = t.TorrentPath

	if t.IsReady() {
		t.State = "pausedUP"
		s.torrents.Update(t)
		return t
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if t.IsReady() {
				t.State = "pausedUP"
				s.torrents.Update(t)
				return t
			}
			updatedT := s.updateTorrent(t, debridTorrent)
			t = updatedT

		case <-time.After(10 * time.Minute): // Add a timeout
			return t
		}
	}
}
