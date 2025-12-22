package repair

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"golang.org/x/sync/errgroup"
)

type Repair struct {
	Jobs        map[string]*Job
	arrs        *arr.Storage
	deb         *debrid.Storage
	interval    string
	ZurgURL     string
	IsZurg      bool
	useWebdav   bool
	autoProcess bool
	logger      zerolog.Logger
	filename    string
	workers     int
	scheduler   gocron.Scheduler

	debridPathCache sync.Map // debridPath:debridName cache.Emptied after each run
	torrentsMap     sync.Map //debridName: map[string]*store.CacheTorrent. Emptied after each run
	ctx             context.Context
}

type JobStatus string

const (
	JobStarted    JobStatus = "started"
	JobPending    JobStatus = "pending"
	JobFailed     JobStatus = "failed"
	JobCompleted  JobStatus = "completed"
	JobProcessing JobStatus = "processing"
	JobCancelled  JobStatus = "cancelled"
)

type Job struct {
	ID            string                       `json:"id"`
	Arrs          []string                     `json:"arrs"`
	MediaIDs      []string                     `json:"media_ids"`
	StartedAt     time.Time                    `json:"created_at"`
	BrokenItems   map[string][]arr.ContentFile `json:"broken_items"`
	Status        JobStatus                    `json:"status"`
	CompletedAt   time.Time                    `json:"finished_at"`
	FailedAt      time.Time                    `json:"failed_at"`
	AutoProcess   bool                         `json:"auto_process"`
	Recurrent     bool                         `json:"recurrent"`

	Error string `json:"error"`

	cancelFunc context.CancelFunc
	ctx        context.Context
}

func New(arrs *arr.Storage, engine *debrid.Storage) *Repair {
	cfg := config.Get()
	workers := runtime.NumCPU() * 20
	if cfg.Repair.Workers > 0 {
		workers = cfg.Repair.Workers
	}
	r := &Repair{
		arrs:        arrs,
		logger:      logger.New("repair"),
		interval:    cfg.Repair.Interval,
		ZurgURL:     cfg.Repair.ZurgURL,
		useWebdav:   cfg.Repair.UseWebDav,
		autoProcess: cfg.Repair.AutoProcess,
		filename:    filepath.Join(cfg.Path, "repair.json"),
		deb:         engine,
		workers:     workers,
		ctx:         context.Background(),
	}
	if r.ZurgURL != "" {
		r.IsZurg = true
	}
	// Load jobs from file
	r.loadFromFile()

	return r
}

func (r *Repair) Reset() {
	// Stop scheduler
	if r.scheduler != nil {
		if err := r.scheduler.Shutdown(); err != nil {
			r.logger.Error().Err(err).Msg("Error shutting down scheduler")
		}
	}
	// Reset jobs
	r.Jobs = make(map[string]*Job)

}

func (r *Repair) Start(ctx context.Context) error {

	r.scheduler, _ = gocron.NewScheduler(gocron.WithLocation(time.Local))

	if jd, err := utils.ConvertToJobDef(r.interval); err != nil {
		r.logger.Error().Err(err).Str("interval", r.interval).Msg("Error converting interval")
	} else {
		_, err2 := r.scheduler.NewJob(jd, gocron.NewTask(func() {
			r.logger.Info().Msgf("Repair job started at %s", time.Now().Format("15:04:05"))
			if err := r.AddJob([]string{}, []string{}, r.autoProcess, true); err != nil {
				r.logger.Error().Err(err).Msg("Error running repair job")
			}
		}))
		if err2 != nil {
			r.logger.Error().Err(err2).Msg("Error creating repair job")
		} else {
			r.scheduler.Start()
			r.logger.Info().Msgf("Repair job scheduled every %s", r.interval)
		}
	}

	<-ctx.Done()

	r.logger.Info().Msg("Stopping repair scheduler")
	r.Reset()

	return nil
}

func (j *Job) discordContext() string {
	format := `
		**ID**: %s
		**Arrs**: %s
		**Media IDs**: %s
		**Status**: %s
		**Started At**: %s
		**Completed At**: %s 
`

	dateFmt := "2006-01-02 15:04:05"

	return fmt.Sprintf(format, j.ID, strings.Join(j.Arrs, ","), strings.Join(j.MediaIDs, ", "), j.Status, j.StartedAt.Format(dateFmt), j.CompletedAt.Format(dateFmt))
}

func (r *Repair) getArrs(arrNames []string) []string {
	r.logger.Debug().Strs("arrNames", arrNames).Msg("getArrs called")
	arrs := make([]string, 0)
	if len(arrNames) == 0 {
		// No specific arrs, get all
		// Also check if any arrs are set to skip repair
		_arrs := r.arrs.GetAll()
		r.logger.Debug().Int("totalArrs", len(_arrs)).Msg("Getting all arrs")
		for _, a := range _arrs {
			if a.SkipRepair {
				r.logger.Debug().Str("arr", a.Name).Msg("Skipping arr (SkipRepair=true)")
				continue
			}
			arrs = append(arrs, a.Name)
		}
	} else {
		for _, name := range arrNames {
			a := r.arrs.Get(name)
			if a == nil {
				r.logger.Warn().Str("arrName", name).Msg("Arr not found")
				continue
			}
			r.logger.Debug().
				Str("arrName", name).
				Str("host", a.Host).
				Bool("hasToken", a.Token != "").
				Int("tokenLen", len(a.Token)).
				Msg("Checking arr configuration")
			if a.Host == "" || a.Token == "" {
				r.logger.Warn().
					Str("arrName", name).
					Str("host", a.Host).
					Bool("hasToken", a.Token != "").
					Msg("Arr not configured (missing host or token)")
				continue
			}
			arrs = append(arrs, a.Name)
		}
	}
	r.logger.Debug().Strs("selectedArrs", arrs).Int("count", len(arrs)).Msg("Arrs selected for repair")
	return arrs
}

func jobKey(arrNames []string, mediaIDs []string) string {
	return fmt.Sprintf("%s-%s", strings.Join(arrNames, ","), strings.Join(mediaIDs, ","))
}

func (r *Repair) reset(j *Job) {
	// Update job for rerun
	j.Status = JobStarted
	j.StartedAt = time.Now()
	j.CompletedAt = time.Time{}
	j.FailedAt = time.Time{}
	j.BrokenItems = nil
	j.Error = ""
	if j.Recurrent || j.Arrs == nil {
		j.Arrs = r.getArrs([]string{}) // Get new arrs
	}
}

func (r *Repair) newJob(arrsNames []string, mediaIDs []string) *Job {
	arrs := r.getArrs(arrsNames)
	return &Job{
		ID:        uuid.New().String(),
		Arrs:      arrs,
		MediaIDs:  mediaIDs,
		StartedAt: time.Now(),
		Status:    JobStarted,
	}
}

// initRun initializes the repair run, setting up necessary configurations, checks and caches
func (r *Repair) initRun(ctx context.Context) {
	// Load debrid torrent caches for STRM validation
	// This works with or without WebDAV since cache is always initialized for STRM mode
	caches := r.deb.Caches()
	if len(caches) == 0 {
		r.logger.Warn().Msg("No caches available for repair")
		return
	}
	for name, cache := range caches {
		r.torrentsMap.Store(name, cache.GetTorrentsName())
		r.logger.Debug().Msgf("Loaded %d torrents from %s cache", len(cache.GetTorrentsName()), name)
	}
}

// // onComplete is called when the repair job is completed
func (r *Repair) onComplete() {
	// Set the cache maps to nil
	r.torrentsMap = sync.Map{} // Clear the torrent map
	r.debridPathCache = sync.Map{}
}

func (r *Repair) preRunChecks() error {
	// With STRM files, we don't need to check mounts
	// STRM files contain HTTP URLs - no mount required
	r.logger.Debug().Msg("Pre-run checks: STRM mode - no mount checks needed")
	return nil
}

func (r *Repair) AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error {
	r.logger.Debug().
		Strs("arrsNames", arrsNames).
		Strs("mediaIDs", mediaIDs).
		Bool("autoProcess", autoProcess).
		Bool("recurrent", recurrent).
		Msg("AddJob called")

	key := jobKey(arrsNames, mediaIDs)
	job, ok := r.Jobs[key]
	if job != nil && job.Status == JobStarted {
		r.logger.Warn().Str("jobKey", key).Msg("Job already running")
		return fmt.Errorf("job already running")
	}
	if !ok {
		job = r.newJob(arrsNames, mediaIDs)
		r.logger.Debug().
			Str("jobID", job.ID).
			Strs("arrs", job.Arrs).
			Int("numArrs", len(job.Arrs)).
			Msg("New repair job created")
	} else {
		r.logger.Debug().Str("jobID", job.ID).Msg("Reusing existing job")
	}
	job.AutoProcess = autoProcess
	job.Recurrent = recurrent
	r.reset(job)

	job.ctx, job.cancelFunc = context.WithCancel(r.ctx)
	r.Jobs[key] = job
	go r.saveToFile()
	go func() {
		if err := r.repair(job); err != nil {
			r.logger.Error().Err(err).Msg("Error running repair")
			if !errors.Is(job.ctx.Err(), context.Canceled) {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = JobFailed
				job.CompletedAt = time.Now()
			} else {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = JobFailed
				job.CompletedAt = time.Now()
			}
		}
		r.onComplete() // Clear caches and maps after job completion
	}()
	return nil
}

func (r *Repair) StopJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}

	// Check if job can be stopped
	if job.Status != JobStarted && job.Status != JobProcessing {
		return fmt.Errorf("job %s cannot be stopped (status: %s)", id, job.Status)
	}

	// Cancel the job
	if job.cancelFunc != nil {
		job.cancelFunc()
		r.logger.Info().Msgf("Job %s cancellation requested", id)
		go func() {
			if job.Status == JobStarted || job.Status == JobProcessing {
				job.Status = JobCancelled
				job.BrokenItems = nil
				job.ctx = nil // Clear context to prevent further processing
				job.CompletedAt = time.Now()
				job.Error = "Job was cancelled by user"
				r.saveToFile()
			}
		}()

		return nil
	}

	return fmt.Errorf("job %s cannot be cancelled", id)
}

func (r *Repair) repair(job *Job) error {
	r.logger.Debug().
		Str("jobID", job.ID).
		Strs("arrs", job.Arrs).
		Int("numArrs", len(job.Arrs)).
		Strs("mediaIDs", job.MediaIDs).
		Bool("autoProcess", job.AutoProcess).
		Msg("Starting repair job")

	defer r.saveToFile()

	if len(job.Arrs) == 0 {
		r.logger.Warn().Msg("No arrs configured for repair, job will complete immediately")
		job.Status = JobCompleted
		job.CompletedAt = time.Now()
		return nil
	}

	if err := r.preRunChecks(); err != nil {
		r.logger.Error().Err(err).Msg("Pre-run checks failed")
		return err
	}

	r.logger.Debug().Msg("Pre-run checks passed")

	// Initialize the run
	r.initRun(job.ctx)

	// Use a mutex to protect concurrent access to brokenItems
	var mu sync.Mutex
	brokenItems := map[string][]arr.ContentFile{}
	g, ctx := errgroup.WithContext(job.ctx)

	for _, a := range job.Arrs {
		a := a // Capture range variable
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var items []arr.ContentFile
			var err error

			if len(job.MediaIDs) == 0 {
				items, err = r.repairArr(job, a, "")
				if err != nil {
					r.logger.Error().Err(err).Msgf("Error repairing %s", a)
					return err
				}
			} else {
				for _, id := range job.MediaIDs {
					someItems, err := r.repairArr(job, a, id)
					if err != nil {
						r.logger.Error().Err(err).Msgf("Error repairing %s with ID %s", a, id)
						return err
					}
					items = append(items, someItems...)
				}
			}

			// Safely append the found items to the shared slice
			if len(items) > 0 {
				mu.Lock()
				brokenItems[a] = items
				mu.Unlock()
			}

			return nil
		})
	}

	// Wait for all goroutines to complete and check for errors
	r.logger.Debug().Msg("Waiting for all arr repairs to complete")
	if err := g.Wait(); err != nil {
		r.logger.Error().Err(err).Msg("Error during repair execution")
		// Check if job was canceled
		if errors.Is(ctx.Err(), context.Canceled) {
			r.logger.Info().Msg("Job was cancelled")
			job.Status = JobCancelled
			job.CompletedAt = time.Now()
			job.Error = "Job was cancelled"
			return fmt.Errorf("job cancelled")
		}

		job.FailedAt = time.Now()
		job.Error = err.Error()
		job.Status = JobFailed
		job.CompletedAt = time.Now()
		go func() {
			if err := request.SendDiscordMessage("repair_failed", "error", job.discordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
		return err
	}

	r.logger.Debug().
		Int("totalBrokenItems", len(brokenItems)).
		Msg("Repair scan completed")

	if len(brokenItems) == 0 {
		r.logger.Debug().Msg("No broken items found, marking job as completed")
		job.CompletedAt = time.Now()
		job.Status = JobCompleted

		go func() {
			if err := request.SendDiscordMessage("repair_complete", "success", job.discordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()

		return nil
	}

	job.BrokenItems = brokenItems
	if job.AutoProcess {
		// Job is already processed
		job.CompletedAt = time.Now() // Mark as completed
		job.Status = JobCompleted
		go func() {
			if err := request.SendDiscordMessage("repair_complete", "success", job.discordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
	} else {
		job.Status = JobPending
		go func() {
			if err := request.SendDiscordMessage("repair_pending", "pending", job.discordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
	}
	return nil
}

func (r *Repair) repairArr(job *Job, _arr string, tmdbId string) ([]arr.ContentFile, error) {
	r.logger.Debug().
		Str("arr", _arr).
		Str("tmdbId", tmdbId).
		Msg("Starting repairArr")

	brokenItems := make([]arr.ContentFile, 0)
	a := r.arrs.Get(_arr)

	if a == nil {
		r.logger.Error().Str("arr", _arr).Msg("Arr not found in storage")
		return brokenItems, fmt.Errorf("arr %s not found", _arr)
	}

	if a.Host == "" || a.Token == "" {
		r.logger.Error().
			Str("arr", a.Name).
			Str("host", a.Host).
			Bool("hasToken", a.Token != "").
			Msg("Arr not configured properly")
		return brokenItems, fmt.Errorf("arr %s not configured (missing token or host)", a.Name)
	}

	r.logger.Debug().Msgf("Starting repair for %s", a.Name)
	media, err := a.GetMedia(tmdbId)
	if err != nil {
		r.logger.Debug().Msgf("Failed to get %s media: %v", a.Name, err)
		return brokenItems, err
	}
	r.logger.Debug().Msgf("Found %d %s media", len(media), a.Name)

	if len(media) == 0 {
		r.logger.Debug().Msgf("No %s media found", a.Name)
		return brokenItems, nil
	}

	// Mutex for brokenItems
	var mu sync.Mutex
	var wg sync.WaitGroup
	workerChan := make(chan arr.Content, min(len(media), r.workers))

	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range workerChan {
				select {
				case <-job.ctx.Done():
					return
				default:
				}
				items := r.getBrokenFiles(job, m)
				if items != nil {
					r.logger.Debug().Msgf("Found %d broken files for %s", len(items), m.Title)
					if job.AutoProcess {
						r.logger.Info().Msgf("Auto processing %d broken items for %s", len(items), m.Title)

						// Delete broken items
						if err := a.DeleteFiles(items); err != nil {
							r.logger.Debug().Msgf("Failed to delete broken items for %s: %v", m.Title, err)
						}

						// Search for missing items
						if err := a.SearchMissing(items); err != nil {
							r.logger.Debug().Msgf("Failed to search missing items for %s: %v", m.Title, err)
						}
					}

					mu.Lock()
					brokenItems = append(brokenItems, items...)
					mu.Unlock()
				}
			}
		}()
	}

	go func() {
		defer close(workerChan)
		for _, m := range media {
			select {
			case <-job.ctx.Done():
				return
			case workerChan <- m:
			}
		}
	}()

	wg.Wait()
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for %s", a.Name)
		return brokenItems, nil
	}

	r.logger.Info().Msgf("Repair completed for %s. %d broken items found", a.Name, len(brokenItems))
	return brokenItems, nil
}

func (r *Repair) getBrokenFiles(job *Job, media arr.Content) []arr.ContentFile {
	// With STRM files, all modes work the same - just check file existence
	return r.getFileBrokenFiles(job, media)
}

func (r *Repair) getFileBrokenFiles(job *Job, media arr.Content) []arr.ContentFile {
	// This checks symlink target, try to get read a tiny bit of the file

	r.logger.Debug().
		Str("mediaTitle", media.Title).
		Int("mediaID", media.Id).
		Msg("Starting file-based repair check")

	brokenFiles := make([]arr.ContentFile, 0)

	uniqueParents := collectFiles(media)
	r.logger.Debug().Int("uniqueParents", len(uniqueParents)).Msg("Collected file parents")

	for parent, files := range uniqueParents {
		r.logger.Debug().
			Str("parent", parent).
			Int("numFiles", len(files)).
			Msg("Checking files in parent directory")

		// Check if STRM files are valid by regenerating URLs from cache
		for _, file := range files {
			r.logger.Debug().
				Str("filePath", file.Path).
				Str("fileName", file.Name).
				Msg("Checking STRM file validity")

			if err := r.isStrmFileValid(file.Path); err != nil {
				r.logger.Debug().
					Str("filePath", file.Path).
					Str("parent", parent).
					Err(err).
					Msg("Broken file found")
				brokenFiles = append(brokenFiles, file)
			} else {
				r.logger.Debug().
					Str("filePath", file.Path).
					Msg("STRM file is valid")
			}
		}
	}
	if len(brokenFiles) == 0 {
		r.logger.Debug().Msgf("No broken files found for %s", media.Title)
		return nil
	}
	r.logger.Debug().Msgf("%d broken files found for %s", len(brokenFiles), media.Title)
	return brokenFiles
}

// isStrmFileValid validates a STRM file by reading the URL and verifying it against cache
// filePath comes from Radarr/Sonarr API and points to the actual .strm file on disk
func (r *Repair) isStrmFileValid(filePath string) error {
	// Only validate .strm files
	if !strings.HasSuffix(strings.ToLower(filePath), ".strm") {
		return nil // Non-STRM files are considered valid
	}

	// Read the STRM file to get the streaming URL
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read STRM file: %w", err)
	}

	streamURL := strings.TrimSpace(string(content))
	if streamURL == "" {
		return fmt.Errorf("STRM file is empty")
	}

	// Extract torrent_id and file_id from the URL
	// Format: https://api.torbox.app/v1/api/torrents/requestdl?token=XXX&torrent_id=123&file_id=0
	torrentID, fileID, err := r.extractIDsFromURL(streamURL)
	if err != nil {
		return fmt.Errorf("failed to extract IDs from URL: %w", err)
	}

	// Find the torrent in cache by ID
	caches := r.deb.Caches()
	var foundCache *store.Cache
	var foundCachedTorrent *store.CachedTorrent
	var foundDebridName string

	for debridName, cache := range caches {
		if cachedTorrent := cache.GetTorrent(torrentID); cachedTorrent != nil {
			foundCache = cache
			foundCachedTorrent = cachedTorrent
			foundDebridName = debridName
			break
		}
	}

	if foundCachedTorrent == nil {
		return fmt.Errorf("torrent %s not found in cache", torrentID)
	}

	// Find the file in the torrent by file_id
	var foundFile *types.File
	for _, file := range foundCachedTorrent.Files {
		if file.Id == fileID {
			foundFile = &file
			break
		}
	}

	if foundFile == nil {
		return fmt.Errorf("file %s not found in torrent %s", fileID, torrentID)
	}

	// Update strm_urls in cache if not already present
	if foundCachedTorrent.StrmUrls == nil || foundCachedTorrent.StrmUrls[foundFile.Name] != streamURL {
		r.logger.Debug().Msgf("Updating strm_urls in cache for torrent %s, file %s", torrentID, foundFile.Name)
		strmUrls := map[string]string{foundFile.Name: streamURL}
		if err := foundCache.AddStrmUrls(torrentID, strmUrls); err != nil {
			r.logger.Warn().Msgf("Failed to update strm_urls in cache: %v", err)
			// Don't fail validation just because cache update failed
		}
	}

	// Validate the URL with a HEAD request
	if err := validateStrmURL(streamURL); err != nil {
		return fmt.Errorf("streaming URL validation failed: %w", err)
	}

	r.logger.Debug().Msgf("STRM file validated successfully: torrent=%s, file=%s, debrid=%s", torrentID, fileID, foundDebridName)
	return nil
}

// extractIDsFromURL extracts torrent_id and file_id from a streaming URL
func (r *Repair) extractIDsFromURL(urlStr string) (torrentID string, fileID string, err error) {
	// Parse the URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", fmt.Errorf("invalid URL: %w", err)
	}

	// Extract query parameters
	query := u.Query()
	torrentID = query.Get("torrent_id")
	fileID = query.Get("file_id")

	if torrentID == "" {
		return "", "", fmt.Errorf("torrent_id not found in URL")
	}
	if fileID == "" {
		return "", "", fmt.Errorf("file_id not found in URL")
	}

	return torrentID, fileID, nil
}

func (r *Repair) GetJob(id string) *Job {
	for _, job := range r.Jobs {
		if job.ID == id {
			return job
		}
	}
	return nil
}

func (r *Repair) GetJobs() []*Job {
	jobs := make([]*Job, 0)
	for _, job := range r.Jobs {
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})

	return jobs
}

func (r *Repair) ProcessJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}
	if job.Status != JobPending {
		return fmt.Errorf("job %s not pending", id)
	}
	if job.StartedAt.IsZero() {
		return fmt.Errorf("job %s not started", id)
	}
	if !job.CompletedAt.IsZero() {
		return fmt.Errorf("job %s already completed", id)
	}
	if !job.FailedAt.IsZero() {
		return fmt.Errorf("job %s already failed", id)
	}

	brokenItems := job.BrokenItems
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for job %s", id)
		job.CompletedAt = time.Now()
		job.Status = JobCompleted
		return nil
	}

	if job.ctx == nil || job.ctx.Err() != nil {
		job.ctx, job.cancelFunc = context.WithCancel(r.ctx)
	}

	g, ctx := errgroup.WithContext(job.ctx)
	g.SetLimit(r.workers)

	for arrName, items := range brokenItems {
		items := items
		arrName := arrName
		g.Go(func() error {

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			a := r.arrs.Get(arrName)
			if a == nil {
				r.logger.Error().Msgf("Arr %s not found", arrName)
				return nil
			}

			if err := a.DeleteFiles(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to delete broken items for %s", arrName)
				return nil
			}
			// Search for missing items
			if err := a.SearchMissing(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to search missing items for %s", arrName)
				return nil
			}
			return nil
		})
	}

	// Update job status to in-progress
	job.Status = JobProcessing
	r.saveToFile()

	// Launch a goroutine to wait for completion and update the job
	go func() {
		if err := g.Wait(); err != nil {
			job.FailedAt = time.Now()
			job.Error = err.Error()
			job.CompletedAt = time.Now()
			job.Status = JobFailed
			r.logger.Error().Err(err).Msgf("Job %s failed", id)
		} else {
			job.CompletedAt = time.Now()
			job.Status = JobCompleted
			r.logger.Info().Msgf("Job %s completed successfully", id)
		}

		r.saveToFile()
	}()

	return nil
}

func (r *Repair) saveToFile() {
	// Save jobs to file
	data, err := json.Marshal(r.Jobs)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to marshal jobs")
	}
	_ = os.WriteFile(r.filename, data, 0644)
}

func (r *Repair) loadFromFile() {
	data, err := os.ReadFile(r.filename)
	if err != nil && os.IsNotExist(err) {
		r.Jobs = make(map[string]*Job)
		return
	}
	_jobs := make(map[string]*Job)
	err = json.Unmarshal(data, &_jobs)
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to unmarshal jobs; resetting")
		r.Jobs = make(map[string]*Job)
		return
	}
	jobs := make(map[string]*Job)
	for k, v := range _jobs {
		if v.Status != JobPending {
			// Skip jobs that are not pending processing due to reboot
			continue
		}
		jobs[k] = v
	}
	r.Jobs = jobs
}

func (r *Repair) DeleteJobs(ids []string) {
	for _, id := range ids {
		if id == "" {
			continue
		}
		for k, job := range r.Jobs {
			if job.ID == id {
				delete(r.Jobs, k)
			}
		}
	}
	go r.saveToFile()
}

// Cleanup Cleans up the repair instance
func (r *Repair) Cleanup() {
	r.Jobs = make(map[string]*Job)
	r.arrs = nil
	r.deb = nil
	r.ctx = nil
	r.logger.Info().Msg("Repair stopped")
}
