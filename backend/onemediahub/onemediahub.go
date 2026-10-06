// Package onemediahub implements the OneMediaHub Server API.
package onemediahub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/list"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/batcher"
	"github.com/rclone/rclone/lib/cache"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/kv"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/singleflight"
)

const (
	pageSize             = 100
	deleteBatchLimit     = 1000
	minSleep             = 10 * time.Millisecond
	maxSleep             = 2 * time.Second
	decayConstant        = 2
	dateFormat           = "20060102T150405Z"
	maxAuthAttempts      = 2
	maxRedirects         = 10
	deviceHeader         = "X-deviceid"
	oauthDevicePrefix    = "fol-"
	passwordDevicePrefix = "fac-"
	metadataTimeout      = 30 * time.Second
	metadataDelay        = 200 * time.Millisecond
	metadataMaxDelay     = 2 * time.Second
	maxErrorSize         = 64 * 1024
	cloudFrontBlockDelay = 30 * time.Second
	sessionCookie        = "JSESSIONID"
	metadataVersion      = 2
	uploadJournalVersion = 1
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "onemediahub",
		Description: "OneMediaHub (including O2 Cloud)",
		NewFs:       NewFs,
		Config:      configure,
		Options: []fs.Option{
			{
				Name:    "url",
				Help:    "OneMediaHub server URL, including any deployment prefix, without /sapi.",
				Default: o2URL,
			},
			{
				Name:    "auth_type",
				Help:    "Authentication method supported by this server.",
				Default: authOAuth,
				Examples: []fs.OptionExample{{
					Value: authOAuth,
					Help:  "OAuth with saved refresh tokens (O2 Cloud).",
				}, {
					Value: authPassword,
					Help:  "OneMediaHub username and password.",
				}},
			},
			{
				Name:      "user",
				Help:      "OneMediaHub username for password authentication.",
				Sensitive: true,
			},
			{
				Name:       "password",
				Help:       "OneMediaHub password for password authentication.",
				IsPassword: true,
			},
			{
				Name:      "client_id",
				Help:      "OAuth client ID supplied by your provider. No proprietary client ID is bundled.",
				Sensitive: true,
			},
			{
				Name:       "client_secret",
				Help:       "OAuth client secret supplied by your provider, if required.",
				IsPassword: true,
			},
			{
				Name:     "auth_url",
				Help:     "OAuth authorization endpoint. Empty uses defaults for O2 Spain or Germany.",
				Advanced: true,
			},
			{
				Name:     "token_url",
				Help:     "OAuth token endpoint. Empty uses defaults for O2 Spain or Germany.",
				Advanced: true,
			},
			{
				Name:     "redirect_url",
				Help:     "Registered OAuth callback URL. Empty uses defaults for O2 Spain or Germany.",
				Advanced: true,
			},
			{
				Name:     "scope",
				Help:     "Space-separated OAuth scopes. Empty uses openid for O2 Spain and no scopes elsewhere.",
				Advanced: true,
			},
			{
				Name:     "platform",
				Help:     "OneMediaHub OAuth client platform.",
				Default:  "windows",
				Advanced: true,
			},
			{
				Name:      "msisdn",
				Help:      "Phone number if required by your provider's OAuth adapter.",
				Sensitive: true,
				Advanced:  true,
			},
			{
				Name:      "device_id",
				Help:      "Persistent client device ID. Empty generates and saves one automatically.",
				Sensitive: true,
				Advanced:  true,
			},
			{
				Name:     "user_agent",
				Help:     "HTTP User-Agent accepted by the provider.",
				Default:  "OneMediaHub",
				Advanced: true,
			},
			{
				Name:     "metadata_cache",
				Help:     "Cache account metadata and refresh it using the changes API. Enables mount change notifications. Persists in rclone's cache directory on supported systems. Requires /profile and /profile/changes support.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "metadata_cache_time",
				Help:     "Interval between changes API refreshes. External changes may remain invisible until the next refresh; rclone writes update the cache immediately.",
				Default:  fs.Duration(time.Minute),
				Advanced: true,
			},
			{
				Name:     "delete_batch_size",
				Help:     "Maximum number of file deletions to batch, from 1 to 1000. Files are grouped by media type. Synchronous batches are limited by --checkers; asynchronous batches use this size directly. Smaller batches flush after 20 ms of inactivity. Set to 1 to send individual requests.",
				Default:  deleteBatchLimit,
				Advanced: true,
			},
			{
				Name:     "async_delete",
				Help:     "Queue file deletions in memory and process batches in the background. Remove reports queue admission; commands drain the queue before exit and fail on background errors. Reads, uploads and folder operations on the same filesystem wait for pending deletions. Drain the originating filesystem before accessing overlapping paths through another root or alias. Accepted deletions finish even if their caller context ends. Progress counts queued files. The queue does not survive forced termination. Mount, serve, RC and library callers must explicitly drain or shut down the backend to receive delayed failures.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "async_upload",
				Help:     "Register metadata separately and upload raw content with asynchronous server processing instead of multipart uploads.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "resume_uploads",
				Help:     "Persist unfinished asynchronous uploads in rclone's cache directory so later runs can resume them. Requires async_upload, a reopenable source with a reliable content hash, and input that can safely be rewound. The input is verified before uploading, which adds a local read. Unfinished items are hidden until their upload is validated, so this option is incompatible with --immutable, --backup-dir, --suffix, --ignore-existing, and --update. Other inputs retain recovery within the current upload attempt. Keep the cache directory to preserve recovery state.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "upload_timeout",
				Help:     "Maximum time to wait for asynchronous upload processing after sending content.",
				Default:  fs.Duration(5 * time.Minute),
				Advanced: true,
			},
			{
				Name:     "upload_url",
				Help:     "Upload server URL. Empty discovers it from the server, falling back to url.",
				Advanced: true,
			},
			{
				Name:     "api_path",
				Help:     "SAPI path relative to the server URL.",
				Default:  "/sapi",
				Advanced: true,
			},
			{
				Name:     "flat_namespace",
				Help:     "Store files under encoded full-path names in one physical folder to avoid the server's folder-count limit. Directories are virtual; empty directories use marker files. Long paths require small immutable mapping files, retained after removal and hidden from rclone. O2's apps display encoded names and mapping files. Only files written with this option are visible; existing files are not migrated. Set root_folder_id to an existing folder, or leave it empty to use unfiled media. Use metadata_cache and --fast-list for large trees.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "root_folder_id",
				Help:     "Folder ID to use as root. Empty exposes top-level folders and unfiled media.",
				Advanced: true,
			},
			{
				Name:      "token",
				Help:      "Saved OAuth token, updated automatically.",
				Sensitive: true,
				Advanced:  true,
				Hide:      fs.OptionHideConfigurator,
			},
			{
				Name:      credentialsKey,
				Help:      "Provider OAuth data returned by SAPI, updated automatically.",
				Sensitive: true,
				Advanced:  true,
				Hide:      fs.OptionHideConfigurator,
			},
			{
				Name:      sessionKey,
				Help:      "Saved server session, renewed automatically.",
				Sensitive: true,
				Advanced:  true,
				Hide:      fs.OptionHideConfigurator,
			},
			{
				Name:      authStateKey,
				Help:      "Temporary OAuth state.",
				Sensitive: true,
				Hide:      fs.OptionHideBoth,
			},
			{
				Name:      authVerifierKey,
				Help:      "Temporary OAuth PKCE verifier.",
				Sensitive: true,
				Hide:      fs.OptionHideBoth,
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Default:  encoder.Display | encoder.EncodeBackSlash | encoder.EncodeLeftPeriod | encoder.EncodeRightPeriod | encoder.EncodeInvalidUtf8,
				Advanced: true,
			},
		},
	})
}

type options struct {
	URL               string               `config:"url"`
	AuthType          string               `config:"auth_type"`
	User              string               `config:"user"`
	Password          string               `config:"password"`
	ClientID          string               `config:"client_id"`
	ClientSecret      string               `config:"client_secret"`
	AuthURL           string               `config:"auth_url"`
	TokenURL          string               `config:"token_url"`
	RedirectURL       string               `config:"redirect_url"`
	Scope             string               `config:"scope"`
	Platform          string               `config:"platform"`
	MSISDN            string               `config:"msisdn"`
	DeviceID          string               `config:"device_id"`
	UserAgent         string               `config:"user_agent"`
	UploadURL         string               `config:"upload_url"`
	AsyncUpload       bool                 `config:"async_upload"`
	ResumeUploads     bool                 `config:"resume_uploads"` // ResumeUploads enables persistent upload recovery.
	UploadTimeout     fs.Duration          `config:"upload_timeout"`
	MetadataCache     bool                 `config:"metadata_cache"`      // MetadataCache enables account metadata caching.
	MetadataCacheTime fs.Duration          `config:"metadata_cache_time"` // MetadataCacheTime is the interval between changes API refreshes.
	DeleteBatchSize   int                  `config:"delete_batch_size"`
	AsyncDelete       bool                 `config:"async_delete"`
	APIPath           string               `config:"api_path"`
	RootFolderID      string               `config:"root_folder_id"`
	FlatNamespace     bool                 `config:"flat_namespace"` // FlatNamespace stores a virtual tree in encoded media names.
	Enc               encoder.MultiEncoder `config:"encoding"`
}

func newClient(ctx context.Context, opt *options) *http.Client {
	ctx, ci := fs.AddConfig(ctx)
	// Some providers reject rclone's default User-Agent before authentication.
	ci.UserAgent = opt.UserAgent
	return fshttp.NewClient(ctx)
}

func parseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("expected an HTTP(S) URL without credentials, query or fragment")
	}
	return u, nil
}

func readOptions(m configmap.Mapper) (*options, error) {
	opt := new(options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}

	if _, err := parseURL(opt.URL); err != nil {
		return nil, fmt.Errorf("invalid server URL: %w", err)
	}

	if opt.AuthType != authOAuth && opt.AuthType != authPassword {
		return nil, errors.New("auth_type must be oauth or password")
	}
	if opt.AsyncUpload && opt.UploadTimeout <= 0 {
		return nil, errors.New("upload_timeout must be positive")
	}
	if opt.ResumeUploads && !opt.AsyncUpload {
		return nil, errors.New("resume_uploads requires async_upload")
	}
	if opt.MetadataCache && opt.MetadataCacheTime <= 0 {
		return nil, errors.New("metadata_cache_time must be positive")
	}
	if opt.DeleteBatchSize < 1 || opt.DeleteBatchSize > deleteBatchLimit {
		return nil, fmt.Errorf("delete_batch_size must be between 1 and %d", deleteBatchLimit)
	}

	if strings.ContainsAny(opt.APIPath, "?#") || strings.Contains(opt.APIPath, "://") {
		return nil, errors.New("api_path must be a relative SAPI path")
	}

	if opt.RootFolderID != "" {
		if _, err := strconv.ParseUint(opt.RootFolderID, 10, 64); err != nil {
			return nil, errors.New("root_folder_id must be numeric")
		}
	}
	return opt, nil
}

func checkResumeUploadOptions(ctx context.Context, opt *options) error {
	ci := fs.GetConfig(ctx)
	if opt.ResumeUploads && (ci.Immutable || ci.BackupDir != "" || ci.Suffix != "" || ci.IgnoreExisting || ci.UpdateOlder) {
		return errors.New("resume_uploads is incompatible with --immutable, --backup-dir, --suffix, --ignore-existing, and --update")
	}
	return nil
}

// Fs represents a OneMediaHub remote.
type Fs struct {
	downloadURLs     *cache.Cache
	downloadRefresh  singleflight.Group
	validation       *batcher.Batcher[validationItem, string]
	validationCancel context.CancelFunc
	deletions        *batcher.Batcher[deleteItem, error]
	deleteMu         sync.Mutex
	deletePending    int
	deleteCause      error
	deleteError      error
	uploadURL        string
	name             string
	root             string
	opt              *options
	features         *fs.Features
	srv              *rest.Client
	download         *rest.Client
	auth             *auth
	dirCache         *dircache.DirCache
	pacer            *fs.Pacer
	metadata         *metadataCache
	uploadJournal    *kv.DB
	uploadJournalMu  sync.RWMutex
	flatMu           sync.Mutex
	flatDirMu        sync.Mutex
	flatRecords      map[api.ID]flatPathRecord
}

// Object describes a OneMediaHub media item.
type Object struct {
	fs            *Fs
	remote        string
	info          api.Media
	flatDirectory bool // flatDirectory identifies a marker whose remote is the full virtual path.
	flatMapping   bool // flatMapping identifies an immutable path mapping whose remote is the full virtual path.
}

// NewFs creates a OneMediaHub filesystem, returning ErrorIsFile for a file root.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt, err := readOptions(m)
	if err != nil {
		return nil, err
	}
	if err := checkResumeUploadOptions(ctx, opt); err != nil {
		return nil, err
	}

	if opt.AuthType == authPassword && (opt.User == "" || opt.Password == "") {
		return nil, errors.New("password authentication requires user and password")
	}

	// Some providers require a stable client identity during login.
	if opt.DeviceID == "" {
		id := uuid.New()
		if opt.AuthType == authPassword {
			opt.DeviceID = passwordDevicePrefix + hex.EncodeToString(id[:])
		} else {
			opt.DeviceID = oauthDevicePrefix + base64.StdEncoding.EncodeToString(id[:])
		}
		m.Set("device_id", opt.DeviceID)
	}
	base := strings.TrimRight(opt.URL, "/") + "/" + strings.Trim(opt.APIPath, "/")
	client := newClient(ctx, opt)
	srv := rest.NewClient(client).SetRoot(base).SetHeader(deviceHeader, opt.DeviceID).SetErrorHandler(errorHandler)
	f := &Fs{name: name, root: strings.Trim(root, "/"), opt: opt, srv: srv, download: rest.NewClient(client), downloadURLs: cache.New(),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))}
	f.auth = &auth{name: name, opt: opt, m: m, srv: srv, httpClient: client}
	f.features = (&fs.Features{CanHaveEmptyDirectories: true}).Fill(ctx, f)
	if !opt.MetadataCache {
		f.features.ChangeNotify = nil
	}
	if opt.FlatNamespace {
		f.features.DirMove = nil
		if f.root != "" && !validFlatPath(f.root) {
			return nil, errors.New("flat namespace root must be a relative path without empty, dot or parent components")
		}
	}
	f.dirCache = dircache.New(f.root, opt.RootFolderID, f)
	keepFs := false
	defer func() {
		if !keepFs {
			_ = f.Shutdown(ctx)
		}
	}()
	uploadURL := opt.UploadURL
	if uploadURL == "" {
		info, err := f.serverInfo(ctx)
		if err != nil {
			return nil, err
		}
		uploadURL = info.UploadURL
	}

	if uploadURL == "" {
		uploadURL = opt.URL
	}
	upload, err := parseURL(uploadURL)
	if err != nil {
		return nil, fmt.Errorf("invalid upload_url: %w", err)
	}

	if strings.HasPrefix(opt.URL, "https:") && upload.Scheme != "https" {
		return nil, errors.New("upload server must use HTTPS")
	}
	f.uploadURL = strings.TrimRight(upload.String(), "/") + "/" + strings.Trim(opt.APIPath, "/")
	err = f.callRead(ctx, func() (bool, error) {
		_, err := f.auth.prepare(ctx)
		return errors.Is(err, errCloudFrontBlocked), err
	})
	if err != nil {
		return nil, err
	}
	if opt.ResumeUploads {
		if err := f.startUploadJournal(ctx); err != nil {
			return nil, fmt.Errorf("start upload recovery: %w", err)
		}
	}
	if opt.MetadataCache {
		if err := f.startMetadataCache(ctx); err != nil {
			return nil, fmt.Errorf("start metadata cache: %w", err)
		}
	}
	if opt.AsyncUpload {
		validationCtx, cancel := context.WithCancel(context.Background())
		f.validationCancel = cancel
		f.validation, err = batcher.New(ctx, f, func(_ context.Context, items []validationItem, results []string, itemErrors []error) error {
			return f.checkUploads(validationCtx, items, results, itemErrors)
		}, batcher.Options{Mode: "sync", Size: pageSize, MaxBatchSize: pageSize, Timeout: 20 * time.Millisecond})
		if err != nil {
			return nil, err
		}
	}
	deleteMode, deleteSize := "sync", min(opt.DeleteBatchSize, max(1, fs.GetConfig(ctx).Checkers))
	if opt.AsyncDelete {
		deleteMode, deleteSize = "async", opt.DeleteBatchSize
	}
	f.deletions, err = batcher.New(ctx, f, f.commitDeletes, batcher.Options{
		Mode: deleteMode, Size: deleteSize,
		MaxBatchSize: deleteBatchLimit, Timeout: 20 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}

	if opt.FlatNamespace {
		if f.root == "" {
			keepFs = true
			return f, nil
		}
		err = fs.ErrorDirNotFound
	} else {
		err = f.dirCache.FindRoot(ctx, false)
	}
	if err == nil {
		keepFs = true
		return f, nil
	}

	if !errors.Is(err, fs.ErrorDirNotFound) {
		return nil, err
	}
	parent, leaf := dircache.SplitPath(f.root)
	originalRoot, originalCache := f.root, f.dirCache
	f.root = parent
	f.dirCache = dircache.New(parent, opt.RootFolderID, f)
	_, err = f.newObject(ctx, leaf, true)
	if err == nil {
		keepFs = true
		return f, fs.ErrorIsFile
	}
	f.root, f.dirCache = originalRoot, originalCache
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) {
		keepFs = true
		return f, nil
	}
	return nil, err
}

// Name returns the remote name.
func (f *Fs) Name() string { return f.name }

// Root returns the root path.
func (f *Fs) Root() string { return f.root }

// String describes the remote.
func (f *Fs) String() string { return "OneMediaHub " + f.opt.URL + ":" + f.root }

// Precision returns the timestamp precision accepted on upload.
func (f *Fs) Precision() time.Duration { return time.Second }

// Hashes returns no hashes: SAPI ETags have no documented checksum contract.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Features returns the optional backend operations.
func (f *Fs) Features() *fs.Features { return f.features }

func (f *Fs) serverInfo(ctx context.Context) (*api.ServerInfo, error) {
	var info api.ServerInfo
	err := f.callRead(ctx, func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &rest.Opts{Method: http.MethodGet, Path: "/system/information", Parameters: url.Values{"action": {"get"}}, NoRedirect: true}, nil, &info)
		return retry(ctx, resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("read OneMediaHub server information: %w", err)
	}

	if info.Error != nil {
		return nil, info.Error
	}
	return &info, nil
}

var errCloudFrontBlocked = errors.New("request blocked by CloudFront")

func errorHandler(resp *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorSize))
	_ = resp.Body.Close()
	var reply api.Response
	if readErr == nil && json.Unmarshal(body, &reply) == nil && reply.Error != nil {
		return reply.Error
	}
	var err error
	if req := resp.Request; req != nil && req.URL != nil {
		err = fmt.Errorf("OneMediaHub HTTP error: %s (%s %s%s action=%q)", resp.Status, req.Method, req.URL.Host, req.URL.EscapedPath(), req.URL.Query().Get("action"))
	} else {
		err = fmt.Errorf("OneMediaHub HTTP error: %s", resp.Status)
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	// O2's temporary edge blocks use this page rather than a SAPI error.
	if readErr == nil && resp.StatusCode == http.StatusForbidden && strings.EqualFold(resp.Header.Get("Server"), "CloudFront") && contentType == "text/html" &&
		bytes.Contains(body, []byte("ERROR: The request could not be satisfied")) && bytes.Contains(body, []byte("Request blocked.")) {
		return fmt.Errorf("%w: %v", errCloudFrontBlocked, err)
	}
	return err
}

func retry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if errors.Is(err, errCloudFrontBlocked) {
		return true, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}), err
}

func (f *Fs) callRead(ctx context.Context, run pacer.Paced) error {
	var blockedUntil time.Time
	return f.pacer.Call(func() (bool, error) {
		// Long edge-block cooldowns must be interruptible by cancellation.
		if delay := time.Until(blockedUntil); delay > 0 {
			fs.Debugf(f, "CloudFront blocked the request; waiting %v before retrying", delay)
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-timer.C:
			}
		}
		retry, err := run()
		if retry && errors.Is(err, errCloudFrontBlocked) {
			blockedUntil = time.Now().Add(cloudFrontBlockDelay)
		}
		return retry, err
	})
}

// call checks JSON errors even on HTTP success and renews expired sessions once.
func (f *Fs) call(ctx context.Context, opts rest.Opts, request any) (reply api.Response, err error) {
	action := opts.Parameters.Get("action")
	readOnly := opts.Body == nil && (action == "get" || action == "get-storage-space")
	run := func() (bool, error) {
		for attempt := 0; attempt < maxAuthAttempts; attempt++ {
			state, err := f.auth.prepare(ctx)
			if err != nil {
				if readOnly && errors.Is(err, errCloudFrontBlocked) {
					return retry(ctx, nil, err)
				}
				return false, err
			}
			respReply, resp, err := f.send(ctx, opts, request, state)
			reply = respReply
			var apiErr *api.Error
			expired := (resp != nil && resp.StatusCode == http.StatusUnauthorized) || (errors.As(err, &apiErr) && apiErr.Code == invalidKeyCode)
			if expired {
				f.auth.invalidate(state.session)
				if attempt == 0 && opts.Body == nil {
					continue
				}
			}
			if errors.Is(err, errCloudFrontBlocked) && !readOnly {
				return false, err
			}
			return retry(ctx, resp, err)
		}
		return false, errors.New("session renewal failed")
	}
	// Only read operations may be replayed after ambiguous network failures.
	if readOnly {
		err = f.callRead(ctx, run)
	} else {
		err = f.pacer.CallNoRetry(run)
	}
	return reply, err
}

func (f *Fs) send(ctx context.Context, opts rest.Opts, request any, state authState) (api.Response, *http.Response, error) {
	session := state.session
	attemptOpts := opts
	attemptOpts.Parameters = maps.Clone(opts.Parameters)
	if attemptOpts.Parameters == nil {
		attemptOpts.Parameters = url.Values{}
	}
	if session.Key != "" {
		attemptOpts.Parameters.Set("validationkey", session.Key)
	}
	attemptOpts.ExtraHeaders = map[string]string{}
	maps.Copy(attemptOpts.ExtraHeaders, opts.ExtraHeaders)
	cookie := &http.Cookie{Name: sessionCookie, Value: session.ID}
	attemptOpts.ExtraHeaders["Cookie"] = cookie.String()
	if state.header != "" {
		attemptOpts.ExtraHeaders["Authorization"] = state.header
	}
	attemptOpts.NoRedirect = true
	if opts.ContentLength != nil {
		size := *opts.ContentLength
		attemptOpts.ContentLength = &size
	}
	var reply api.Response
	// Delete APIs can return an empty HTTP 200 body.
	resp, err := f.srv.CallJSON(ctx, &attemptOpts, request, nil)
	if saveErr := f.auth.saveHeader(resp, state.accessToken); saveErr != nil {
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
		return reply, resp, saveErr
	}

	if err != nil || opts.NoResponse {
		return reply, resp, err
	}
	b, err := rest.ReadBody(resp)
	if err == nil && len(bytes.TrimSpace(b)) != 0 {
		err = json.Unmarshal(b, &reply)
	}
	if err == nil && reply.Error != nil {
		err = reply.Error
	}
	return reply, resp, err
}

func (f *Fs) request(ctx context.Context, method, endpoint, action string, params url.Values, data any) (api.Response, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("action", action)
	var request any
	if data != nil {
		request = map[string]any{"data": data}
	}
	return f.call(ctx, rest.Opts{Method: method, Path: endpoint, Parameters: params}, request)
}

func pageParams(offset int) url.Values {
	params := url.Values{"limit": {strconv.Itoa(pageSize)}}
	// Some servers reject an explicit zero offset.
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	return params
}

type metadataSnapshot struct {
	Version        int                       // Version identifies the cache format.
	Anchor         int64                     // Anchor is the last applied server response time in milliseconds.
	Folders        []api.Folder              // Folders contains account folders.
	Media          map[api.ID]api.Media      // Media indexes account files by ID.
	Pending        []api.ID                  // Pending identifies locked or unavailable media to fetch again.
	PendingFolders []api.ID                  // PendingFolders identifies unavailable or locked folders to fetch again.
	FlatPaths      map[api.ID]flatPathRecord `json:",omitempty"` // FlatPaths caches validated immutable path mappings by raw media identity.
}

type metadataCache struct {
	refreshMu    sync.Mutex
	mu           sync.Mutex
	db           *kv.DB
	state        *metadataSnapshot
	checked      time.Time
	byName       map[mediaKey]api.ID
	dirty        bool
	flatNames    map[mediaKey]int
	flatDirs     map[mediaKey]int
	flatBad      map[string]int
	flatPaths    map[mediaKey]string
	flatReady    bool
	flatRevision uint64
}

type mediaKey struct {
	parent string
	name   string
}

func (f *Fs) mediaKey(item api.Media) mediaKey {
	parent := string(item.FolderID)
	if parent == "0" {
		parent = ""
	}
	name := f.opt.Enc.ToStandardName(item.Name)
	if f.opt.FlatNamespace && item.Size == 0 {
		if canonical, ok := flatDirectoryAlias(name); ok {
			name = canonical
		}
	}
	return mediaKey{parent: parent, name: name}
}

func (f *Fs) cacheMedia(item api.Media, remove bool) {
	remove = remove || item.IsDeleted()
	if f.downloadURLs != nil {
		f.downloadURLs.DeletePrefix(string(item.ID) + "/")
	}
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.dirty = true
		if old, found := c.state.Media[item.ID]; found {
			f.indexFlatMedia(c, old, -1)
		}
		if f.opt.FlatNamespace && (isFlatMapping(item.Name) || isFlatMapping(c.state.Media[item.ID].Name)) {
			c.flatReady = false
			c.flatRevision++
		}
		if old, ok := c.state.Media[item.ID]; ok && (remove || f.mediaKey(old) != f.mediaKey(item)) {
			key := f.mediaKey(old)
			if c.byName[key] == item.ID {
				delete(c.byName, key)
				for id, other := range c.state.Media {
					if id != item.ID && !other.IsDeleted() && f.mediaKey(other) == key && id > c.byName[key] {
						c.byName[key] = id
					}
				}
			}
		}
		delete(c.state.Media, item.ID)
		if !remove {
			c.state.Media[item.ID] = item
			f.indexFlatMedia(c, item, 1)
			key := f.mediaKey(item)
			if item.ID >= c.byName[key] {
				c.byName[key] = item.ID
			}
		}
	}
}

func (f *Fs) cacheFolder(folder api.Folder, remove bool) {
	remove = remove || folder.IsDeleted()
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.dirty = true
		c.state.Folders = slices.DeleteFunc(c.state.Folders, func(old api.Folder) bool { return old.ID == folder.ID })
		if !remove {
			c.state.Folders = append(c.state.Folders, folder)
		}
	}
}

type metadataOp struct {
	state      *metadataSnapshot
	write      bool
	cursorOnly bool
}

func (op *metadataOp) Do(_ context.Context, bucket kv.Bucket) error {
	key := []byte("metadata")
	if !op.write {
		if b := bucket.Get(key); b != nil {
			if err := json.Unmarshal(b, &op.state); err != nil {
				return err
			}
			if op.state == nil {
				return errors.New("invalid cached metadata snapshot")
			}
			if b := bucket.Get([]byte("anchor")); b != nil {
				anchor, err := strconv.ParseInt(string(b), 10, 64)
				if err != nil || anchor < op.state.Anchor {
					return errors.New("invalid cached changes cursor")
				}
				op.state.Anchor = anchor
			}
		}
		return nil
	}
	if op.cursorOnly {
		return bucket.Put([]byte("anchor"), []byte(strconv.FormatInt(op.state.Anchor, 10)))
	}
	b, err := json.Marshal(op.state)
	if err != nil {
		return err
	}
	if err := bucket.Put(key, b); err != nil {
		return err
	}
	return bucket.Delete([]byte("anchor"))
}

func (f *Fs) accountID(ctx context.Context) (string, error) {
	reply, err := f.request(ctx, http.MethodGet, "/profile", "get", nil, nil)
	if err != nil {
		return "", err
	}
	var profile struct {
		User struct {
			Generic struct {
				ID string `json:"userid"`
			} `json:"generic"`
		} `json:"user"`
	}
	if err := json.Unmarshal(reply.Data, &profile); err != nil {
		return "", err
	}
	if profile.User.Generic.ID == "" {
		return "", errors.New("profile returned no account ID")
	}
	return profile.User.Generic.ID, nil
}

func (f *Fs) startMetadataCache(ctx context.Context) error {
	id, err := f.accountID(ctx)
	if err != nil {
		return err
	}
	f.metadata = &metadataCache{}
	if kv.Supported() {
		// Token rotation must not change the account's cache namespace.
		scope, _ := json.Marshal([]string{strings.TrimRight(f.opt.URL, "/"), f.opt.APIPath, id})
		digest := sha256.Sum256(scope)
		db, err := kv.Start(ctx, fmt.Sprintf("onemediahub-%x", digest[:]), f)
		if err != nil {
			return err
		}
		f.metadata.db = db
		op := &metadataOp{}
		if err := db.Do(false, op); err != nil && !errors.Is(err, kv.ErrEmpty) {
			fs.Debugf(f, "Discarding unreadable metadata cache: %v", err)
		} else if op.state != nil && op.state.Version == metadataVersion && op.state.Anchor > 0 && op.state.Media != nil {
			f.metadata.state = op.state
			fs.Debugf(f, "Loaded metadata cache")
		}
	}
	return f.syncMetadata(ctx)
}

func (f *Fs) changes(ctx context.Context, anchor int64) (map[string]api.Changes, int64, error) {
	from := time.UnixMilli(anchor).UTC()
	if anchor > 0 {
		// SAPI dates have second precision, while requesttime has millisecond precision.
		from = from.Add(-time.Second)
	}
	reply, err := f.request(ctx, http.MethodGet, "/profile/changes", "get", url.Values{
		"from": {from.Format(dateFormat)}, "type": {"folder,file,picture,video,audio"}, "responsetime": {"true"},
		"sortby": {"creationdate"}, "sortorder": {"descending"}, "locked": {"true"},
	}, nil)
	if err != nil {
		return nil, 0, err
	}
	timestamp, err := reply.RequestTime.Int64()
	if err != nil || timestamp <= 0 || timestamp < anchor {
		return nil, 0, errors.New("changes API returned an invalid requesttime")
	}
	if reply.More {
		return nil, 0, errors.New("changes API returned an incomplete result")
	}
	var raw map[string]map[string][]api.ID
	if err := json.Unmarshal(reply.Data, &raw); err != nil || raw == nil {
		return nil, 0, errors.New("changes API returned invalid changes")
	}
	changes := make(map[string]api.Changes, len(raw))
	for source, statuses := range raw {
		if !slices.Contains([]string{"folder", "file", "picture", "video", "audio"}, source) {
			return nil, 0, fmt.Errorf("changes API returned unsupported source %q", source)
		}
		for status := range statuses {
			if !slices.Contains([]string{"N", "U", "D", "S", "L"}, status) {
				return nil, 0, fmt.Errorf("changes API returned unsupported status %q", status)
			}
		}
		changes[source] = api.Changes{New: statuses["N"], Updated: statuses["U"], Deleted: slices.Concat(statuses["D"], statuses["S"]), Locked: statuses["L"]}
	}
	return changes, timestamp, nil
}

func (f *Fs) syncMetadata(ctx context.Context) error {
	return f.updateMetadata(ctx, false)
}

func (f *Fs) updateMetadata(ctx context.Context, force bool) error {
	c := f.metadata
	if c == nil {
		return nil
	}
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	if force {
		c.checked = time.Time{}
	}
	foldersChanged, err := f.refreshMetadata(ctx)
	c.mu.Unlock()
	// FindLeaf reads metadata while dircache holds its lock.
	if foldersChanged {
		f.dirCache.ResetRoot()
	}
	return err
}

func (f *Fs) expireMetadata() {
	if c := f.metadata; c != nil {
		c.mu.Lock()
		c.checked = time.Time{}
		c.mu.Unlock()
	}
}

// refreshMetadata runs with the metadata cache locked.
func (f *Fs) refreshMetadata(ctx context.Context) (bool, error) {
	c := f.metadata
	if !c.checked.IsZero() && time.Since(c.checked) < time.Duration(f.opt.MetadataCacheTime) {
		return false, nil
	}
	next := &metadataSnapshot{Version: metadataVersion, Media: map[api.ID]api.Media{}}
	if c.state != nil {
		*next = *c.state
		next.Media = maps.Clone(c.state.Media)
		next.Folders = slices.Clone(c.state.Folders)
		next.FlatPaths = maps.Clone(c.state.FlatPaths)
	}
	changes, timestamp, err := f.changes(ctx, next.Anchor)
	if err != nil {
		return false, err
	}
	folderChanges := changes["folder"]
	foldersChanged := c.state == nil || len(next.PendingFolders)+len(folderChanges.New)+len(folderChanges.Updated)+len(folderChanges.Deleted)+len(folderChanges.Locked) > 0
	if c.state == nil {
		next.Folders, err = f.fetchFolders(ctx, nil)
		if err != nil {
			return false, err
		}
	}
	if foldersChanged {
		ids := map[api.ID]struct{}{}
		for _, id := range slices.Concat(next.PendingFolders, folderChanges.New, folderChanges.Updated, folderChanges.Locked) {
			ids[id] = struct{}{}
		}
		for _, id := range folderChanges.Deleted {
			delete(ids, id)
			next.Folders = slices.DeleteFunc(next.Folders, func(folder api.Folder) bool { return folder.ID == id })
		}
		if c.state == nil {
			for _, folder := range next.Folders {
				delete(ids, folder.ID)
			}
		}
		for batch := range slices.Chunk(slices.Sorted(maps.Keys(ids)), pageSize) {
			folders, err := f.fetchFolders(ctx, batch)
			if err != nil {
				return false, err
			}
			for _, folder := range folders {
				next.Folders = slices.DeleteFunc(next.Folders, func(old api.Folder) bool { return old.ID == folder.ID })
				if !folder.IsDeleted() {
					next.Folders = append(next.Folders, folder)
				}
				if folder.Status != "L" && !slices.Contains(folderChanges.Locked, folder.ID) {
					delete(ids, folder.ID)
				}
			}
		}
		for _, folder := range next.Folders {
			if folder.Status == "L" || slices.Contains(folderChanges.Locked, folder.ID) {
				ids[folder.ID] = struct{}{}
			}
			if folder.ParentID != "" && folder.ParentID != "0" && !slices.Contains(folderChanges.Deleted, folder.ParentID) && !slices.ContainsFunc(next.Folders, func(parent api.Folder) bool { return parent.ID == folder.ParentID }) {
				ids[folder.ParentID] = struct{}{}
			}
		}
		next.PendingFolders = slices.Sorted(maps.Keys(ids))
	}
	ids := map[api.ID]struct{}{}
	locked := map[api.ID]struct{}{}
	changedDownloads := map[api.ID]struct{}{}
	metadataChanged := c.state == nil || c.dirty || foldersChanged
	indexChanged := c.byName == nil
	for _, id := range next.Pending {
		ids[id] = struct{}{}
	}
	for source, change := range changes {
		if source == "folder" {
			continue
		}
		for _, id := range slices.Concat(change.New, change.Updated, change.Locked) {
			ids[id] = struct{}{}
		}
		for _, id := range change.Locked {
			locked[id] = struct{}{}
			changedDownloads[id] = struct{}{}
		}
		for _, id := range change.Deleted {
			if _, ok := next.Media[id]; ok {
				metadataChanged, indexChanged = true, true
			}
			delete(next.Media, id)
			delete(ids, id)
			changedDownloads[id] = struct{}{}
		}
	}
	ordered := slices.Sorted(maps.Keys(ids))
	for batch := range slices.Chunk(ordered, pageSize) {
		if err := f.fetchMedia(ctx, batch, true, func(item api.Media) error {
			old, found := next.Media[item.ID]
			if item.IsDeleted() {
				delete(next.Media, item.ID)
				delete(ids, item.ID)
				metadataChanged = metadataChanged || found
				indexChanged = indexChanged || found
				changedDownloads[item.ID] = struct{}{}
				return nil
			}
			if sameMediaContent(old, item) && item.Status != "L" {
				item.URL = old.URL
			} else {
				changedDownloads[item.ID] = struct{}{}
			}
			if !found || old != item {
				metadataChanged = true
				indexChanged = indexChanged || !found || f.mediaKey(old) != f.mediaKey(item)
				next.Media[item.ID] = item
			}
			if _, isLocked := locked[item.ID]; !isLocked && item.Status != "L" {
				delete(ids, item.ID)
			}
			return nil
		}); err != nil {
			return false, err
		}
	}
	// Unfinished uploads may not yet be visible to the metadata endpoint.
	next.Pending = slices.Sorted(maps.Keys(ids))
	if c.state != nil && !slices.Equal(c.state.Pending, next.Pending) {
		metadataChanged = true
	}
	next.Anchor = timestamp
	if c.db != nil {
		if err := c.db.Do(true, &metadataOp{state: next, write: true, cursorOnly: !metadataChanged}); err != nil {
			return false, fmt.Errorf("save metadata cache: %w", err)
		}
	}
	for id := range changedDownloads {
		f.downloadURLs.DeletePrefix(string(id) + "/")
	}
	c.state = next
	c.dirty = false
	if f.opt.FlatNamespace && (metadataChanged || c.flatNames == nil) {
		c.flatReady = false
		c.flatRevision++
		f.rebuildFlatIndex(c)
	}
	if indexChanged {
		c.byName = make(map[mediaKey]api.ID, len(next.Media))
		for _, id := range slices.Sorted(maps.Keys(next.Media)) {
			if item := next.Media[id]; !item.IsDeleted() {
				c.byName[f.mediaKey(item)] = id
			}
		}
	}
	c.checked = time.Now()
	fs.Debugf(f, "Refreshed metadata cache: fetched %d media IDs, %d pending", len(ordered), len(next.Pending))
	return foldersChanged, nil
}

// Shutdown finishes deletion batches, stops upload validation and closes the persistent caches.
func (f *Fs) Shutdown(ctx context.Context) error {
	if f.deletions != nil {
		f.deletions.Shutdown()
	}
	if f.validationCancel != nil {
		f.validationCancel()
	}
	if f.validation != nil {
		f.validation.Shutdown()
	}
	err := f.asyncDeleteError(ctx)
	if closeErr := f.stopUploadJournal(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if c := f.metadata; c != nil {
		c.refreshMu.Lock()
		defer c.refreshMu.Unlock()
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.db != nil && !c.db.IsStopped() {
			if c.dirty {
				if saveErr := c.db.Do(true, &metadataOp{state: c.state, write: true}); saveErr != nil {
					err = errors.Join(err, saveErr)
				}
			}
			if closeErr := c.db.Stop(false); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			c.db = nil
			return err
		}
	}
	return err
}

func (f *Fs) folders(ctx context.Context) ([]api.Folder, error) {
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		return slices.Clone(c.state.Folders), nil
	}
	return f.fetchFolders(ctx, nil)
}

func (f *Fs) fetchFolders(ctx context.Context, ids []api.ID) ([]api.Folder, error) {
	var folders []api.Folder
	seen := map[api.ID]struct{}{}
	for offset := 0; ; {
		method, params := http.MethodGet, pageParams(offset)
		var request any
		if ids != nil {
			method, params, request = http.MethodPost, nil, map[string]any{"ids": ids}
		}
		reply, err := f.request(ctx, method, "/media/folder", "get", params, request)
		if err != nil {
			return nil, err
		}
		var data struct {
			Folders []api.Folder `json:"folders"`
		}

		if err := json.Unmarshal(reply.Data, &data); err != nil {
			return nil, err
		}
		if ids != nil && reply.More {
			return nil, errors.New("folder ID lookup returned an incomplete result")
		}
		// Some servers repeat the root folder on the last page.
		for _, folder := range data.Folders {
			if ids != nil && !slices.Contains(ids, folder.ID) {
				return nil, errors.New("folder ID lookup returned an unexpected ID")
			}
			if _, ok := seen[folder.ID]; ok {
				continue
			}
			seen[folder.ID] = struct{}{}
			folders = append(folders, folder)
		}
		if ids != nil || len(data.Folders) < pageSize {
			return folders, nil
		}
		offset += len(data.Folders)
	}
}

func sameParent(id api.ID, parent string) bool {
	return string(id) == parent || parent == "" && id == "0"
}

// FindLeaf finds a child directory for dircache.
func (f *Fs) FindLeaf(ctx context.Context, parent, leaf string) (string, bool, error) {
	folders, err := f.folders(ctx)
	if err != nil {
		return "", false, err
	}
	// Compare displayed names to resolve server-created names such as "/".
	for _, folder := range folders {
		if sameParent(folder.ParentID, parent) && f.opt.Enc.ToStandardName(folder.Name) == leaf && !folder.IsDeleted() {
			return string(folder.ID), true, nil
		}
	}
	return "", false, nil
}

// CreateDir creates a child directory for dircache.
func (f *Fs) CreateDir(ctx context.Context, parent, leaf string) (string, error) {
	folder := api.Folder{Name: f.opt.Enc.FromStandardName(leaf), ParentID: api.ID(parent)}
	reply, err := f.request(ctx, http.MethodPost, "/media/folder", "save", nil, folder)
	if err != nil {
		f.expireMetadata()
		return "", err
	}

	if reply.ID == "" {
		f.expireMetadata()
		return "", errors.New("folder creation returned no ID")
	}
	folder.ID = reply.ID
	folder.Date = time.Now().UnixMilli()
	f.cacheFolder(folder, false)
	return string(reply.ID), nil
}

func (f *Fs) media(ctx context.Context, ids []api.ID, visit func(api.Media) error) error {
	if c := f.metadata; c != nil && ids == nil {
		c.mu.Lock()
		items := slices.Collect(maps.Values(c.state.Media))
		c.mu.Unlock()
		for _, item := range items {
			if item.IsDeleted() {
				continue
			}
			if err := visit(item); err != nil {
				return err
			}
		}
		return nil
	}
	return f.fetchMedia(ctx, ids, false, visit)
}

func (f *Fs) fetchMedia(ctx context.Context, ids []api.ID, includeDeleted bool, visit func(api.Media) error) error {
	for offset := 0; ; {
		params := url.Values{}
		data := map[string]any{"fields": []string{"name", "size", "modificationdate", "url", "folderid", "etag"}}
		if ids != nil {
			data["ids"] = ids
		} else {
			params = pageParams(offset)
		}
		reply, err := f.request(ctx, http.MethodPost, "/media", "get", params, data)
		if err != nil {
			return err
		}
		var result struct {
			Media []api.Media `json:"media"`
			More  bool        `json:"more"`
		}

		if err := json.Unmarshal(reply.Data, &result); err != nil {
			return err
		}
		for _, item := range result.Media {
			if !includeDeleted && item.IsDeleted() {
				continue
			}

			if item.FolderID == "" {
				item.FolderID = item.Folder
			}

			if err := visit(item); err != nil {
				return err
			}
		}

		if ids != nil || !(reply.More || result.More) {
			return nil
		}

		if len(result.Media) == 0 {
			return errors.New("server reports more media but returned an empty page")
		}
		offset += len(result.Media)
	}
}

const (
	flatPrefix       = "rclone-flat-v1-"
	flatNameLimit    = 255
	flatMappingLimit = 1 << 20
)

var flatEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

var errFlatMappingChanged = errors.New("flat path mappings changed while validating")

func validFlatPath(remote string) bool {
	return remote != "" && remote != "." && !strings.HasPrefix(remote, "/") && remote == path.Clean(remote) && remote != ".." && !strings.HasPrefix(remote, "../")
}

func flatName(remote string, directory bool) (string, error) {
	if !validFlatPath(remote) {
		return "", errors.New("flat namespace path must be relative without empty, dot or parent components")
	}
	kind := "f-"
	if directory {
		kind = "d-"
	}
	// One case of ASCII avoids provider case folding and Unicode normalization.
	name := flatPrefix + kind + flatEncoding.EncodeToString([]byte(remote))
	if len(name) > flatNameLimit {
		name = flatPrefix + kind + "h-" + flatDigest(remote)
	}
	return name, nil
}

func flatDigest(remote string) string {
	digest := sha256.Sum256([]byte(remote))
	return hex.EncodeToString(digest[:])
}

func flatMappingName(remote string) string { return flatPrefix + "p-" + flatDigest(remote) }

func isFlatMapping(name string) bool { return strings.HasPrefix(name, flatPrefix+"p-") }

func parseFlatHashName(name string) (digest string, directory, mapping, ok bool) {
	name, found := strings.CutPrefix(name, flatPrefix)
	if !found {
		return "", false, false, false
	}
	switch {
	case strings.HasPrefix(name, "p-"):
		digest, mapping = strings.TrimPrefix(name, "p-"), true
	case strings.HasPrefix(name, "f-h-"):
		digest = strings.TrimPrefix(name, "f-h-")
	case strings.HasPrefix(name, "d-h-"):
		digest, directory = strings.TrimPrefix(name, "d-h-"), true
	default:
		return "", false, false, false
	}
	b, err := hex.DecodeString(digest)
	return digest, directory, mapping, err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == digest
}

type flatPathPayload struct {
	Version int    `json:"version"`        // Version identifies the path mapping format.
	Path    string `json:"path,omitempty"` // Path contains the UTF-8 logical path when PathBytes is empty.
	// PathBytes contains the exact bytes of a non-UTF-8 path as unpadded base64.
	PathBytes string `json:"path_bytes,omitempty"`
}

type flatPathRecord struct {
	Name     string // Name is the server filename.
	ETag     string // ETag identifies the server content version.
	FolderID api.ID // FolderID identifies the physical parent.
	Size     int64  // Size is the mapping content length.
	Modified int64  // Modified is the server modification time in milliseconds.
	Date     int64  // Date is the server date in milliseconds.
	flatPathPayload
}

func flatPayload(remote string) flatPathPayload {
	p := flatPathPayload{Version: 1}
	if utf8.ValidString(remote) {
		p.Path = remote
	} else {
		p.PathBytes = base64.RawStdEncoding.EncodeToString([]byte(remote))
	}
	return p
}

func flatPayloadPath(payload flatPathPayload, name string) (string, error) {
	if payload.Version != 1 || (payload.Path == "") == (payload.PathBytes == "") {
		return "", errors.New("invalid flat path mapping")
	}
	remote := payload.Path
	if payload.PathBytes != "" {
		b, err := base64.RawStdEncoding.DecodeString(payload.PathBytes)
		if err != nil || base64.RawStdEncoding.EncodeToString(b) != payload.PathBytes || utf8.Valid(b) {
			return "", errors.New("invalid flat path byte mapping")
		}
		remote = string(b)
	}
	if !validFlatPath(remote) || flatMappingName(remote) != name {
		return "", errors.New("flat path mapping does not match its name")
	}
	return remote, nil
}

func flatRecordMatches(record flatPathRecord, item api.Media) bool {
	return record.Name == item.Name && record.FolderID == item.FolderID && record.ETag == item.ETag &&
		record.Size == item.Size && record.Modified == item.Modified && record.Date == item.Date
}

func (f *Fs) readFlatMapping(ctx context.Context, item api.Media, records map[api.ID]flatPathRecord) (string, error) {
	if item.IsDeleted() || item.Size <= 0 || item.Size > flatMappingLimit {
		return "", errors.New("invalid flat path mapping media")
	}
	name := f.opt.Enc.ToStandardName(item.Name)
	if record, found := records[item.ID]; found && flatRecordMatches(record, item) {
		if remote, err := flatPayloadPath(record.flatPathPayload, name); err == nil {
			return remote, nil
		}
	}
	o := &Object{fs: f, info: item}
	body, err := o.Open(ctx)
	if err != nil {
		return "", fmt.Errorf("open flat path mapping: %w", err)
	}
	b, readErr := io.ReadAll(io.LimitReader(body, flatMappingLimit+1))
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		return "", fmt.Errorf("read flat path mapping: %w", errors.Join(readErr, closeErr))
	}
	if len(b) > flatMappingLimit || int64(len(b)) != item.Size {
		return "", errors.New("invalid flat path mapping size")
	}
	var payload flatPathPayload
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return "", fmt.Errorf("decode flat path mapping: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return "", errors.New("flat path mapping has trailing data")
	}
	remote, err := flatPayloadPath(payload, name)
	if err != nil {
		return "", err
	}
	records[item.ID] = flatPathRecord{Name: item.Name, FolderID: item.FolderID, ETag: item.ETag,
		Size: item.Size, Modified: item.Modified, Date: item.Date, flatPathPayload: payload}
	return remote, nil
}

// flatMappings validates referenced remote mappings without holding the metadata lock.
func (f *Fs) flatMappings(ctx context.Context, items []api.Media) (map[mediaKey]string, error) {
	f.flatMu.Lock()
	defer f.flatMu.Unlock()
	records := maps.Clone(f.flatRecords)
	if records == nil {
		records = make(map[api.ID]flatPathRecord)
	}
	if c := f.metadata; c != nil {
		c.mu.Lock()
		maps.Copy(records, c.state.FlatPaths)
		c.mu.Unlock()
	}
	mappings := make(map[mediaKey][]api.Media)
	references := make(map[mediaKey]bool)
	for _, item := range items {
		if !sameParent(item.FolderID, f.opt.RootFolderID) || item.IsDeleted() {
			continue
		}
		key := f.mediaKey(item)
		if _, _, ok := parseFlatName(key.name); ok {
			continue
		}
		digest, _, mapping, ok := parseFlatHashName(key.name)
		if !ok {
			if strings.HasPrefix(key.name, "rclone-flat-") {
				return nil, fmt.Errorf("invalid or unsupported flat namespace name for media %s", item.ID)
			}
			continue
		}
		key.name = flatPrefix + "p-" + digest
		if mapping {
			mappings[key] = append(mappings[key], item)
		} else {
			references[key] = true
		}
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return nil, err
	}
	for _, record := range pending {
		if !sameParent(record.FolderID, f.opt.RootFolderID) {
			continue
		}
		if digest, _, mapping, ok := parseFlatHashName(f.opt.Enc.ToStandardName(record.Name)); ok && !mapping {
			parent := string(record.FolderID)
			if parent == "0" {
				parent = ""
			}
			references[mediaKey{parent: parent, name: flatPrefix + "p-" + digest}] = true
		}
	}
	paths := make(map[mediaKey]string)
	for key := range references {
		if len(mappings[key]) == 0 {
			return nil, errors.New("missing flat path mapping")
		}
		for _, item := range mappings[key] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			remote, err := f.readFlatMapping(ctx, item, records)
			if err != nil {
				return nil, err
			}
			if old, found := paths[key]; found && old != remote {
				return nil, errors.New("conflicting flat path mappings")
			}
			paths[key] = remote
		}
	}
	for _, item := range items {
		if sameParent(item.FolderID, f.opt.RootFolderID) && !item.IsDeleted() {
			key := f.mediaKey(item)
			if _, _, mapping, ok := parseFlatHashName(key.name); ok && !mapping {
				if _, _, err := f.resolvedFlatName(key, paths); err != nil {
					return nil, err
				}
			}
		}
	}
	f.flatRecords = records
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.state.FlatPaths == nil {
			c.state.FlatPaths = make(map[api.ID]flatPathRecord)
		}
		for id, record := range records {
			if item, found := c.state.Media[id]; found && flatRecordMatches(record, item) {
				if old, found := c.state.FlatPaths[id]; !found || old != record {
					c.state.FlatPaths[id], c.dirty = record, true
				}
			}
		}
		f.rebuildFlatIndex(c)
		c.flatReady = true
		currentReferences := maps.Clone(references)
		for _, item := range c.state.Media {
			if !sameParent(item.FolderID, f.opt.RootFolderID) || item.IsDeleted() {
				continue
			}
			if digest, _, mapping, ok := parseFlatHashName(f.mediaKey(item).name); ok && !mapping {
				key := f.mediaKey(item)
				currentReferences[mediaKey{parent: key.parent, name: flatPrefix + "p-" + digest}] = true
				if _, _, err := f.resolvedFlatName(f.mediaKey(item), c.flatPaths); err != nil {
					c.flatReady = false
				}
			}
		}
		counts := make(map[mediaKey]int)
		for _, item := range c.state.Media {
			key := f.mediaKey(item)
			if !currentReferences[key] || item.IsDeleted() {
				continue
			}
			counts[key]++
			if !slices.ContainsFunc(mappings[key], func(old api.Media) bool {
				return old.ID == item.ID && old.Name == item.Name && old.FolderID == item.FolderID && old.ETag == item.ETag &&
					old.Size == item.Size && old.Modified == item.Modified && old.Date == item.Date
			}) {
				c.flatReady = false
			}
		}
		for key := range currentReferences {
			if counts[key] != len(mappings[key]) {
				c.flatReady = false
			}
		}
		if !c.flatReady {
			return paths, errFlatMappingChanged
		}
	}
	return paths, nil
}

func (f *Fs) resolvedFlatName(key mediaKey, paths map[mediaKey]string) (remote string, directory bool, err error) {
	if remote, directory, ok := parseFlatName(key.name); ok {
		return remote, directory, nil
	}
	digest, directory, mapping, ok := parseFlatHashName(key.name)
	if !ok || mapping {
		return "", false, errors.New("invalid flat namespace file name")
	}
	remote, found := paths[mediaKey{parent: key.parent, name: flatPrefix + "p-" + digest}]
	if !found {
		return "", false, errors.New("missing flat path mapping")
	}
	if canonical, err := flatName(remote, directory); err != nil || canonical != key.name {
		return "", false, errors.New("noncanonical flat namespace file name")
	}
	return remote, directory, nil
}

func (f *Fs) rebuildFlatIndex(c *metadataCache) {
	c.flatPaths = make(map[mediaKey]string)
	for id, record := range c.state.FlatPaths {
		item, found := c.state.Media[id]
		if !found || item.IsDeleted() || !flatRecordMatches(record, item) {
			delete(c.state.FlatPaths, id)
			c.dirty = true
			continue
		}
		key := f.mediaKey(item)
		if remote, err := flatPayloadPath(record.flatPathPayload, key.name); err == nil {
			c.flatPaths[key] = remote
		} else {
			delete(c.state.FlatPaths, id)
			c.dirty = true
		}
	}
	c.flatNames = make(map[mediaKey]int)
	c.flatDirs = make(map[mediaKey]int)
	c.flatBad = make(map[string]int)
	for _, item := range c.state.Media {
		f.indexFlatMedia(c, item, 1)
	}
}

func (f *Fs) ensureFlatNamespace(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c := f.metadata; c != nil {
			c.mu.Lock()
			ready := c.flatReady
			c.mu.Unlock()
			if ready {
				return nil
			}
		}
		var items []api.Media
		if err := f.media(ctx, nil, func(item api.Media) error { items = append(items, item); return ctx.Err() }); err != nil {
			return err
		}
		if _, err := f.flatMappings(ctx, items); err != nil && !errors.Is(err, errFlatMappingChanged) {
			return err
		}
		if f.metadata == nil {
			return nil
		}
	}
}

// ensureFlatMapping publishes and validates the immutable mapping before any hashed media.
func (f *Fs) ensureFlatMapping(ctx context.Context, full string) error {
	name, err := flatName(full, false)
	if err != nil || !strings.HasPrefix(name, flatPrefix+"f-h-") {
		return err
	}
	payload, err := json.Marshal(flatPayload(full))
	if err != nil {
		return err
	}
	if len(payload) > flatMappingLimit {
		return errors.New("flat path mapping exceeds its size limit")
	}
	f.flatMu.Lock()
	defer f.flatMu.Unlock()
	if f.flatRecords == nil {
		f.flatRecords = make(map[api.ID]flatPathRecord)
	}
	name = flatMappingName(full)
	var items []api.Media
	var ready bool
	var revision uint64
	if c := f.metadata; c != nil {
		c.mu.Lock()
		ready, revision = c.flatReady, c.flatRevision
		parent := f.opt.RootFolderID
		if parent == "0" {
			parent = ""
		}
		key := mediaKey{parent: parent, name: name}
		if c.flatNames[key] > 1 {
			for _, item := range c.state.Media {
				if f.mediaKey(item) == key && !item.IsDeleted() {
					items = append(items, item)
				}
			}
		} else if id, found := c.byName[key]; found {
			items = append(items, c.state.Media[id])
		}
		for _, item := range items {
			if record, found := c.state.FlatPaths[item.ID]; found {
				f.flatRecords[item.ID] = record
			}
		}
		c.mu.Unlock()
	} else if err := f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, f.opt.RootFolderID) && f.opt.Enc.ToStandardName(item.Name) == name {
			items = append(items, item)
		}
		return ctx.Err()
	}); err != nil {
		return err
	}
	created := len(items) == 0
	if created {
		o := &Object{fs: f, remote: full, flatMapping: true}
		src := object.NewStaticObjectInfo(full, time.Now(), int64(len(payload)), true, nil, f).WithMimeType("application/octet-stream")
		if err := o.Update(ctx, bytes.NewReader(payload), src); err != nil {
			return fmt.Errorf("create flat path mapping: %w", err)
		}
		items = append(items, o.info)
	}
	for _, item := range items {
		remote, err := f.readFlatMapping(ctx, item, f.flatRecords)
		if err != nil {
			return err
		}
		if remote != full {
			return errors.New("flat path mapping names a different destination")
		}
	}
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.state.FlatPaths == nil {
			c.state.FlatPaths = make(map[api.ID]flatPathRecord)
		}
		if c.flatNames[f.mediaKey(items[0])] != len(items) {
			return errors.New("flat path mapping candidates changed while publishing")
		}
		for _, item := range items {
			current, found := c.state.Media[item.ID]
			if !found || !flatRecordMatches(f.flatRecords[item.ID], current) {
				return errors.New("flat path mapping changed while publishing")
			}
			c.state.FlatPaths[item.ID] = f.flatRecords[item.ID]
			c.flatPaths[f.mediaKey(item)] = full
			c.dirty = true
		}
		// Publishing an orphan mapping leaves every existing file and directory index intact.
		if created && ready && c.flatRevision == revision+1 {
			c.flatReady = true
		}
	}
	return nil
}

func parseFlatName(name string) (remote string, directory, ok bool) {
	if len(name) > flatNameLimit {
		return "", false, false
	}
	encoded, found := strings.CutPrefix(name, flatPrefix)
	if !found || len(encoded) < 3 || encoded[1] != '-' || (encoded[0] != 'f' && encoded[0] != 'd') {
		return "", false, false
	}
	b, err := flatEncoding.DecodeString(encoded[2:])
	if err != nil || !validFlatPath(string(b)) || flatEncoding.EncodeToString(b) != encoded[2:] {
		return "", false, false
	}
	return string(b), encoded[0] == 'd', true
}

// flatDirectoryAlias recognizes provider-numbered copies of canonical directory markers.
func flatDirectoryAlias(name string) (string, bool) {
	// A name at the provider limit may have a truncated, but still decodable, base.
	if len(name) >= flatNameLimit || !strings.HasSuffix(name, ")") {
		return "", false
	}
	i := strings.LastIndex(name, " (")
	if i < 0 {
		return "", false
	}
	number := name[i+2 : len(name)-1]
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != number {
		return "", false
	}
	canonical := name[:i]
	if _, directory, ok := parseFlatName(canonical); ok && directory {
		return canonical, true
	}
	if _, directory, mapping, ok := parseFlatHashName(canonical); ok && directory && !mapping {
		return canonical, true
	}
	return "", false
}

func (f *Fs) flatPath(remote string) (string, error) {
	if remote != "" && !validFlatPath(remote) {
		return "", errors.New("flat namespace path must be relative without empty, dot or parent components")
	}
	if f.root == "" {
		return remote, nil
	}
	if remote == "" {
		return f.root, nil
	}
	return f.root + "/" + remote, nil
}

// indexFlatMedia maintains directory reference counts alongside the raw name index.
// The metadata lock must be held, and delta is 1 for addition or -1 for removal.
func (f *Fs) indexFlatMedia(c *metadataCache, item api.Media, delta int) {
	if c.flatNames == nil || item.IsDeleted() {
		return
	}
	key := f.mediaKey(item)
	if !strings.HasPrefix(key.name, "rclone-flat-") {
		return
	}
	c.flatNames[key] += delta
	if c.flatNames[key] == 0 {
		delete(c.flatNames, key)
	}
	if _, _, mapping, ok := parseFlatHashName(key.name); ok && mapping {
		return
	}
	full, directory, err := f.resolvedFlatName(key, c.flatPaths)
	if err != nil || (directory && item.Size != 0) {
		c.flatBad[key.parent] += delta
		return
	}
	if !directory {
		full, _ = dircache.SplitPath(full)
	}
	for full != "" {
		dirKey := mediaKey{parent: key.parent, name: full}
		c.flatDirs[dirKey] += delta
		if c.flatDirs[dirKey] == 0 {
			delete(c.flatDirs, dirKey)
		}
		full, _ = dircache.SplitPath(full)
	}
}

func (f *Fs) objectPath(ctx context.Context, remote string, create bool) (leaf, parent string, err error) {
	if !f.opt.FlatNamespace {
		return f.dirCache.FindPath(ctx, remote, create)
	}
	full, err := f.flatPath(remote)
	if err != nil {
		return "", "", err
	}
	leaf, err = flatName(full, false)
	if err != nil {
		return "", "", err
	}
	if err := f.ensureFlatNamespace(ctx); err != nil {
		return "", "", err
	}
	if create {
		var directory bool
		if c := f.metadata; c != nil {
			parent := f.opt.RootFolderID
			if parent == "0" {
				parent = ""
			}
			c.mu.Lock()
			bad := c.flatBad[parent] != 0
			directory = c.flatDirs[mediaKey{parent: parent, name: full}] != 0
			c.mu.Unlock()
			if bad {
				return "", "", errors.New("invalid or unsupported flat namespace metadata")
			}
		} else {
			tree, err := f.flatTree(ctx)
			if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
				return "", "", err
			}
			_, directory = tree[remote]
		}
		if directory {
			return "", "", fs.ErrorIsDir
		}
		parent, _ := dircache.SplitPath(full)
		if err := f.flatMkdir(ctx, parent); err != nil {
			return "", "", err
		}
		if err := f.ensureFlatMapping(ctx, full); err != nil {
			return "", "", err
		}
	}
	return leaf, f.opt.RootFolderID, nil
}

func (f *Fs) flatTree(ctx context.Context) (dirtree.DirTree, error) {
	if err := f.syncMetadata(ctx); err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pending, err := f.pendingUploads()
		if err != nil {
			return nil, err
		}
		var items []api.Media
		if err := f.media(ctx, nil, func(item api.Media) error {
			items = append(items, item)
			return ctx.Err()
		}); err != nil {
			return nil, err
		}
		tree, err := f.projectFlatTree(ctx, items, pending, false)
		if !errors.Is(err, errFlatMappingChanged) {
			return tree, err
		}
	}
}

func (f *Fs) projectFlatTree(ctx context.Context, items []api.Media, pending map[api.ID]*uploadRecord, historical bool) (dirtree.DirTree, error) {
	paths, err := f.flatMappings(ctx, items)
	if err != nil && !(historical && errors.Is(err, errFlatMappingChanged)) {
		return nil, err
	}
	tree := dirtree.New()
	files := make(map[string]api.ID)
	dirs := make(map[string]bool)
	rootFound := f.root == ""
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !sameParent(item.FolderID, f.opt.RootFolderID) || item.IsDeleted() || pending[item.ID] != nil {
			continue
		}
		name := f.opt.Enc.ToStandardName(item.Name)
		if isFlatMapping(name) {
			continue
		}
		if !strings.HasPrefix(name, "rclone-flat-") {
			continue
		}
		full, directory, err := f.resolvedFlatName(f.mediaKey(item), paths)
		if err != nil {
			return nil, fmt.Errorf("invalid or unsupported flat namespace name for media %s: %w", item.ID, err)
		}
		if directory && item.Size != 0 {
			return nil, fmt.Errorf("nonempty flat directory marker for media %s", item.ID)
		}
		remote := full
		if f.root != "" {
			if full == f.root && directory {
				rootFound = true
				continue
			}
			var found bool
			remote, found = strings.CutPrefix(full, f.root+"/")
			if !found {
				continue
			}
		}
		rootFound = true
		if directory {
			if !dirs[remote] {
				tree.AddDir(fs.NewDir(remote, time.Unix(0, 0)))
				dirs[remote] = true
			}
		} else {
			if _, found := files[remote]; found {
				return nil, fmt.Errorf("duplicate flat namespace file for media %s", item.ID)
			}
			files[remote] = item.ID
			tree.Add(&Object{fs: f, remote: remote, info: item})
		}
	}
	if !rootFound {
		return nil, fs.ErrorDirNotFound
	}
	// Build parents in one pass; AddEntry scans siblings for every added object.
	tree.CheckParents("")
	for remote, id := range files {
		if _, found := tree[remote]; found {
			return nil, fmt.Errorf("flat namespace file conflicts with a directory for media %s", id)
		}
	}
	if _, found := tree[""]; !found {
		tree[""] = nil
	}
	for _, entries := range tree {
		for i, entry := range entries {
			if _, ok := entry.(fs.Directory); ok {
				entries[i] = fs.NewDir(entry.Remote(), time.Unix(0, 0))
			}
		}
	}
	tree.Sort()
	return tree, nil
}

func (f *Fs) flatMkdir(ctx context.Context, full string) error {
	// Concurrent creators otherwise cause O2 to append a number to the marker name.
	f.flatDirMu.Lock()
	defer f.flatDirMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.ensureFlatNamespace(ctx); err != nil {
		return err
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return err
	}
	var missing []string
	for full != "" {
		file, err := flatName(full, false)
		if err != nil {
			return err
		}
		for _, record := range pending {
			if sameParent(record.FolderID, f.opt.RootFolderID) && record.Name == f.opt.Enc.FromStandardName(file) {
				return fs.ErrorIsFile
			}
		}
		if _, err := f.objectByName(ctx, full, file, f.opt.RootFolderID, nil); err == nil {
			return fs.ErrorIsFile
		} else if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		name, _ := flatName(full, true)
		marker, err := f.objectByName(ctx, full, name, f.opt.RootFolderID, nil)
		if errors.Is(err, fs.ErrorObjectNotFound) {
			missing = append(missing, full)
		} else if err != nil {
			return err
		} else if marker.Size() != 0 {
			return errors.New("nonempty flat directory marker")
		}
		full, _ = dircache.SplitPath(full)
	}
	// Check every ancestor before creating anything, then create parents first.
	for _, full := range slices.Backward(missing) {
		if err := f.ensureFlatMapping(ctx, full); err != nil {
			return err
		}
		o := &Object{fs: f, remote: full, flatDirectory: true}
		src := object.NewStaticObjectInfo(full, time.Now(), 0, true, nil, f).WithMimeType("application/octet-stream")
		if err := o.Update(ctx, strings.NewReader(""), src); err != nil {
			return fmt.Errorf("create flat directory marker: %w", err)
		}
	}
	return nil
}

func (f *Fs) flatRmdir(ctx context.Context, dir string) error {
	f.expireMetadata()
	entries, err := f.List(ctx, dir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	full, err := f.flatPath(dir)
	if err != nil {
		return err
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return err
	}
	for _, record := range pending {
		if !sameParent(record.FolderID, f.opt.RootFolderID) {
			continue
		}
		key := mediaKey{parent: string(record.FolderID), name: f.opt.Enc.ToStandardName(record.Name)}
		if key.parent == "0" {
			key.parent = ""
		}
		var remote string
		var decodeErr error
		if c := f.metadata; c != nil {
			c.mu.Lock()
			remote, _, decodeErr = f.resolvedFlatName(key, c.flatPaths)
			c.mu.Unlock()
		} else {
			f.flatMu.Lock()
			paths := make(map[mediaKey]string)
			for _, mapping := range f.flatRecords {
				if path, err := flatPayloadPath(mapping.flatPathPayload, f.opt.Enc.ToStandardName(mapping.Name)); err == nil {
					paths[mediaKey{parent: key.parent, name: f.opt.Enc.ToStandardName(mapping.Name)}] = path
				}
			}
			remote, _, decodeErr = f.resolvedFlatName(key, paths)
			f.flatMu.Unlock()
		}
		if decodeErr != nil {
			if strings.HasPrefix(record.Name, "rclone-flat-") {
				return errors.New("invalid flat namespace upload recovery name")
			}
			continue
		}
		if full == "" || remote == full || strings.HasPrefix(remote, full+"/") {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	if full == "" {
		return nil
	}
	name, _ := flatName(full, true)
	var markers []*Object
	err = f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, f.opt.RootFolderID) && f.mediaKey(item).name == name {
			markers = append(markers, &Object{fs: f, info: item})
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, marker := range markers {
		if err := marker.Remove(ctx); err != nil {
			return err
		}
	}
	return f.flushDeletions(ctx)
}

// List lists the files and directories in dir.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if err := f.flushDeletions(ctx); err != nil {
		return nil, err
	}
	if f.opt.FlatNamespace {
		tree, err := f.flatTree(ctx)
		if err != nil {
			return nil, err
		}
		entries, found := tree[dir]
		if !found {
			return nil, fs.ErrorDirNotFound
		}
		return entries, nil
	}
	if err := f.syncMetadata(ctx); err != nil {
		return nil, err
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return nil, err
	}
	parent, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	folders, err := f.folders(ctx)
	if err != nil {
		return nil, err
	}

	if parent != "" && !slices.ContainsFunc(folders, func(folder api.Folder) bool {
		return string(folder.ID) == parent && !folder.IsDeleted()
	}) {
		return nil, fs.ErrorDirNotFound
	}
	var entries fs.DirEntries
	for _, folder := range folders {
		if !sameParent(folder.ParentID, parent) || folder.IsDeleted() {
			continue
		}
		remote := path.Join(dir, f.opt.Enc.ToStandardName(folder.Name))
		f.dirCache.Put(remote, string(folder.ID))
		entries = append(entries, fs.NewDir(remote, time.UnixMilli(folder.Date)).SetID(string(folder.ID)))
	}
	err = f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, parent) && pending[item.ID] == nil {
			entries = append(entries, &Object{fs: f, remote: path.Join(dir, f.opt.Enc.ToStandardName(item.Name)), info: item})
		}
		return nil
	})
	return entries, err
}

func (f *Fs) folderPaths(ctx context.Context, folders []api.Folder, parent api.ID, dir string) (map[api.ID]string, error) {
	if parent == "0" {
		parent = ""
	}
	children := make(map[api.ID][]api.Folder, len(folders))
	for _, folder := range folders {
		if folder.IsDeleted() {
			continue
		}
		id := folder.ParentID
		if id == "0" {
			id = ""
		}
		children[id] = append(children[id], folder)
	}
	paths := map[api.ID]string{parent: dir}
	queue := []api.ID{parent}
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := queue[i]
		for _, folder := range children[id] {
			if _, found := paths[folder.ID]; found {
				return nil, fmt.Errorf("folder hierarchy repeats ID %s", folder.ID)
			}
			paths[folder.ID] = path.Join(paths[id], f.opt.Enc.ToStandardName(folder.Name))
			queue = append(queue, folder.ID)
		}
	}
	return paths, nil
}

// ListR lists files and directories recursively below dir.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	if err := f.flushDeletions(ctx); err != nil {
		return err
	}
	if f.opt.FlatNamespace {
		tree, err := f.flatTree(ctx)
		if err != nil {
			return err
		}
		if _, found := tree[dir]; !found {
			return fs.ErrorDirNotFound
		}
		helper := list.NewHelper(callback)
		for _, parent := range tree.Dirs() {
			if parent != dir && (dir != "" && !strings.HasPrefix(parent, dir+"/")) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			for _, entry := range tree[parent] {
				if err := helper.Add(entry); err != nil {
					return err
				}
			}
		}
		return helper.Flush()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.syncMetadata(ctx); err != nil {
		return err
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return err
	}
	parent, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	var folders []api.Folder
	var items []api.Media
	if c := f.metadata; c != nil {
		c.mu.Lock()
		folders = slices.Clone(c.state.Folders)
		items = slices.Collect(maps.Values(c.state.Media))
		c.mu.Unlock()
	} else {
		folders, err = f.folders(ctx)
		if err != nil {
			return err
		}
	}
	if parent != "" && parent != "0" && !slices.ContainsFunc(folders, func(folder api.Folder) bool {
		return string(folder.ID) == parent && !folder.IsDeleted()
	}) {
		return fs.ErrorDirNotFound
	}
	paths, err := f.folderPaths(ctx, folders, api.ID(parent), dir)
	if err != nil {
		return err
	}
	helper := list.NewHelper(callback)
	for _, folder := range folders {
		if err := ctx.Err(); err != nil {
			return err
		}
		remote, found := paths[folder.ID]
		if !found || string(folder.ID) == parent {
			continue
		}
		f.dirCache.Put(remote, string(folder.ID))
		if err := helper.Add(fs.NewDir(remote, time.UnixMilli(folder.Date)).SetID(string(folder.ID))); err != nil {
			return err
		}
	}
	visit := func(item api.Media) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pending[item.ID] != nil {
			return nil
		}
		parent := item.FolderID
		if parent == "0" {
			parent = ""
		}
		if remote, found := paths[parent]; found && !item.IsDeleted() {
			return helper.Add(&Object{fs: f, remote: path.Join(remote, f.opt.Enc.ToStandardName(item.Name)), info: item})
		}
		return nil
	}
	if f.metadata != nil {
		for _, item := range items {
			if err := visit(item); err != nil {
				return err
			}
		}
	} else if err := f.media(ctx, nil, visit); err != nil {
		return err
	}
	return helper.Flush()
}

type notificationKey struct {
	id   api.ID
	kind fs.EntryType
}

type notificationEntry struct {
	remote, version, status string
	size, modified, date    int64
}

func (f *Fs) notificationState(ctx context.Context) (map[notificationKey]notificationEntry, error) {
	c := f.metadata
	c.mu.Lock()
	folders := slices.Clone(c.state.Folders)
	items := slices.Collect(maps.Values(c.state.Media))
	c.mu.Unlock()
	return f.notificationEntries(ctx, folders, items)
}

func (f *Fs) notificationEntries(ctx context.Context, folders []api.Folder, items []api.Media) (map[notificationKey]notificationEntry, error) {
	pending, err := f.pendingUploads()
	if err != nil {
		return nil, err
	}
	if f.opt.FlatNamespace {
		tree, err := f.projectFlatTree(ctx, items, pending, true)
		if errors.Is(err, fs.ErrorDirNotFound) {
			return map[notificationKey]notificationEntry{}, nil
		}
		if err != nil {
			return nil, err
		}
		entries := make(map[notificationKey]notificationEntry)
		for _, children := range tree {
			for _, entry := range children {
				switch entry := entry.(type) {
				case *Object:
					item := entry.info
					entries[notificationKey{item.ID, fs.EntryObject}] = notificationEntry{remote: entry.remote, version: item.ETag, status: item.Status, size: item.Size, modified: item.Modified, date: item.Date}
				case fs.Directory:
					entries[notificationKey{api.ID("flat-dir:" + entry.Remote()), fs.EntryDirectory}] = notificationEntry{remote: entry.Remote()}
				}
			}
		}
		return entries, nil
	}
	paths, err := f.folderPaths(ctx, folders, api.ID(f.opt.RootFolderID), "")
	if err != nil {
		return nil, err
	}
	relative := func(remote string) (string, bool) {
		if f.root == "" {
			return remote, true
		}
		if remote == f.root {
			return "", true
		}
		return strings.CutPrefix(remote, f.root+"/")
	}
	entries := make(map[notificationKey]notificationEntry, len(folders)+len(items))
	for _, folder := range folders {
		remote, found := paths[folder.ID]
		if !found || folder.IsDeleted() {
			continue
		}
		if remote, found = relative(remote); found {
			entries[notificationKey{folder.ID, fs.EntryDirectory}] = notificationEntry{remote: remote, date: folder.Date, status: folder.Status}
		}
	}
	for _, item := range items {
		parent := item.FolderID
		if parent == "0" {
			parent = ""
		}
		remote, found := paths[parent]
		if !found || item.IsDeleted() || pending[item.ID] != nil {
			continue
		}
		if remote, found = relative(path.Join(remote, f.opt.Enc.ToStandardName(item.Name))); found {
			entries[notificationKey{item.ID, fs.EntryObject}] = notificationEntry{remote: remote, version: item.ETag, status: item.Status, size: item.Size, modified: item.Modified, date: item.Date}
		}
	}
	return entries, nil
}

// ChangeNotify polls committed metadata changes when metadata_cache is enabled.
func (f *Fs) ChangeNotify(ctx context.Context, notify func(string, fs.EntryType), intervals <-chan time.Duration) {
	c := f.metadata
	if c == nil || ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	initialFolders := slices.Clone(c.state.Folders)
	initialItems := slices.Collect(maps.Values(c.state.Media))
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		type result struct {
			entries map[notificationKey]notificationEntry
			err     error
		}
		results := make(chan result, 1)
		var baseline map[notificationKey]notificationEntry
		polling := false
		start := func(refresh bool) {
			polling = true
			go func() {
				var item result
				if refresh {
					item.err = f.updateMetadata(ctx, true)
					if item.err == nil {
						item.entries, item.err = f.notificationState(ctx)
					}
				} else {
					item.entries, item.err = f.notificationEntries(ctx, initialFolders, initialItems)
					initialFolders, initialItems = nil, nil
				}
				select {
				case results <- item:
				case <-ctx.Done():
				}
			}()
		}
		// Keep a separate baseline because foreground requests can refresh the cache.
		start(false)
		var ticker *time.Ticker
		var ticks <-chan time.Time
		defer func() {
			if ticker != nil {
				ticker.Stop()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case interval, open := <-intervals:
				if !open {
					return
				}
				if ticker != nil {
					ticker.Stop()
					ticks = nil
				}
				if interval > 0 {
					ticker = time.NewTicker(interval)
					ticks = ticker.C
				}
			case <-ticks:
				if !polling {
					start(true)
				}
			case item := <-results:
				polling = false
				if item.err != nil {
					if ctx.Err() == nil {
						fs.Errorf(f, "ChangeNotify: %v", item.err)
					}
					continue
				}
				type changedPath struct {
					remote string
					kind   fs.EntryType
				}
				changed := map[changedPath]struct{}{}
				if baseline != nil {
					for key, old := range baseline {
						if next, found := item.entries[key]; !found || old != next {
							changed[changedPath{old.remote, key.kind}] = struct{}{}
						}
					}
					for key, next := range item.entries {
						if old, found := baseline[key]; !found || old != next {
							changed[changedPath{next.remote, key.kind}] = struct{}{}
						}
					}
				}
				baseline = item.entries
				paths := slices.Collect(maps.Keys(changed))
				// Invalidate cached descendants before their old parent paths disappear.
				slices.SortFunc(paths, func(a, b changedPath) int { return len(b.remote) - len(a.remote) })
				for _, change := range paths {
					if ctx.Err() != nil {
						return
					}
					notify(change.remote, change.kind)
				}
			}
		}
	}()
}

// NewObject finds a file by its path, returning ErrorObjectNotFound if absent.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObject(ctx, remote, false)
}

func (f *Fs) newObject(ctx context.Context, remote string, includePending bool) (fs.Object, error) {
	if err := f.flushDeletions(ctx); err != nil {
		return nil, err
	}
	if err := f.syncMetadata(ctx); err != nil {
		return nil, err
	}
	var pending map[api.ID]*uploadRecord
	if !includePending {
		var err error
		pending, err = f.pendingUploads()
		if err != nil {
			return nil, err
		}
	}
	leaf, parent, err := f.objectPath(ctx, remote, false)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil, fs.ErrorObjectNotFound
	}

	if err != nil {
		return nil, err
	}
	return f.objectByName(ctx, remote, leaf, parent, pending)
}

func (f *Fs) objectByName(ctx context.Context, remote, leaf, parent string, pending map[api.ID]*uploadRecord) (*Object, error) {
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if parent == "0" {
			parent = ""
		}
		if f.opt.FlatNamespace && strings.HasPrefix(leaf, flatPrefix+"f-") && c.flatNames[mediaKey{parent: parent, name: leaf}] > 1 {
			return nil, errors.New("duplicate flat namespace file")
		}
		if parent != "" && !slices.ContainsFunc(c.state.Folders, func(folder api.Folder) bool {
			return string(folder.ID) == parent && !folder.IsDeleted()
		}) {
			return nil, fs.ErrorObjectNotFound
		}
		if id, ok := c.byName[mediaKey{parent: parent, name: leaf}]; ok && !c.state.Media[id].IsDeleted() && pending[id] == nil {
			return &Object{fs: f, remote: remote, info: c.state.Media[id]}, nil
		}
		return nil, fs.ErrorObjectNotFound
	}
	var found *Object
	err := f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, parent) && f.mediaKey(item).name == leaf && pending[item.ID] == nil {
			if f.opt.FlatNamespace && found != nil && strings.HasPrefix(leaf, flatPrefix+"f-") {
				return errors.New("duplicate flat namespace file")
			}
			found = &Object{fs: f, remote: remote, info: item}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if found == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return found, nil
}

// Mkdir creates dir and its missing parents.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	if err := f.flushDeletions(ctx); err != nil {
		return err
	}
	if err := f.syncMetadata(ctx); err != nil {
		return err
	}
	if f.opt.FlatNamespace {
		full, err := f.flatPath(dir)
		if err != nil {
			return err
		}
		return f.flatMkdir(ctx, full)
	}
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes an empty directory, returning ErrorDirectoryNotEmpty otherwise.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	if err := f.flushDeletions(ctx); err != nil {
		return err
	}
	if f.opt.FlatNamespace {
		return f.flatRmdir(ctx, dir)
	}
	// Cached emptiness must not authorize removal after another client adds files.
	f.expireMetadata()
	entries, err := f.List(ctx, dir)
	if err != nil {
		return err
	}

	if len(entries) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	id, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	pending, err := f.pendingUploads()
	if err != nil {
		return err
	}
	for _, record := range pending {
		if sameParent(record.FolderID, id) {
			return fs.ErrorDirectoryNotEmpty
		}
	}

	if id == "" || id == "0" {
		return nil
	}
	var anchor int64
	if c := f.metadata; c != nil {
		c.mu.Lock()
		anchor = c.state.Anchor
		c.mu.Unlock()
	}
	_, err = f.request(ctx, http.MethodPost, "/media/folder", "delete", nil, map[string]any{"folders": []api.ID{api.ID(id)}})
	if err != nil {
		f.expireMetadata()
		var apiErr *api.Error
		uncertain := fserrors.IsRetryError(err) || fserrors.ShouldRetry(err)
		if errors.As(err, &apiErr) {
			uncertain = apiErr.Code == "FOL-1000"
		}
		if uncertain {
			// An ambiguous response can follow removal of the folder on the server.
			checkCtx, cancel := context.WithTimeout(ctx, metadataTimeout)
			changes, _, checkErr := f.changes(checkCtx, anchor)
			cancel()
			change := changes["folder"]
			if checkErr == nil && slices.Contains(change.Deleted, api.ID(id)) && !slices.Contains(slices.Concat(change.New, change.Updated, change.Locked), api.ID(id)) {
				err = nil
			} else if checkErr != nil {
				fs.Debugf(f, "Could not confirm folder deletion using changes: %v", checkErr)
			}
		}
	}
	if err == nil {
		f.cacheFolder(api.Folder{ID: api.ID(id)}, true)
		f.dirCache.FlushDir(dir)
	}
	return err
}

// DirMove moves a directory on the same account, returning ErrorDirExists for an existing destination.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || f.opt.FlatNamespace || srcFs.opt.FlatNamespace || strings.TrimRight(f.opt.URL, "/") != strings.TrimRight(srcFs.opt.URL, "/") || f.opt.APIPath != srcFs.opt.APIPath {
		return fs.ErrorCantDirMove
	}
	if err := srcFs.flushDeletions(ctx); err != nil {
		return err
	}
	if srcFs != f {
		if err := f.flushDeletions(ctx); err != nil {
			return err
		}
	}
	if srcFs != f {
		srcAccount, err := srcFs.accountID(ctx)
		if err != nil {
			return err
		}
		dstAccount, err := f.accountID(ctx)
		if err != nil {
			return err
		}
		if srcAccount != dstAccount {
			return fs.ErrorCantDirMove
		}
	}
	remotes := []*Fs{f}
	if srcFs != f {
		remotes = append(remotes, srcFs)
	}
	for _, remote := range remotes {
		remote.expireMetadata()
		if err := remote.syncMetadata(ctx); err != nil {
			return err
		}
		remote.dirCache.ResetRoot()
	}
	srcID, err := srcFs.dirCache.FindDir(ctx, srcRemote, false)
	if err != nil {
		return err
	}
	if _, err := f.dirCache.FindDir(ctx, dstRemote, false); err == nil {
		return fs.ErrorDirExists
	} else if !errors.Is(err, fs.ErrorDirNotFound) {
		return err
	}
	// Check the closest existing ancestor before creating destination parents.
	ancestor := path.Dir(path.Join(f.root, dstRemote))
	if ancestor == "." {
		ancestor = ""
	}
	var parentID string
	dstRootCache := dircache.New("", f.opt.RootFolderID, f)
	for {
		parentID, err = dstRootCache.FindDir(ctx, ancestor, false)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrorDirNotFound) || ancestor == "" {
			return err
		}
		ancestor = path.Dir(ancestor)
		if ancestor == "." {
			ancestor = ""
		}
	}
	folders, err := f.folders(ctx)
	if err != nil {
		return err
	}
	parents := map[api.ID]api.ID{}
	for _, folder := range folders {
		parents[folder.ID] = folder.ParentID
	}
	seen := map[api.ID]bool{}
	for id := api.ID(parentID); id != "" && id != "0"; id = parents[id] {
		if string(id) == srcID {
			return errors.New("cannot move a directory into itself")
		}
		if seen[id] {
			return errors.New("folder ancestry contains a cycle")
		}
		seen[id] = true
		if _, ok := parents[id]; !ok {
			return fs.ErrorCantDirMove
		}
	}
	srcID, _, _, dstParent, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	folder := api.Folder{ID: api.ID(srcID), ParentID: api.ID(dstParent), Name: f.opt.Enc.FromStandardName(dstLeaf)}
	reply, err := f.request(ctx, http.MethodPost, "/media/folder", "save", nil, folder)
	if err == nil && reply.ID != "" && reply.ID != folder.ID {
		err = errors.New("folder move returned an unexpected ID")
	}
	for _, remote := range remotes {
		if err != nil {
			remote.expireMetadata()
		} else {
			remote.cacheFolder(folder, false)
		}
		remote.dirCache.ResetRoot()
	}
	return err
}

// Put creates or replaces an object and preserves its upload modification time.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, opts ...fs.OpenOption) (fs.Object, error) {
	if err := checkResumeUploadOptions(ctx, f.opt); err != nil {
		return nil, err
	}
	existing, err := f.newObject(ctx, src.Remote(), true)
	if err == nil {
		return existing, existing.Update(ctx, in, src, opts...)
	}

	if !errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, err
	}
	o := &Object{fs: f, remote: src.Remote()}
	err = o.Update(ctx, in, src, opts...)
	return o, err
}

// About returns the user's storage quota.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	if err := f.flushDeletions(ctx); err != nil {
		return nil, err
	}
	reply, err := f.request(ctx, http.MethodGet, "/media", "get-storage-space", nil, nil)
	if err != nil {
		return nil, err
	}
	var data struct {
		Quota int64 `json:"quota"`
		Free  int64 `json:"free"`
	}

	if err = json.Unmarshal(reply.Data, &data); err != nil {
		return nil, err
	}
	return &fs.Usage{Total: fs.NewUsageValue(data.Quota), Free: fs.NewUsageValue(data.Free), Used: fs.NewUsageValue(data.Quota - data.Free)}, nil
}

// Fs returns the object's filesystem.
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the object's path.
func (o *Object) Remote() string { return o.remote }

// String returns the object's path.
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Size returns the content length.
func (o *Object) Size() int64 { return o.info.Size }

// ModTime returns the modification time, falling back to the server date.
func (o *Object) ModTime(context.Context) time.Time {
	if o.info.Modified != 0 {
		return time.UnixMilli(o.info.Modified)
	}
	return time.UnixMilli(o.info.Date)
}

// Storable reports whether this object can be stored.
func (o *Object) Storable() bool { return true }

// Hash returns ErrUnsupported because SAPI does not specify content hashes.
func (o *Object) Hash(context.Context, hash.Type) (string, error) { return "", hash.ErrUnsupported }

// SetModTime returns ErrorCantSetModTime; timestamps can only be set on upload.
func (o *Object) SetModTime(context.Context, time.Time) error { return fs.ErrorCantSetModTime }

// ID returns the SAPI media identifier.
func (o *Object) ID() string { return string(o.info.ID) }

func (o *Object) refresh(ctx context.Context) error {
	found := false
	err := o.fs.media(ctx, []api.ID{o.info.ID}, func(item api.Media) error {
		if item.ID == o.info.ID {
			o.info = item
			found = true
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !found {
		return fs.ErrorObjectNotFound
	}
	return nil
}

func (o *Object) waitMetadata(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	delay := metadataDelay
	for {
		err := o.refresh(ctx)
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			if err == nil {
				o.fs.cacheMedia(o.info, false)
			}
			return err
		}

		// The upload server may finish before SAPI can see the new media.
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for uploaded media: %w", ctx.Err())
		case <-time.After(delay):
		}
		delay = min(2*delay, metadataMaxDelay)
	}
}

// Open downloads original content and supports range requests.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Range downloads may open the same object concurrently.
	snapshot := *o
	o = &snapshot
	if err := o.fs.flushDeletions(ctx); err != nil {
		return nil, err
	}
	if o.fs.opt.AsyncDelete {
		existing := *o
		if err := existing.refresh(ctx); err != nil {
			return nil, err
		}
	}
	// Some providers have no downloadable blob for a stored empty file.
	if o.Size() == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	key := o.downloadKey()
	rawURL := o.info.URL
	if cached, ok := o.fs.downloadURLs.GetMaybe(key); ok {
		rawURL = cached.(string)
	}
	var err error
	if rawURL == "" {
		rawURL, err = o.refreshDownloadURL(ctx, key, "")
		if err != nil {
			return nil, err
		}
	}
	options = slices.Clone(options)
	fs.FixRangeOption(options, o.Size())
	base, _ := url.Parse(o.fs.opt.URL + "/")
	for attempt := range 2 {
		u, err := rest.URLJoin(base, rawURL)
		if err != nil || rawURL == "" {
			return nil, errors.New("invalid media download URL")
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, errors.New("unsupported download URL scheme")
		}
		body, err := o.fs.open(ctx, u, options)
		if attempt != 0 || !errors.Is(err, errDownloadURLExpired) {
			return body, err
		}
		rawURL, err = o.refreshDownloadURL(ctx, key, rawURL)
		if err != nil {
			return nil, err
		}
	}
	return nil, errDownloadURLExpired
}

var errDownloadURLExpired = errors.New("expired media download URL")

func sameMediaContent(a, b api.Media) bool {
	return a.ETag != "" && a.ETag == b.ETag && a.Size == b.Size
}

func (o *Object) downloadKey() string {
	if o.info.ETag != "" {
		return fmt.Sprintf("%s/etag/%q/%d", o.info.ID, o.info.ETag, o.info.Size)
	}
	return fmt.Sprintf("%s/%d/%d/%d", o.info.ID, o.info.Size, o.info.Modified, o.info.Date)
}

func (o *Object) refreshDownloadURL(ctx context.Context, key, expired string) (string, error) {
	result := o.fs.downloadRefresh.DoChan(key, func() (any, error) {
		if cached, ok := o.fs.downloadURLs.GetMaybe(key); ok && cached.(string) != expired {
			return cached, nil
		}
		lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metadataTimeout)
		defer cancel()
		snapshot := *o
		if err := snapshot.refresh(lookupCtx); err != nil {
			return nil, err
		}
		if o.info.ETag != "" && !sameMediaContent(o.info, snapshot.info) || o.info.Size != snapshot.info.Size || o.info.ETag == "" && o.info.Modified != 0 && snapshot.info.Modified != o.info.Modified {
			return nil, errors.New("media changed while opening download")
		}
		if snapshot.info.URL == "" {
			return nil, errors.New("media has no download URL")
		}
		o.fs.downloadURLs.Put(key, snapshot.info.URL)
		return snapshot.info.URL, nil
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case refreshed := <-result:
		if refreshed.Err != nil {
			return "", refreshed.Err
		}
		return refreshed.Val.(string), nil
	}
}

func downloadError(resp *http.Response) error {
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if (contentType != "application/json" && contentType != "text/javascript") || resp.Header.Get("Content-Disposition") != "" {
		return nil
	}

	// SAPI errors use HTTP 200. Preserve JSON file contents and bound buffering.
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorSize+1))
	var reply api.Response
	if err == nil && len(b) <= maxErrorSize && json.Unmarshal(b, &reply) == nil && reply.Error != nil && reply.Error.Code != "" {
		err = reply.Error
	}

	if err != nil {
		_ = resp.Body.Close()
		return err
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(b), resp.Body), resp.Body}
	return nil
}

func (f *Fs) open(ctx context.Context, u *url.URL, options []fs.OpenOption) (io.ReadCloser, error) {
	apiURL, _ := url.Parse(strings.TrimRight(f.opt.URL, "/") + "/" + strings.Trim(f.opt.APIPath, "/") + "/")
	isAPI := func(u *url.URL) bool {
		return u.Scheme == apiURL.Scheme && u.Host == apiURL.Host && strings.HasPrefix(u.Path, apiURL.Path)
	}
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		for attempt := 0; attempt < maxAuthAttempts; attempt++ {
			opts := rest.Opts{Method: http.MethodGet, RootURL: u.String(), Options: options}
			var state authState
			if isAPI(u) {
				var err error
				state, err = f.auth.prepare(ctx)
				if err != nil {
					return false, err
				}
				opts.ExtraHeaders = map[string]string{
					"Cookie":     (&http.Cookie{Name: sessionCookie, Value: state.session.ID}).String(),
					deviceHeader: f.opt.DeviceID,
				}
				if state.header != "" {
					opts.ExtraHeaders["Authorization"] = state.header
				}
				target := *u
				q := target.Query()
				if state.session.Key != "" {
					q.Set("validationkey", state.session.Key)
				}
				target.RawQuery = q.Encode()
				opts.RootURL = target.String()
			}
			opts.CheckRedirect = func(req *http.Request, via []*http.Request) error {
				if len(via) >= maxRedirects {
					return errors.New("too many download redirects")
				}
				// Process refresh headers before following a redirect to a media server.
				if previous := req.Response; previous != nil && isAPI(previous.Request.URL) {
					if err := f.auth.saveHeader(previous, state.accessToken); err != nil {
						return err
					}
				}
				if !isAPI(req.URL) {
					req.Header.Del("Authorization")
					req.Header.Del("Cookie")
					req.Header.Del("Referer")
					req.Header.Del(deviceHeader)
				}
				return nil
			}
			var err error
			resp, err = f.download.Call(ctx, &opts)
			if resp != nil && resp.Request != nil && isAPI(resp.Request.URL) {
				if saveErr := f.auth.saveHeader(resp, state.accessToken); saveErr != nil {
					if err == nil {
						_ = resp.Body.Close()
					}
					return false, saveErr
				}
				if err == nil {
					err = downloadError(resp)
				}
				var apiErr *api.Error
				if resp.StatusCode == http.StatusUnauthorized || (errors.As(err, &apiErr) && apiErr.Code == invalidKeyCode) {
					f.auth.invalidate(state.session)
					if attempt == 0 {
						continue
					}
				}
			}
			return retry(ctx, resp, err)
		}
		return false, errors.New("download session renewal failed")
	})
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusForbidden && resp.Request != nil && !isAPI(resp.Request.URL) {
			return nil, fmt.Errorf("%w: %w", errDownloadURLExpired, err)
		}
		return nil, err
	}
	return resp.Body, nil
}

// Update replaces content and waits for the uploaded object to be available.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	if err := o.fs.flushDeletions(ctx); err != nil {
		return err
	}
	if o.fs.opt.AsyncDelete && o.info.ID != "" {
		if err := o.refresh(ctx); err != nil {
			return err
		}
	}
	if err := checkResumeUploadOptions(ctx, o.fs.opt); err != nil {
		return err
	}
	o.fs.downloadURLs.DeletePrefix(string(o.info.ID) + "/")
	defer func() {
		if err != nil {
			o.fs.expireMetadata()
		}
	}()
	if src.Size() < 0 {
		return errors.New("OneMediaHub uploads require a known size")
	}
	if err := o.fs.syncMetadata(ctx); err != nil {
		return err
	}
	var leaf, parent string
	if o.flatMapping {
		leaf, parent = flatMappingName(o.remote), o.fs.opt.RootFolderID
	} else if o.flatDirectory {
		leaf, err = flatName(o.remote, true)
		parent = o.fs.opt.RootFolderID
	} else {
		leaf, parent, err = o.fs.objectPath(ctx, o.remote, true)
	}
	if err != nil {
		return err
	}
	modified := src.ModTime(ctx).UTC().Format(dateFormat)
	data := api.Upload{ID: string(o.info.ID), FolderID: api.ID(parent), Name: o.fs.opt.Enc.FromStandardName(leaf), Size: src.Size(), ContentType: fs.MimeType(ctx, src), Created: modified, Modified: modified}
	if o.fs.opt.FlatNamespace {
		defer func() {
			nameMatches := o.info.Name == data.Name || o.flatDirectory && o.info.Size == 0 && o.fs.mediaKey(o.info).name == leaf
			if err == nil && (!nameMatches || !sameParent(o.info.FolderID, string(data.FolderID)) || o.info.Size != data.Size) {
				err = errors.New("uploaded flat namespace media does not match its intended name, parent or size")
			}
		}()
	}
	if o.fs.opt.AsyncUpload {
		return o.uploadAsync(ctx, in, data, options, src)
	}
	endpoint := "/upload"
	if o.info.ID != "" {
		endpoint = "/upload/file"
	}
	size := src.Size()
	opts := rest.Opts{Method: http.MethodPost, RootURL: o.fs.uploadURL, Path: endpoint, Parameters: url.Values{"action": {"save"}}, Body: in, ContentLength: &size, Options: options, MultipartMetadataName: "data", MultipartContentName: "file", MultipartFileName: data.Name, MultipartContentType: data.ContentType}
	reply, err := o.fs.call(ctx, opts, map[string]any{"data": data})
	if err != nil {
		return err
	}

	if reply.ID == "" {
		return errors.New("upload returned no media ID")
	}
	o.info.ID = reply.ID
	return o.waitMetadata(ctx)
}

type uploadRecord struct {
	Version    int       // Version identifies the journal schema.
	ID         api.ID    // ID identifies the unfinished media item.
	FolderID   api.ID    // FolderID identifies the destination parent.
	Name       string    // Name is the encoded destination filename.
	Size       int64     // Size is the expected content length.
	Modified   time.Time // Modified is the source modification time.
	Source     string    // Source identifies the source configuration, root, and path.
	HashType   string    // HashType identifies the source checksum algorithm.
	Hash       string    // Hash verifies the source content against the upload input.
	Processing bool      // Processing indicates all bytes are accepted, awaiting validation and metadata.
}

type uploadJournalOp struct {
	key    string
	record *uploadRecord
	write  bool
	remove bool
	items  map[api.ID]*uploadRecord
}

func (op *uploadJournalOp) Do(_ context.Context, bucket kv.Bucket) error {
	if op.items != nil {
		return bucket.ForEach(func(_, value []byte) error {
			var record *uploadRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			if record == nil || record.Version != uploadJournalVersion || record.ID == "" {
				return errors.New("invalid upload recovery record")
			}
			if _, found := op.items[record.ID]; found {
				return errors.New("duplicate upload recovery ID")
			}
			op.items[record.ID] = record
			return nil
		})
	}
	key := []byte(op.key)
	if op.remove {
		return bucket.Delete(key)
	}
	if op.write {
		b, err := json.Marshal(op.record)
		if err != nil {
			return err
		}
		return bucket.Put(key, b)
	}
	if b := bucket.Get(key); b != nil {
		if err := json.Unmarshal(b, &op.record); err != nil {
			return err
		}
		if op.record == nil {
			return errors.New("invalid upload recovery record")
		}
	}
	return nil
}

func (f *Fs) pendingUploads() (map[api.ID]*uploadRecord, error) {
	f.uploadJournalMu.RLock()
	defer f.uploadJournalMu.RUnlock()
	if f.uploadJournal == nil {
		if f.opt.ResumeUploads {
			return nil, kv.ErrInactive
		}
		return nil, nil
	}
	op := &uploadJournalOp{items: make(map[api.ID]*uploadRecord)}
	if err := f.uploadJournal.Do(false, op); err != nil && !errors.Is(err, kv.ErrEmpty) {
		return nil, fmt.Errorf("read unfinished uploads: %w", err)
	}
	return op.items, nil
}

func (f *Fs) journalDB() *kv.DB {
	f.uploadJournalMu.RLock()
	defer f.uploadJournalMu.RUnlock()
	return f.uploadJournal
}

func (f *Fs) journalDo(write bool, op kv.Op) error {
	f.uploadJournalMu.RLock()
	defer f.uploadJournalMu.RUnlock()
	if f.uploadJournal == nil {
		return kv.ErrInactive
	}
	return f.uploadJournal.Do(write, op)
}

func (f *Fs) stopUploadJournal() error {
	f.uploadJournalMu.Lock()
	defer f.uploadJournalMu.Unlock()
	if f.uploadJournal == nil {
		return nil
	}
	err := f.uploadJournal.Stop(false)
	f.uploadJournal = nil
	return err
}

func (f *Fs) startUploadJournal(ctx context.Context) error {
	if !kv.Supported() {
		return kv.ErrUnsupported
	}
	id, err := f.accountID(ctx)
	if err != nil {
		return err
	}
	scope, _ := json.Marshal([]string{strings.TrimRight(f.opt.URL, "/"), strings.Trim(f.opt.APIPath, "/"), f.uploadURL, id})
	digest := sha256.Sum256(scope)
	// Remote aliases and roots share upload ownership for the same account.
	f.uploadJournalMu.Lock()
	defer f.uploadJournalMu.Unlock()
	f.uploadJournal, err = kv.Start(ctx, fmt.Sprintf("onemediahub-uploads-%x", digest[:]), nil)
	return err
}

type uploadRecovery struct {
	fs       *Fs
	db       *kv.DB
	key      string
	lock     *flock.Flock
	record   *uploadRecord
	previous *uploadRecord
	source   fs.Object
	reader   io.Reader
	existing bool
}

func uploadSource(src fs.ObjectInfo) fs.Object {
	// OverrideRemote changes the destination name without changing source bytes.
	for {
		override, ok := src.(*fs.OverrideRemote)
		if !ok {
			break
		}
		src = override.ObjectInfo
	}
	source, _ := src.(fs.Object)
	return source
}

func freshUploadSource(ctx context.Context, source fs.Object, size int64, modified time.Time) (fs.Object, error) {
	sourceFs, ok := source.Fs().(fs.Fs)
	if !ok {
		return nil, nil
	}
	fresh, err := sourceFs.NewObject(ctx, source.Remote())
	if err != nil {
		return nil, fmt.Errorf("check upload source: %w", err)
	}
	if fresh.Size() != size || !fresh.ModTime(ctx).Equal(modified) {
		return nil, errors.New("upload source changed before resuming")
	}
	return fresh, nil
}

func bindUploadReader(ctx context.Context, in io.Reader, size int64, hashType hash.Type, expected string) (io.Reader, string, error) {
	reader, wrap := accounting.UnWrap(in)
	seeker, seekable := reader.(io.ReadSeeker)
	_, stable := reader.(*os.File)
	var buffered *accounting.Account
	var baseOffset int64
	if seekable {
		var err error
		baseOffset, err = seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return in, "", nil
		}
	} else {
		_, acc := accounting.UnWrapAccounting(in)
		if acc == nil {
			acc, _ = in.(*accounting.Account)
		}
		if acc == nil || acc.GetAsyncReader() == nil || reader != acc.GetAsyncReader() {
			return in, "", nil
		}
		var ok bool
		seeker, ok = acc.GetReader().(io.ReadSeeker)
		if !ok {
			return in, "", nil
		}
		if file, ok := seeker.(*os.File); ok {
			info, err := file.Stat()
			if err != nil {
				return nil, "", err
			}
			if info.Size() != size {
				return in, "", nil
			}
			stable = true
		}
		buffered = acc
	}
	hasher, err := hash.NewMultiHasherTypes(hash.NewHashSet(hashType))
	if err != nil {
		return nil, "", err
	}
	n, readErr := io.Copy(hasher, readers.NewContextReader(ctx, reader))
	if buffered != nil {
		buffered.Abandon()
	}
	if readErr != nil {
		return nil, "", fmt.Errorf("hash upload input: %w", readErr)
	}
	if n != size {
		return nil, "", errors.New("upload input size does not match its source")
	}
	sum, err := hasher.SumString(hashType, false)
	if err != nil {
		return nil, "", err
	}
	// Some seekers reopen the source path; a mismatch must fail before rewinding.
	if !stable && sum != expected {
		return nil, "", errors.New("upload input content differs from its source")
	}
	if buffered != nil && !stable {
		position, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, "", err
		}
		if position != size {
			return nil, "", errors.New("buffered upload input did not start at zero")
		}
	}
	position, seekErr := seeker.Seek(baseOffset, io.SeekStart)
	if seekErr != nil {
		return nil, "", fmt.Errorf("rewind upload input: %w", seekErr)
	}
	if position != baseOffset {
		return nil, "", errors.New("upload input returned an unexpected rewind position")
	}
	if buffered != nil {
		buffered.SetStream(seeker)
	}
	return wrap(seeker), sum, nil
}

func (o *Object) prepareUploadRecovery(ctx context.Context, data api.Upload, src fs.ObjectInfo, in io.Reader) (_ *uploadRecovery, err error) {
	db := o.fs.journalDB()
	if db == nil {
		if o.fs.opt.ResumeUploads {
			return nil, kv.ErrInactive
		}
		return nil, nil
	}
	parent := string(data.FolderID)
	if parent == "0" {
		parent = ""
	}
	destination, _ := json.Marshal([]string{parent, data.Name})
	digest := sha256.Sum256(destination)
	r := &uploadRecovery{fs: o.fs, db: db, key: hex.EncodeToString(digest[:]), reader: in}
	r.lock = flock.New(r.db.Path() + "." + r.key + ".lock")
	locked, err := r.lock.TryLock()
	contended := err == nil && !locked
	if contended {
		locked, err = r.lock.TryLockContext(ctx, metadataDelay)
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload recovery: %w", err)
	}
	if !locked {
		return nil, errors.New("could not lock upload recovery")
	}
	defer func() {
		if err != nil {
			_ = r.lock.Unlock()
		}
	}()
	if data.ID != "" {
		pending, err := o.fs.pendingUploads()
		if err != nil {
			return nil, err
		}
		if record := pending[api.ID(data.ID)]; record != nil && (record.Name != data.Name || !sameParent(record.FolderID, string(data.FolderID))) {
			return nil, errors.New("upload destination belongs to an unfinished upload at another path")
		}
	}
	if contended && !o.flatMapping && !o.flatDirectory {
		if err := o.fs.updateMetadata(ctx, true); err != nil {
			return nil, err
		}
		current, err := o.fs.newObject(ctx, o.remote, true)
		if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, err
		}
		if errors.Is(err, fs.ErrorObjectNotFound) && data.ID != "" {
			return nil, fmt.Errorf("upload destination changed while waiting for recovery lock: %w", fs.ErrorObjectNotFound)
		}
		if err == nil && current.(*Object).ID() != data.ID {
			return nil, errors.New("upload destination changed while waiting for recovery lock")
		}
	}
	if source := uploadSource(src); source != nil && data.Size > 0 {
		r.source, err = freshUploadSource(ctx, source, data.Size, src.ModTime(ctx))
		if err != nil {
			return nil, err
		}
	}
	if r.source != nil {
		for _, hashType := range []hash.Type{hash.SHA256, hash.SHA512, hash.BLAKE3, hash.MD5, hash.SHA1, hash.Whirlpool} {
			if !r.source.Fs().Hashes().Contains(hashType) {
				continue
			}
			sum, hashErr := r.source.Hash(ctx, hashType)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if _, decodeErr := hex.DecodeString(sum); hashErr != nil || decodeErr != nil || len(sum) != hash.Width(hashType, false) {
				continue
			}
			var actual string
			r.reader, actual, err = bindUploadReader(ctx, r.reader, data.Size, hashType, sum)
			if err != nil {
				return nil, err
			}
			if actual == "" || actual != sum {
				break
			}
			identity, _ := json.Marshal([]string{r.source.Fs().Name(), r.source.Fs().Root(), r.source.Remote()})
			r.record = &uploadRecord{Version: uploadJournalVersion, FolderID: data.FolderID, Name: data.Name,
				Size: data.Size, Modified: src.ModTime(ctx), Source: string(identity), HashType: hashType.String(), Hash: sum}
			break
		}
	}
	op := &uploadJournalOp{key: r.key}
	if err := r.fs.journalDo(false, op); err != nil && !errors.Is(err, kv.ErrEmpty) {
		return nil, fmt.Errorf("read upload recovery: %w", err)
	}
	if old := op.record; old != nil {
		if old.Version != uploadJournalVersion || old.ID == "" {
			return nil, errors.New("invalid upload recovery record")
		}
		if next := r.record; next != nil && sameParent(old.FolderID, string(next.FolderID)) && old.Name == next.Name && old.Size == next.Size &&
			old.Modified.Equal(next.Modified) && old.Source == next.Source && old.HashType == next.HashType && old.Hash == next.Hash &&
			(data.ID == "" || data.ID == string(old.ID)) {
			r.record, r.existing = old, true
		} else {
			r.previous = old
		}
	}
	if r.record == nil {
		fs.Debugf(o, "Upload input cannot be matched to a rewindable source hash; using recovery within this attempt")
	}
	return r, nil
}

func (r *uploadRecovery) save() error {
	if r == nil || r.record == nil {
		return nil
	}
	if err := r.fs.journalDo(true, &uploadJournalOp{key: r.key, record: r.record, write: true}); err != nil {
		return fmt.Errorf("save upload recovery: %w", err)
	}
	return nil
}

func (o *Object) completeUploadRecovery(r *uploadRecovery, data api.Upload) error {
	if r == nil || r.record == nil {
		return nil
	}
	if o.info.ID != r.record.ID || o.info.Size != data.Size || o.info.Name != data.Name ||
		!sameParent(o.info.FolderID, string(data.FolderID)) || o.info.IsDeleted() {
		return errors.New("validated upload metadata does not match the recovery record")
	}
	if err := r.fs.journalDo(true, &uploadJournalOp{key: r.key, remove: true}); err != nil {
		return fmt.Errorf("clear completed upload recovery: %w", err)
	}
	return nil
}

func (o *Object) uploadAsync(ctx context.Context, in io.Reader, data api.Upload, options []fs.OpenOption, src fs.ObjectInfo) error {
	originalModTime := src.ModTime(ctx)
	recovery, err := o.prepareUploadRecovery(ctx, data, src, in)
	if err != nil {
		return err
	}
	if recovery != nil {
		defer func() { _ = recovery.lock.Unlock() }()
		in = recovery.reader
	}
	var reply api.Response
	if o.fs.opt.ResumeUploads {
		id := api.ID(data.ID)
		allowInvisible := recovery != nil && recovery.existing
		if allowInvisible {
			id = recovery.record.ID
		} else if id == "" && recovery != nil && recovery.previous != nil {
			id = recovery.previous.ID
		}
		if id != "" {
			found := false
			if err := o.fs.fetchMedia(ctx, []api.ID{id}, true, func(item api.Media) error {
				if item.ID != id {
					return errors.New("upload destination lookup returned an unexpected media ID")
				}
				found = true
				if item.IsDeleted() {
					return errors.New("upload recovery item is deleted or in trash")
				}
				if item.Name != data.Name || !sameParent(item.FolderID, string(data.FolderID)) || allowInvisible && item.Size != data.Size {
					return errors.New("upload recovery destination changed")
				}
				if !allowInvisible && data.ID == "" && recovery != nil && recovery.previous != nil {
					data.ID, o.info.ID = string(item.ID), item.ID
				}
				return nil
			}); err != nil {
				return err
			}
			if !found && !allowInvisible && data.ID != "" {
				return fmt.Errorf("upload replacement destination: %w", fs.ErrorObjectNotFound)
			}
		}
	}
	if recovery != nil && recovery.existing {
		o.info.ID = recovery.record.ID
	} else {
		metadata, err := json.Marshal(map[string]any{"data": data})
		if err != nil {
			return err
		}
		opts := rest.Opts{Method: http.MethodPost, RootURL: o.fs.uploadURL, Path: "/upload/file", Parameters: url.Values{"action": {"save-metadata"}, "responsetime": {"true"}, "lastupdate": {"true"}}, ContentType: "application/octet-stream", Body: bytes.NewReader(metadata)}
		reply, err = o.fs.call(ctx, opts, nil)
		if err != nil {
			return fmt.Errorf("register upload metadata: %w", err)
		}
		if reply.ID == "" {
			return errors.New("upload metadata returned no media ID")
		}
		if o.info.ID != "" && reply.ID != o.info.ID {
			return fmt.Errorf("upload metadata returned media ID %s, expected %s", reply.ID, o.info.ID)
		}
		o.info.ID = reply.ID
		if recovery != nil && recovery.record != nil {
			recovery.record.ID = reply.ID
			if err := recovery.save(); err != nil {
				return err
			}
		} else if recovery != nil && recovery.previous != nil {
			if err := recovery.fs.journalDo(true, &uploadJournalOp{key: recovery.key, remove: true}); err != nil {
				return fmt.Errorf("discard stale upload recovery: %w", err)
			}
		}
	}
	size := data.Size
	if size == 0 {
		// net/http treats an arbitrary reader with zero ContentLength as an unknown length.
		in = http.NoBody
	}
	opts := rest.Opts{Method: http.MethodPost, RootURL: o.fs.uploadURL, Path: "/upload/file",
		Parameters: url.Values{"action": {"save"}, "lastupdate": {"true"}, "acceptasynchronous": {"true"}},
		Body:       in, ContentType: data.ContentType, ContentLength: &size, Options: options,
		ExtraHeaders: map[string]string{"X-funambol-id": string(o.info.ID), "X-funambol-file-size": strconv.FormatInt(size, 10)}}
	unwrapped, wrap := accounting.UnWrap(in)
	seeker, canSeek := unwrapped.(io.ReadSeeker)
	var baseOffset int64
	if canSeek {
		baseOffset, err = seeker.Seek(0, io.SeekCurrent)
		canSeek = err == nil
	}
	var reopened io.ReadCloser
	defer func() {
		if reopened != nil {
			_ = reopened.Close()
		}
	}()
	var offset, available int64
	processing := recovery != nil && recovery.existing && recovery.record.Processing
	if recovery != nil && recovery.existing && !processing {
		// Replacement items may retain the old validated content while bytes are pending.
		offset, err = o.uploadOffset(ctx, size)
		if err != nil {
			return fmt.Errorf("recover upload media %s: %w", o.info.ID, err)
		}
		available = offset
		processing = offset == size
		if processing {
			recovery.record.Processing = true
			if err := recovery.save(); err != nil {
				return err
			}
		} else {
			if !canSeek {
				return errors.New("recovered upload input cannot be sought")
			}
			position, err := seeker.Seek(baseOffset+offset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("seek recovered upload input: %w", err)
			}
			if position != baseOffset+offset {
				return errors.New("recovered upload input returned an unexpected seek position")
			}
			opts.Body = wrap(seeker)
			remaining := size - offset
			opts.ContentLength = &remaining
			opts.ContentRange = fmt.Sprintf("bytes %d-%d/%d", offset, size-1, size)
		}
	}
	for attempt := 0; !processing; attempt++ {
		counter := readers.NewCountingReader(opts.Body)
		if size != 0 {
			opts.Body = counter
		}
		reply, err = o.fs.call(ctx, opts, nil)
		available = max(available, offset+int64(counter.BytesRead()))
		if err == nil {
			if reply.ID != "" && reply.ID != o.info.ID {
				return fmt.Errorf("upload returned media ID %s, expected %s", reply.ID, o.info.ID)
			}
			if recovery != nil && recovery.record != nil {
				recovery.record.Processing = true
				if err := recovery.save(); err != nil {
					return err
				}
			}
			break
		}
		if ctx.Err() != nil || attempt+1 >= max(1, fs.GetConfig(ctx).LowLevelRetries) || !fserrors.IsRetryError(err) && !fserrors.ShouldRetry(err) {
			return fmt.Errorf("upload media %s: %w", o.info.ID, err)
		}
		confirmed, probeErr := o.uploadOffset(ctx, size)
		if probeErr != nil {
			return fmt.Errorf("resume upload media %s after %v: %w", o.info.ID, err, probeErr)
		}
		if confirmed > available {
			return errors.New("upload offset exceeds bytes sent")
		}
		offset = confirmed
		if source := uploadSource(src); source != nil {
			fresh, err := freshUploadSource(ctx, source, data.Size, originalModTime)
			if err != nil {
				return err
			}
			if fresh != nil && recovery != nil && recovery.record != nil {
				var hashType hash.Type
				if err := hashType.Set(recovery.record.HashType); err != nil {
					return err
				}
				sum, err := fresh.Hash(ctx, hashType)
				if err != nil || sum != recovery.record.Hash {
					return errors.New("upload source content changed before resuming")
				}
			}
		}
		if offset == size {
			if recovery != nil && recovery.record != nil {
				recovery.record.Processing = true
				if err := recovery.save(); err != nil {
					return err
				}
			}
			break
		}
		if canSeek {
			position, err := seeker.Seek(baseOffset+offset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("seek upload source: %w", err)
			}
			if position != baseOffset+offset {
				return errors.New("upload source returned an unexpected seek position")
			}
			opts.Body = wrap(seeker)
		} else if source, ok := src.(fs.Object); ok {
			if reopened != nil {
				_ = reopened.Close()
			}
			reopened, err = source.Open(ctx, &fs.SeekOption{Offset: offset})
			if err != nil {
				return fmt.Errorf("reopen upload source: %w", err)
			}
			opts.Body = wrap(reopened)
		} else {
			return fmt.Errorf("cannot resume upload from a non-seekable source: %w", err)
		}
		remaining := size - offset
		opts.ContentLength = &remaining
		opts.ContentRange = fmt.Sprintf("bytes %d-%d/%d", offset, size-1, size)
	}
	if reply.ID != "" && reply.ID != o.info.ID {
		return fmt.Errorf("upload returned media ID %s, expected %s", reply.ID, o.info.ID)
	}
	if err := o.waitUpload(ctx, data.FolderID); err != nil {
		return err
	}
	if err := o.waitMetadata(ctx); err != nil {
		return err
	}
	return o.completeUploadRecovery(recovery, data)
}

func (o *Object) uploadOffset(ctx context.Context, size int64) (int64, error) {
	zero := int64(0)
	opts := rest.Opts{Method: http.MethodPost, RootURL: o.fs.uploadURL, Path: "/upload/file", Parameters: url.Values{"action": {"save"}, "lastupdate": {"true"}, "acceptasynchronous": {"true"}}, Body: http.NoBody, ContentLength: &zero, ContentRange: fmt.Sprintf("bytes */%d", size), IgnoreStatus: true, NoResponse: true, ExtraHeaders: map[string]string{"X-funambol-id": string(o.info.ID), "X-funambol-file-size": strconv.FormatInt(size, 10)}}
	var offset int64
	err := o.fs.pacer.Call(func() (bool, error) {
		for attempt := range maxAuthAttempts {
			state, err := o.fs.auth.prepare(ctx)
			if err != nil {
				return false, err
			}
			_, resp, err := o.fs.send(ctx, opts, nil, state)
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				o.fs.auth.invalidate(state.session)
				if attempt == 0 {
					continue
				}
			}
			if err != nil {
				return retry(ctx, resp, err)
			}
			if resp.StatusCode != 308 && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
				return retry(ctx, resp, fmt.Errorf("upload offset probe: HTTP %s", resp.Status))
			}
			rangeValue := strings.TrimPrefix(strings.TrimSpace(resp.Header.Get("Range")), "bytes=")
			start, end, ok := strings.Cut(rangeValue, "-")
			last, parseErr := strconv.ParseInt(end, 10, 64)
			if !ok || start != "0" || parseErr != nil || last < 0 || last >= size {
				return false, errors.New("upload offset probe returned an invalid Range")
			}
			offset = last + 1
			return false, nil
		}
		return false, errors.New("upload offset session renewal failed")
	})
	return offset, err
}

func (o *Object) waitUpload(ctx context.Context, folderID api.ID) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(o.fs.opt.UploadTimeout))
	defer cancel()
	delay := metadataDelay
	for {
		type response struct {
			status string
			err    error
		}
		result := make(chan response, 1)
		go func() {
			status, err := o.fs.validation.Commit(ctx, string(o.info.ID), validationItem{ctx: ctx, id: o.info.ID, folderID: folderID})
			result <- response{status, err}
		}()
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for upload processing for media %s: %w", o.info.ID, ctx.Err())
		case item := <-result:
			if item.err != nil {
				return fmt.Errorf("check upload processing for media %s: %w", o.info.ID, item.err)
			}
			switch item.status {
			case "V":
				return nil
			case "U", "A":
				// Acceptance does not guarantee the server has processed the content.
			default:
				return fmt.Errorf("upload processing for media %s returned status %q", o.info.ID, item.status)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for upload processing for media %s: %w", o.info.ID, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(2*delay, metadataMaxDelay)
	}
}

type validationItem struct {
	ctx          context.Context
	id, folderID api.ID
}

func (f *Fs) checkUploads(ctx context.Context, items []validationItem, results []string, itemErrors []error) error {
	ctx, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	var active atomic.Int32
	var ids []map[string]string
	for i, item := range items {
		if itemErrors[i] = item.ctx.Err(); itemErrors[i] == nil {
			ids = append(ids, map[string]string{"id": string(item.id), "folder_id": string(item.folderID)})
			active.Add(1)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	for i, item := range items {
		if itemErrors[i] != nil {
			continue
		}
		stop := context.AfterFunc(item.ctx, func() {
			if active.Add(-1) == 0 {
				cancel()
			}
		})
		defer stop()
	}
	reply, err := f.request(ctx, http.MethodPost, "/media", "get-validation-status", nil, map[string]any{"ids": ids})
	if err != nil {
		return err
	}
	var result struct {
		IDs []struct {
			ID     api.ID `json:"id"`
			Status string `json:"status"`
		} `json:"ids"`
	}
	if err := json.Unmarshal(reply.Data, &result); err != nil {
		return fmt.Errorf("decode upload processing status: %w", err)
	}
	statuses := map[api.ID]string{}
	for _, item := range result.IDs {
		if _, exists := statuses[item.ID]; exists {
			return errors.New("duplicate upload processing status")
		}
		if !slices.ContainsFunc(items, func(request validationItem) bool { return request.id == item.ID }) {
			return errors.New("unexpected upload processing ID")
		}
		statuses[item.ID] = item.Status
	}
	for i, item := range items {
		status, found := statuses[item.id]
		if !found {
			status = "U"
		}
		results[i], itemErrors[i] = status, item.ctx.Err()
	}
	return nil
}

// Remove moves the media item to the trash. With async_delete, success confirms queue admission.
func (o *Object) Remove(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.fs.downloadURLs.DeletePrefix(string(o.info.ID) + "/")
	switch o.info.Type {
	case "picture", "video", "audio", "file":
	default:
		return fmt.Errorf("unsupported media type %q", o.info.Type)
	}
	item := deleteItem{ctx: ctx, info: o.info, remote: o.remote}
	if o.fs.opt.AsyncDelete {
		if err := o.fs.asyncDeleteError(ctx); err != nil {
			return err
		}
		o.fs.deleteMu.Lock()
		o.fs.deletePending++
		o.fs.deleteMu.Unlock()
		// Sync cancels its worker context after admitting the last deletion.
		item.ctx = context.WithoutCancel(ctx)
	}
	result, err := o.fs.deletions.Commit(ctx, o.remote, item)
	if err != nil {
		if o.fs.opt.AsyncDelete {
			o.fs.deleteMu.Lock()
			o.fs.deletePending--
			o.fs.deleteMu.Unlock()
		}
		return err
	}
	return result
}

type deleteItem struct {
	ctx    context.Context
	info   api.Media
	remote string
	done   chan error
}

func (f *Fs) commitDeletes(_ context.Context, items []deleteItem, results []error, _ []error) error {
	start := 0
	for i, item := range items {
		if item.done == nil {
			continue
		}
		f.commitDeleteItems(items[start:i], results[start:i])
		item.done <- f.asyncDeleteError(item.ctx)
		start = i + 1
	}
	f.commitDeleteItems(items[start:], results[start:])
	// Errors are results so the shared batcher does not label deletions as uploads.
	return nil
}

func (f *Fs) commitDeleteItems(items []deleteItem, results []error) {
	groups := map[string][]int{}
	for i, item := range items {
		results[i] = item.ctx.Err()
		if results[i] == nil {
			groups[item.info.Type] = append(groups[item.info.Type], i)
		}
	}
	for _, typ := range slices.Sorted(maps.Keys(groups)) {
		indexes := groups[typ]
		func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(items[indexes[0]].ctx), metadataTimeout)
			defer cancel()
			var active atomic.Int32
			active.Store(int32(len(indexes)))
			for _, i := range indexes {
				stop := context.AfterFunc(items[i].ctx, func() {
					if active.Add(-1) == 0 {
						cancel()
					}
				})
				defer stop()
			}
			f.deleteMediaBatch(ctx, items, indexes, results)
		}()
	}
	if f.opt.AsyncDelete {
		for i, result := range results {
			var cause, reported error
			if result != nil {
				cause = fserrors.FatalError(fmt.Errorf("asynchronous deletion of %q: %w", items[i].remote, result))
				reported = fs.CountError(items[i].ctx, cause)
				fs.Errorf(f, "%v", cause)
			}
			f.deleteMu.Lock()
			f.deletePending--
			if cause != nil {
				f.deleteCause, f.deleteError = cause, reported
			}
			f.deleteMu.Unlock()
		}
	}
}

func (f *Fs) asyncDeleteError(ctx context.Context) error {
	f.deleteMu.Lock()
	defer f.deleteMu.Unlock()
	if f.deleteCause != nil && !accounting.Stats(ctx).HadFatalError() {
		// High-level attempts can reset accounting while this queue retains failures.
		f.deleteError = fs.CountError(ctx, f.deleteCause)
	}
	return f.deleteError
}

func (f *Fs) flushDeletions(ctx context.Context) error {
	if !f.opt.AsyncDelete {
		return nil
	}
	f.deleteMu.Lock()
	pending := f.deletePending
	f.deleteMu.Unlock()
	if pending == 0 {
		return f.asyncDeleteError(ctx)
	}
	done := make(chan error, 1)
	_, err := f.deletions.Commit(ctx, "pending deletions", deleteItem{ctx: ctx, done: done})
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *Fs) deleteMediaBatch(ctx context.Context, items []deleteItem, indexes []int, results []error) {
	var ids []api.ID
	seen := map[api.ID]bool{}
	active := make([]int, 0, len(indexes))
	for _, i := range indexes {
		if results[i] = items[i].ctx.Err(); results[i] != nil {
			continue
		}
		active = append(active, i)
		id := items[i].info.ID
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	if len(active) == 0 {
		return
	}
	typ := items[active[0]].info.Type
	var anchor int64
	if c := f.metadata; c != nil {
		c.mu.Lock()
		if c.state != nil {
			anchor = c.state.Anchor
		}
		c.mu.Unlock()
	}
	_, err := f.request(ctx, http.MethodPost, "/media/"+typ, "delete", url.Values{"softdelete": {"true"}}, map[string]any{typ + "s": ids})
	if err != nil {
		// Failed batches can leave some members deleted without confirming them.
		f.expireMetadata()
	}
	var apiErr *api.Error
	mediaError := errors.As(err, &apiErr)
	uncertain := !mediaError && (fserrors.IsRetryError(err) || fserrors.ShouldRetry(err))
	if mediaError {
		// Unknown exceptions and already-trashed batches do not identify completed IDs.
		uncertain = apiErr.Code == "MED-1000" || apiErr.Code == "MED-1022" && len(ids) > 1
	}
	if anchor > 0 && err != nil && uncertain {
		changes, _, checkErr := f.changes(ctx, anchor)
		if checkErr == nil {
			confirmed := map[api.ID]bool{}
			for _, id := range changes[typ].Deleted {
				confirmed[id] = true
			}
			for source, change := range changes {
				if source == "folder" {
					continue
				}
				for _, id := range slices.Concat(change.New, change.Updated, change.Locked) {
					delete(confirmed, id)
				}
			}
			remaining := active[:0]
			for _, i := range active {
				id := items[i].info.ID
				if confirmed[id] {
					results[i] = nil
					f.cacheMedia(items[i].info, true)
				} else {
					remaining = append(remaining, i)
				}
			}
			active = remaining
			if len(active) == 0 {
				return
			}
		} else {
			fs.Debugf(f, "Could not confirm batch deletions using changes: %v", checkErr)
		}
	}
	if errors.As(err, &apiErr) && apiErr.Code == "MED-1022" {
		if len(ids) == 1 {
			// Already-trashed status confirms deletion only for a single ID.
			err = nil
		} else {
			// A batch's already-trashed error does not identify the affected ID.
			for _, i := range active {
				f.deleteMediaBatch(ctx, items, []int{i}, results)
			}
			return
		}
	}
	for _, i := range active {
		results[i] = err
		if err == nil {
			f.cacheMedia(items[i].info, true)
		}
	}
}

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.ListRer         = (*Fs)(nil)
	_ fs.ChangeNotifier  = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
