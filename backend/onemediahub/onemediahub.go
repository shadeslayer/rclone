// Package onemediahub implements the OneMediaHub Server API.
package onemediahub

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/batcher"
	"github.com/rclone/rclone/lib/cache"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/kv"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/singleflight"
)

const (
	pageSize             = 100
	minSleep             = 10 * time.Millisecond
	maxSleep             = 2 * time.Second
	decayConstant        = 2
	dateFormat           = "20060102T150405Z"
	statusDeleted        = "D"
	maxAuthAttempts      = 2
	maxRedirects         = 10
	deviceHeader         = "X-deviceid"
	oauthDevicePrefix    = "fol-"
	passwordDevicePrefix = "fac-"
	metadataTimeout      = 30 * time.Second
	metadataDelay        = 200 * time.Millisecond
	metadataMaxDelay     = 2 * time.Second
	maxErrorSize         = 64 * 1024
	sessionCookie        = "JSESSIONID"
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
				Help:     "Cache account metadata and refresh it using the changes API. Persists in rclone's cache directory on supported systems. Requires /profile and /profile/changes support.",
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
				Name:     "async_upload",
				Help:     "Register metadata separately and upload raw content with asynchronous server processing instead of multipart uploads.",
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
	UploadTimeout     fs.Duration          `config:"upload_timeout"`
	MetadataCache     bool                 `config:"metadata_cache"`      // MetadataCache enables account metadata caching.
	MetadataCacheTime fs.Duration          `config:"metadata_cache_time"` // MetadataCacheTime is the interval between changes API refreshes.
	APIPath           string               `config:"api_path"`
	RootFolderID      string               `config:"root_folder_id"`
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
	if opt.MetadataCache && opt.MetadataCacheTime <= 0 {
		return nil, errors.New("metadata_cache_time must be positive")
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

// Fs represents a OneMediaHub remote.
type Fs struct {
	downloadURLs     *cache.Cache
	downloadRefresh  singleflight.Group
	validation       *batcher.Batcher[validationItem, string]
	validationCancel context.CancelFunc
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
}

// Object describes a OneMediaHub media item.
type Object struct {
	fs     *Fs
	remote string
	info   api.Media
}

// NewFs creates a OneMediaHub filesystem, returning ErrorIsFile for a file root.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt, err := readOptions(m)
	if err != nil {
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
	f.dirCache = dircache.New(f.root, opt.RootFolderID, f)
	info, err := serverInfo(ctx, srv)
	if err != nil {
		return nil, err
	}
	uploadURL := opt.UploadURL
	if uploadURL == "" {
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
	if _, err = f.auth.prepare(ctx); err != nil {
		return nil, err
	}
	if opt.MetadataCache {
		if err := f.startMetadataCache(ctx); err != nil {
			return nil, fmt.Errorf("start metadata cache: %w", err)
		}
	}
	keepCache := false
	defer func() {
		if !keepCache {
			_ = f.Shutdown(ctx)
		}
	}()
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

	err = f.dirCache.FindRoot(ctx, false)
	if err == nil {
		keepCache = true
		return f, nil
	}

	if !errors.Is(err, fs.ErrorDirNotFound) {
		return nil, err
	}
	parent, leaf := dircache.SplitPath(f.root)
	originalRoot, originalCache := f.root, f.dirCache
	f.root = parent
	f.dirCache = dircache.New(parent, opt.RootFolderID, f)
	_, err = f.NewObject(ctx, leaf)
	if err == nil {
		keepCache = true
		return f, fs.ErrorIsFile
	}
	f.root, f.dirCache = originalRoot, originalCache
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) {
		keepCache = true
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

func serverInfo(ctx context.Context, srv *rest.Client) (*api.ServerInfo, error) {
	var info api.ServerInfo
	_, err := srv.CallJSON(ctx, &rest.Opts{Method: http.MethodGet, Path: "/system/information", Parameters: url.Values{"action": {"get"}}, NoRedirect: true}, nil, &info)
	if err != nil {
		return nil, fmt.Errorf("read OneMediaHub server information: %w", err)
	}

	if info.Error != nil {
		return nil, info.Error
	}
	return &info, nil
}

func errorHandler(resp *http.Response) error {
	var reply api.Response
	if err := rest.DecodeJSON(resp, &reply); err == nil && reply.Error != nil {
		return reply.Error
	}
	if req := resp.Request; req != nil && req.URL != nil {
		return fmt.Errorf("OneMediaHub HTTP error: %s (%s %s%s action=%q)", resp.Status, req.Method, req.URL.Host, req.URL.EscapedPath(), req.URL.Query().Get("action"))
	}
	return fmt.Errorf("OneMediaHub HTTP error: %s", resp.Status)
}

func retry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}), err
}

// call checks JSON errors even on HTTP success and renews expired sessions once.
func (f *Fs) call(ctx context.Context, opts rest.Opts, request any) (reply api.Response, err error) {
	run := func() (bool, error) {
		for attempt := 0; attempt < maxAuthAttempts; attempt++ {
			state, err := f.auth.prepare(ctx)
			if err != nil {
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
			return retry(ctx, resp, err)
		}
		return false, errors.New("session renewal failed")
	}
	// Only read operations may be replayed after ambiguous network failures.
	action := opts.Parameters.Get("action")
	if opts.Body == nil && (action == "get" || action == "get-storage-space") {
		err = f.pacer.Call(run)
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
	for key, value := range opts.ExtraHeaders {
		attemptOpts.ExtraHeaders[key] = value
	}
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

	if err == nil {
		b, readErr := rest.ReadBody(resp)
		err = readErr
		if err == nil && len(strings.TrimSpace(string(b))) != 0 {
			err = json.Unmarshal(b, &reply)
		}

		if err == nil && reply.Error != nil {
			err = reply.Error
		}
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
	Version        int                  // Version identifies the cache format.
	Anchor         int64                // Anchor is the last applied server response time in milliseconds.
	Folders        []api.Folder         // Folders contains account folders.
	Media          map[api.ID]api.Media // Media indexes account files by ID.
	Pending        []api.ID             // Pending identifies locked or unavailable media to fetch again.
	PendingFolders []api.ID             // PendingFolders identifies unavailable or locked folders to fetch again.
}

type metadataCache struct {
	refreshMu sync.Mutex
	mu        sync.Mutex
	db        *kv.DB
	state     *metadataSnapshot
	checked   time.Time
	byName    map[mediaKey]api.ID
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
	return mediaKey{parent: parent, name: f.opt.Enc.ToStandardName(item.Name)}
}

func (f *Fs) cacheMedia(item api.Media, remove bool) {
	if f.downloadURLs != nil {
		f.downloadURLs.DeletePrefix(string(item.ID) + "/")
	}
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if old, ok := c.state.Media[item.ID]; ok && (remove || f.mediaKey(old) != f.mediaKey(item)) {
			key := f.mediaKey(old)
			if c.byName[key] == item.ID {
				delete(c.byName, key)
				for id, other := range c.state.Media {
					if id != item.ID && f.mediaKey(other) == key && id > c.byName[key] {
						c.byName[key] = id
					}
				}
			}
		}
		delete(c.state.Media, item.ID)
		if !remove {
			c.state.Media[item.ID] = item
			key := f.mediaKey(item)
			if item.ID >= c.byName[key] {
				c.byName[key] = item.ID
			}
		}
	}
}

func (f *Fs) cacheFolder(folder api.Folder, remove bool) {
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.state.Folders = slices.DeleteFunc(c.state.Folders, func(old api.Folder) bool { return old.ID == folder.ID })
		if !remove {
			c.state.Folders = append(c.state.Folders, folder)
		}
	}
}

type metadataOp struct {
	state *metadataSnapshot
	write bool
}

func (op *metadataOp) Do(_ context.Context, bucket kv.Bucket) error {
	key := []byte("metadata")
	if !op.write {
		if b := bucket.Get(key); b != nil {
			return json.Unmarshal(b, &op.state)
		}
		return nil
	}
	b, err := json.Marshal(op.state)
	if err != nil {
		return err
	}
	return bucket.Put(key, b)
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
		} else if op.state != nil && op.state.Version == 1 && op.state.Anchor > 0 && op.state.Media != nil {
			f.metadata.state = op.state
			fs.Debugf(f, "Loaded metadata cache")
		}
	}
	if err := f.syncMetadata(ctx); err != nil {
		_ = f.Shutdown(ctx)
		return err
	}
	return nil
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
	for source, changes := range raw {
		if !slices.Contains([]string{"folder", "file", "picture", "video", "audio"}, source) {
			return nil, 0, fmt.Errorf("changes API returned unsupported source %q", source)
		}
		for status := range changes {
			if !slices.Contains([]string{"N", "U", "D", "L"}, status) {
				return nil, 0, fmt.Errorf("changes API returned unsupported status %q", status)
			}
		}
	}
	var changes map[string]api.Changes
	if err := json.Unmarshal(reply.Data, &changes); err != nil {
		return nil, 0, err
	}
	return changes, timestamp, nil
}

func (f *Fs) syncMetadata(ctx context.Context) error {
	c := f.metadata
	if c == nil {
		return nil
	}
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
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
	next := &metadataSnapshot{Version: 1, Media: map[api.ID]api.Media{}}
	if c.state != nil {
		*next = *c.state
		next.Media = maps.Clone(c.state.Media)
		next.Folders = slices.Clone(c.state.Folders)
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
				if !folder.SoftDeleted && folder.Status != statusDeleted {
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
		}
		for _, id := range change.Deleted {
			delete(next.Media, id)
			delete(ids, id)
		}
	}
	ordered := slices.Sorted(maps.Keys(ids))
	for batch := range slices.Chunk(ordered, pageSize) {
		if err := f.fetchMedia(ctx, batch, func(item api.Media) error {
			next.Media[item.ID] = item
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
	next.Anchor = timestamp
	if c.db != nil {
		if err := c.db.Do(true, &metadataOp{state: next, write: true}); err != nil {
			return false, fmt.Errorf("save metadata cache: %w", err)
		}
	}
	for source, change := range changes {
		if source == "folder" {
			continue
		}
		for _, id := range slices.Concat(change.New, change.Updated, change.Deleted, change.Locked) {
			f.downloadURLs.DeletePrefix(string(id) + "/")
		}
	}
	c.state = next
	c.byName = make(map[mediaKey]api.ID, len(next.Media))
	for _, id := range slices.Sorted(maps.Keys(next.Media)) {
		c.byName[f.mediaKey(next.Media[id])] = id
	}
	c.checked = time.Now()
	fs.Debugf(f, "Refreshed metadata cache: fetched %d media IDs, %d pending", len(ordered), len(next.Pending))
	return foldersChanged, nil
}

// Shutdown closes the persistent metadata cache.
func (f *Fs) Shutdown(ctx context.Context) error {
	if f.validationCancel != nil {
		f.validationCancel()
	}
	if f.validation != nil {
		f.validation.Shutdown()
	}
	if c := f.metadata; c != nil {
		c.refreshMu.Lock()
		defer c.refreshMu.Unlock()
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.db != nil && !c.db.IsStopped() {
			var err error
			if c.state != nil {
				err = c.db.Do(true, &metadataOp{state: c.state, write: true})
			}
			err = errors.Join(err, c.db.Stop(false))
			c.db = nil
			return err
		}
	}
	return nil
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
		if sameParent(folder.ParentID, parent) && f.opt.Enc.ToStandardName(folder.Name) == leaf && folder.Status != statusDeleted && !folder.SoftDeleted {
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
			if err := visit(item); err != nil {
				return err
			}
		}
		return nil
	}
	return f.fetchMedia(ctx, ids, visit)
}

func (f *Fs) fetchMedia(ctx context.Context, ids []api.ID, visit func(api.Media) error) error {
	for offset := 0; ; {
		params := url.Values{}
		data := map[string]any{"fields": []string{"name", "size", "modificationdate", "url", "folderid"}}
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
			if item.SoftDeleted || item.Status == statusDeleted {
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

// List lists the files and directories in dir.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if err := f.syncMetadata(ctx); err != nil {
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
		return string(folder.ID) == parent && !folder.SoftDeleted && folder.Status != statusDeleted
	}) {
		return nil, fs.ErrorDirNotFound
	}
	var entries fs.DirEntries
	for _, folder := range folders {
		if !sameParent(folder.ParentID, parent) || folder.Status == statusDeleted || folder.SoftDeleted {
			continue
		}
		remote := path.Join(dir, f.opt.Enc.ToStandardName(folder.Name))
		f.dirCache.Put(remote, string(folder.ID))
		entries = append(entries, fs.NewDir(remote, time.UnixMilli(folder.Date)).SetID(string(folder.ID)))
	}
	err = f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, parent) {
			entries = append(entries, &Object{fs: f, remote: path.Join(dir, f.opt.Enc.ToStandardName(item.Name)), info: item})
		}
		return nil
	})
	return entries, err
}

// NewObject finds a file by its path, returning ErrorObjectNotFound if absent.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if err := f.syncMetadata(ctx); err != nil {
		return nil, err
	}
	leaf, parent, err := f.dirCache.FindPath(ctx, remote, false)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil, fs.ErrorObjectNotFound
	}

	if err != nil {
		return nil, err
	}
	if c := f.metadata; c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if parent == "0" {
			parent = ""
		}
		if parent != "" && !slices.ContainsFunc(c.state.Folders, func(folder api.Folder) bool {
			return string(folder.ID) == parent && !folder.SoftDeleted && folder.Status != statusDeleted
		}) {
			return nil, fs.ErrorObjectNotFound
		}
		if id, ok := c.byName[mediaKey{parent: parent, name: leaf}]; ok {
			return &Object{fs: f, remote: remote, info: c.state.Media[id]}, nil
		}
		return nil, fs.ErrorObjectNotFound
	}
	var found *Object
	err = f.media(ctx, nil, func(item api.Media) error {
		if sameParent(item.FolderID, parent) && f.opt.Enc.ToStandardName(item.Name) == leaf {
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
	if err := f.syncMetadata(ctx); err != nil {
		return err
	}
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes an empty directory, returning ErrorDirectoryNotEmpty otherwise.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
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

	if id == "" {
		return nil
	}
	_, err = f.request(ctx, http.MethodPost, "/media/folder", "delete", nil, map[string]any{"folders": []api.ID{api.ID(id)}})
	if err == nil {
		f.cacheFolder(api.Folder{ID: api.ID(id)}, true)
		f.dirCache.FlushDir(dir)
	} else {
		f.expireMetadata()
	}
	return err
}

// DirMove moves a directory on the same account, returning ErrorDirExists for an existing destination.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || strings.TrimRight(f.opt.URL, "/") != strings.TrimRight(srcFs.opt.URL, "/") || f.opt.APIPath != srcFs.opt.APIPath {
		return fs.ErrorCantDirMove
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
	f.expireMetadata()
	if err := f.syncMetadata(ctx); err != nil {
		return err
	}
	if srcFs != f {
		srcFs.expireMetadata()
		if err := srcFs.syncMetadata(ctx); err != nil {
			return err
		}
	}
	f.dirCache.ResetRoot()
	if srcFs != f {
		srcFs.dirCache.ResetRoot()
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
	if err != nil {
		f.expireMetadata()
		srcFs.expireMetadata()
		f.dirCache.ResetRoot()
		if srcFs != f {
			srcFs.dirCache.ResetRoot()
		}
		return err
	}
	f.cacheFolder(folder, false)
	if srcFs != f {
		srcFs.cacheFolder(folder, false)
	}
	f.dirCache.ResetRoot()
	if srcFs != f {
		srcFs.dirCache.ResetRoot()
	}
	return nil
}

// Put creates or replaces an object and preserves its upload modification time.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, opts ...fs.OpenOption) (fs.Object, error) {
	existing, err := f.NewObject(ctx, src.Remote())
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
	// Some providers have no downloadable blob for a stored empty file.
	if o.Size() == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	key := fmt.Sprintf("%s/%d/%d/%d", o.info.ID, o.info.Size, o.info.Modified, o.info.Date)
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
	for attempt := range 2 {
		u, err := url.Parse(rawURL)
		if err != nil || rawURL == "" {
			return nil, errors.New("invalid media download URL")
		}
		base, _ := url.Parse(o.fs.opt.URL + "/")
		u = base.ResolveReference(u)
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
		if snapshot.info.Size != o.info.Size || o.info.Modified != 0 && snapshot.info.Modified != o.info.Modified {
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
	leaf, parent, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	modified := src.ModTime(ctx).UTC().Format(dateFormat)
	data := api.Upload{ID: string(o.info.ID), FolderID: api.ID(parent), Name: o.fs.opt.Enc.FromStandardName(leaf), Size: src.Size(), ContentType: fs.MimeType(ctx, src), Created: modified, Modified: modified}
	if o.fs.opt.AsyncUpload {
		return o.uploadAsync(ctx, in, data, options)
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

func (o *Object) uploadAsync(ctx context.Context, in io.Reader, data api.Upload, options []fs.OpenOption) error {
	metadata, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return err
	}
	opts := rest.Opts{Method: http.MethodPost, RootURL: o.fs.uploadURL, Path: "/upload/file", Parameters: url.Values{"action": {"save-metadata"}, "responsetime": {"true"}, "lastupdate": {"true"}}, ContentType: "application/octet-stream", Body: bytes.NewReader(metadata)}
	reply, err := o.fs.call(ctx, opts, nil)
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
	size := data.Size
	if size == 0 {
		// net/http treats an arbitrary reader with zero ContentLength as an unknown length.
		in = http.NoBody
	}
	opts.Parameters = url.Values{"action": {"save"}, "lastupdate": {"true"}, "acceptasynchronous": {"true"}}
	opts.Body = in
	opts.ContentType = data.ContentType
	opts.ContentLength = &size
	opts.Options = options
	opts.ExtraHeaders = map[string]string{"X-funambol-id": string(reply.ID), "X-funambol-file-size": strconv.FormatInt(size, 10)}
	reply, err = o.fs.call(ctx, opts, nil)
	if err != nil {
		return fmt.Errorf("upload media %s: %w", o.info.ID, err)
	}
	if reply.ID != "" && reply.ID != o.info.ID {
		return fmt.Errorf("upload returned media ID %s, expected %s", reply.ID, o.info.ID)
	}
	if err := o.waitUpload(ctx, data.FolderID); err != nil {
		return err
	}
	return o.waitMetadata(ctx)
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

// Remove moves the media item to the trash.
func (o *Object) Remove(ctx context.Context) error {
	o.fs.downloadURLs.DeletePrefix(string(o.info.ID) + "/")
	var field string
	switch o.info.Type {
	case "picture":
		field = "pictures"
	case "video":
		field = "videos"
	case "audio":
		field = "audios"
	case "file":
		field = "files"
	default:
		return fmt.Errorf("unsupported media type %q", o.info.Type)
	}
	_, err := o.fs.request(ctx, http.MethodPost, "/media/"+o.info.Type, "delete", url.Values{"softdelete": {"true"}}, map[string]any{field: []api.ID{o.info.ID}})
	if err == nil {
		o.fs.cacheMedia(o.info, true)
	} else {
		o.fs.expireMetadata()
	}
	return err
}

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
