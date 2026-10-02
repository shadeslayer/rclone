// Package onemediahub implements the OneMediaHub Server API.
package onemediahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	pageSize        = 100
	minSleep        = 10 * time.Millisecond
	maxSleep        = 2 * time.Second
	decayConstant   = 2
	dateFormat      = "20060102T150405Z"
	statusDeleted   = "D"
	maxAuthAttempts = 2
	maxRedirects    = 10
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
				Help:     "OAuth authorization endpoint. Empty uses the O2 default only for the O2 server.",
				Advanced: true,
			},
			{
				Name:     "token_url",
				Help:     "OAuth token endpoint. Empty uses the O2 default only for the O2 server.",
				Advanced: true,
			},
			{
				Name:     "redirect_url",
				Help:     "Registered OAuth callback URL. Empty uses the O2 default only for the O2 server.",
				Advanced: true,
			},
			{
				Name:     "scope",
				Help:     "Space-separated OAuth scopes; offline access must yield a refresh token.",
				Default:  "openid",
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
				Name:     "user_agent",
				Help:     "HTTP User-Agent accepted by the provider.",
				Default:  "OneMediaHub",
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
				Default:  encoder.Display | encoder.EncodeBackSlash | encoder.EncodeInvalidUtf8,
				Advanced: true,
			},
		},
	})
}

type options struct {
	URL          string               `config:"url"`
	AuthType     string               `config:"auth_type"`
	User         string               `config:"user"`
	Password     string               `config:"password"`
	ClientID     string               `config:"client_id"`
	ClientSecret string               `config:"client_secret"`
	AuthURL      string               `config:"auth_url"`
	TokenURL     string               `config:"token_url"`
	RedirectURL  string               `config:"redirect_url"`
	Scope        string               `config:"scope"`
	Platform     string               `config:"platform"`
	MSISDN       string               `config:"msisdn"`
	UserAgent    string               `config:"user_agent"`
	UploadURL    string               `config:"upload_url"`
	APIPath      string               `config:"api_path"`
	RootFolderID string               `config:"root_folder_id"`
	Enc          encoder.MultiEncoder `config:"encoding"`
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
	uploadURL string
	name      string
	root      string
	opt       *options
	features  *fs.Features
	srv       *rest.Client
	download  *rest.Client
	auth      *auth
	dirCache  *dircache.DirCache
	pacer     *fs.Pacer
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
	base := strings.TrimRight(opt.URL, "/") + "/" + strings.Trim(opt.APIPath, "/")
	client := newClient(ctx, opt)
	srv := rest.NewClient(client).SetRoot(base).SetErrorHandler(errorHandler)
	f := &Fs{name: name, root: strings.Trim(root, "/"), opt: opt, srv: srv, download: rest.NewClient(client),
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

	err = f.dirCache.FindRoot(ctx, false)
	if err == nil {
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
		return f, fs.ErrorIsFile
	}
	f.root, f.dirCache = originalRoot, originalCache
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) {
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
	attemptOpts.Parameters.Set("validationkey", session.Key)
	attemptOpts.ExtraHeaders = map[string]string{}
	for key, value := range opts.ExtraHeaders {
		attemptOpts.ExtraHeaders[key] = value
	}
	cookie := &http.Cookie{Name: "JSESSIONID", Value: session.ID}
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

func (f *Fs) folders(ctx context.Context) ([]api.Folder, error) {
	var folders []api.Folder
	for offset := 0; ; {
		reply, err := f.request(ctx, http.MethodGet, "/media/folder", "get", url.Values{"limit": {strconv.Itoa(pageSize)}, "offset": {strconv.Itoa(offset)}}, nil)
		if err != nil {
			return nil, err
		}
		var data struct {
			Folders []api.Folder `json:"folders"`
		}

		if err := json.Unmarshal(reply.Data, &data); err != nil {
			return nil, err
		}
		folders = append(folders, data.Folders...)
		if len(data.Folders) < pageSize {
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
	leaf = f.opt.Enc.FromStandardName(leaf)
	for _, folder := range folders {
		if sameParent(folder.ParentID, parent) && folder.Name == leaf && folder.Status != statusDeleted && !folder.SoftDeleted {
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
		return "", err
	}

	if reply.ID == "" {
		return "", errors.New("folder creation returned no ID")
	}
	return string(reply.ID), nil
}

func (f *Fs) media(ctx context.Context, ids []api.ID, visit func(api.Media) error) error {
	for offset := 0; ; {
		params := url.Values{}
		data := map[string]any{"fields": []string{"name", "size", "modificationdate", "url", "folderid"}}
		if ids != nil {
			data["ids"] = ids
		} else {
			params.Set("limit", strconv.Itoa(pageSize))
			params.Set("offset", strconv.Itoa(offset))
		}
		reply, err := f.request(ctx, http.MethodPost, "/media", "get", params, data)
		if err != nil {
			return err
		}
		var result struct {
			Media []api.Media `json:"media"`
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

		if ids != nil || !reply.More {
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
	leaf, parent, err := f.dirCache.FindPath(ctx, remote, false)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil, fs.ErrorObjectNotFound
	}

	if err != nil {
		return nil, err
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
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes an empty directory, returning ErrorDirectoryNotEmpty otherwise.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
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
		f.dirCache.FlushDir(dir)
	}
	return err
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

// Open downloads original content and supports range requests.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Range downloads may open the same object concurrently.
	snapshot := *o
	o = &snapshot
	if err := o.refresh(ctx); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.info.URL)
	if err != nil || o.info.URL == "" {
		return nil, errors.New("invalid media download URL")
	}
	base, _ := url.Parse(o.fs.opt.URL + "/")
	u = base.ResolveReference(u)
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("unsupported download URL scheme")
	}
	fs.FixRangeOption(options, o.Size())
	return o.fs.open(ctx, u, options)
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
				opts.ExtraHeaders = map[string]string{"Cookie": (&http.Cookie{Name: "JSESSIONID", Value: state.session.ID}).String()}
				if state.header != "" {
					opts.ExtraHeaders["Authorization"] = state.header
				}
				target := *u
				q := target.Query()
				q.Set("validationkey", state.session.Key)
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
				if resp.StatusCode == http.StatusUnauthorized {
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
		return nil, err
	}
	return resp.Body, nil
}

// Update replaces content using a multipart upload with metadata.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("OneMediaHub uploads require a known size")
	}
	leaf, parent, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	modified := src.ModTime(ctx).UTC().Format(dateFormat)
	data := api.Upload{ID: string(o.info.ID), FolderID: api.ID(parent), Name: o.fs.opt.Enc.FromStandardName(leaf), Size: src.Size(), ContentType: fs.MimeType(ctx, src), Created: modified, Modified: modified}
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
	return o.refresh(ctx)
}

// Remove moves the media item to the trash.
func (o *Object) Remove(ctx context.Context) error {
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
	return err
}

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
