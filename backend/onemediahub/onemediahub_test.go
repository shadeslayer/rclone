package onemediahub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func testConfig(t *testing.T, values configmap.Simple) configmap.Simple {
	t.Helper()
	ri, err := fs.Find("onemediahub")
	require.NoError(t, err)
	m := configmap.Simple{}
	for _, opt := range ri.Options {
		m[opt.Name] = opt.String()
	}
	for key, value := range values {
		m[key] = value
	}
	return m
}

func jsonReply(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	assert.NoError(t, json.NewEncoder(w).Encode(value))
}

func TestMetadataCache(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "directory", Status: "U"}}
	fx.media = []api.Media{{ID: "2", FolderID: "1", Name: "old.txt", Size: 3, Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"1"}}, "file": {New: []api.ID{"2"}}}
	m := testConfig(t, configmap.Simple{"url": fx.server.URL, "auth_type": authPassword, "user": "alice", "password": obscure.MustObscure("password"), "metadata_cache": "true"})
	remote, err := NewFs(ctx, "metadata-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			obj, err := f.NewObject(ctx, "directory/old.txt")
			assert.NoError(t, err)
			if obj != nil {
				assert.EqualValues(t, 3, obj.Size())
			}
		})
	}
	wg.Wait()
	fx.mu.Lock()
	assert.Equal(t, 1, fx.requests["/sapi/profile/changes"])
	assert.Equal(t, 1, fx.requests["/sapi/media"])
	assert.Equal(t, 1, fx.requests["/sapi/media/folder"])
	fx.requestTime += 60000
	fx.folders[0].Name = "renamed"
	fx.media = []api.Media{{ID: "3", FolderID: "1", Name: "new.txt", Size: 7, Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {Updated: []api.ID{"1"}}, "file": {New: []api.ID{"3"}, Deleted: []api.ID{"2"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	for range 12 {
		wg.Go(func() {
			_, err := f.NewObject(ctx, "directory/old.txt")
			assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
			_, err = f.NewObject(ctx, "renamed/new.txt")
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	obj, err := f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 7, obj.Size())

	// A failed metadata fetch must leave the saved change cursor unchanged.
	fx.mu.Lock()
	fx.requestTime += 60000
	fx.media[0].Size = 9
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"3"}}}
	fx.failMedia = true
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	anchor := f.metadata.state.Anchor
	f.metadata.mu.Unlock()
	_, err = f.NewObject(ctx, "renamed/new.txt")
	require.ErrorContains(t, err, "failed metadata")
	assert.Equal(t, anchor, f.metadata.state.Anchor)
	fx.mu.Lock()
	fx.failMedia = false
	fx.mu.Unlock()
	obj, err = f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 9, obj.Size())

	// Download lookups must not rewrite comparison metadata outside a changes refresh.
	fx.mu.Lock()
	fx.media[0].Name = "download-name.txt"
	fx.media[0].URL = fx.server.URL + "/content/3"
	fx.content["3"] = "123456789"
	fx.mu.Unlock()
	body, err := obj.Open(ctx)
	require.NoError(t, err)
	contentBytes, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	assert.Equal(t, "123456789", string(contentBytes))
	_, err = f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].Name = "new.txt"
	fx.mu.Unlock()

	// A second filesystem reads the persisted snapshot and applies only changes.
	fx.mu.Lock()
	fx.changes = map[string]api.Changes{}
	folderRequests := fx.requests["/sapi/media/folder"]
	mediaRequests := fx.requests["/sapi/media"]
	fx.mu.Unlock()
	restarted, err := NewFs(ctx, "metadata-test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.(*Fs).Shutdown(ctx)) })
	obj, err = restarted.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 9, obj.Size())
	fx.mu.Lock()
	assert.Equal(t, folderRequests, fx.requests["/sapi/media/folder"])
	assert.Equal(t, mediaRequests, fx.requests["/sapi/media"])
	fx.mu.Unlock()

	// Local writes must be visible before the next changes poll.
	fx.mu.Lock()
	fx.nextID = 10
	fx.mu.Unlock()
	content := "cached upload"
	src := object.NewStaticObjectInfo("created/file.txt", time.Now(), int64(len(content)), true, nil, f)
	uploaded, err := f.Put(ctx, strings.NewReader(content), src)
	require.NoError(t, err)
	listed, err := f.List(ctx, "created")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	found, err := f.NewObject(ctx, src.Remote())
	require.NoError(t, err)
	assert.Equal(t, uploaded.(*Object).ID(), found.(*Object).ID())
	require.NoError(t, uploaded.Remove(ctx))
	_, err = f.NewObject(ctx, src.Remote())
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	require.NoError(t, f.Rmdir(ctx, "created"))

	// Directory removal must check external changes even within the cache interval.
	require.NoError(t, f.Mkdir(ctx, "external"))
	parent, err := f.dirCache.FindDir(ctx, "external", false)
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media = append(fx.media, api.Media{ID: "99", FolderID: api.ID(parent), Name: "external.txt", Size: 1, Type: "file"})
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"99"}}}
	fx.mu.Unlock()
	assert.ErrorIs(t, f.Rmdir(ctx, "external"), fs.ErrorDirectoryNotEmpty)
}

func TestMetadataCacheDeletedRoot(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "root"}}
	fx.media = []api.Media{{ID: "2", FolderID: "1", Name: "file", Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"1"}}, "file": {New: []api.ID{"2"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	m["root_folder_id"] = "1"
	remote, err := NewFs(ctx, "deleted-root-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	_, err = f.NewObject(ctx, "file")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.folders = nil
	fx.changes = map[string]api.Changes{"folder": {Deleted: []api.ID{"1"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	_, err = f.NewObject(ctx, "file")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestMetadataCacheFolderDeltas(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "old"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "3", Name: "deleted"}}
	fx.changes = map[string]api.Changes{}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "folder-deltas", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	_, err = f.List(ctx, "old/child")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.folders[0].Name = "renamed"
	fx.folders = fx.folders[:2]
	fx.changes = map[string]api.Changes{"folder": {Updated: []api.ID{"1"}, New: []api.ID{"4"}, Deleted: []api.ID{"3"}}}
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	_, err = f.List(ctx, "renamed/child")
	require.NoError(t, err)
	assert.Equal(t, [][]api.ID{{"1", "4"}}, fx.folderBatches)
	_, err = f.List(ctx, "old/child")
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
	_, err = f.List(ctx, "deleted")
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
	fx.mu.Lock()
	fx.changes = map[string]api.Changes{}
	fx.folders = append(fx.folders, api.Folder{ID: "4", Name: "late"})
	fx.failFolders = true
	fx.mu.Unlock()
	f.expireMetadata()
	anchor := f.metadata.state.Anchor
	_, err = f.List(ctx, "late")
	require.ErrorContains(t, err, "failed folders")
	assert.Equal(t, anchor, f.metadata.state.Anchor)
	fx.mu.Lock()
	fx.failFolders = false
	fx.mu.Unlock()
	_, err = f.List(ctx, "late")
	require.NoError(t, err)
	assert.Equal(t, [][]api.ID{{"1", "4"}, {"4"}}, fx.folderBatches)

	// A child can arrive before its parent is visible in the changes feed.
	fx.mu.Lock()
	fx.folders = append(fx.folders, api.Folder{ID: "5", ParentID: "6", Name: "child"})
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"5"}}}
	fx.mu.Unlock()
	f.expireMetadata()
	require.NoError(t, f.syncMetadata(ctx))
	assert.Equal(t, []api.ID{"6"}, f.metadata.state.PendingFolders)
	fx.mu.Lock()
	fx.folders = append(fx.folders, api.Folder{ID: "6", Name: "parent"})
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	f.expireMetadata()
	_, err = f.List(ctx, "parent/child")
	require.NoError(t, err)
	assert.Empty(t, f.metadata.state.PendingFolders)
}

func TestMetadataCachePendingAndBatching(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	change := api.Changes{Locked: []api.ID{"999"}}
	fx.media = append(fx.media, api.Media{ID: "999", Name: "locked", Status: "L", Type: "file"})
	for i := 1; i <= pageSize+1; i++ {
		id := api.ID(strconv.Itoa(i))
		change.New = append(change.New, id)
		if i > 1 {
			fx.media = append(fx.media, api.Media{ID: id, Name: "file" + string(id), Size: 1, Type: "file"})
		}
	}
	fx.changes = map[string]api.Changes{"file": change}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "pending-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	assert.ElementsMatch(t, []int{pageSize, 2}, fx.mediaBatches)
	assert.ElementsMatch(t, []api.ID{"1", "999"}, f.metadata.state.Pending)
	assert.Equal(t, "19700101T000000Z", fx.changesFrom[0])
	anchor := f.metadata.state.Anchor
	fx.mu.Lock()
	fx.media = append(fx.media, api.Media{ID: "1", Name: "ready", Size: 5, Type: "file"})
	fx.requestTime += 60000
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	obj, err := f.NewObject(ctx, "ready")
	require.NoError(t, err)
	assert.EqualValues(t, 5, obj.Size())
	assert.Equal(t, []api.ID{"999"}, f.metadata.state.Pending)
	assert.Equal(t, time.UnixMilli(anchor).Add(-time.Second).UTC().Format(dateFormat), fx.changesFrom[1])
	assert.Equal(t, 2, fx.mediaBatches[2])
	fx.mu.Lock()
	fx.requestTime += 60000
	fx.changes = map[string]api.Changes{"file": {Deleted: []api.ID{"999"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	_, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, f.metadata.state.Pending)

	// Reconnecting the same remote to another account must start a new snapshot.
	fx.mu.Lock()
	fx.accountID = "another-account"
	fx.media = nil
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	other, err := NewFs(ctx, "pending-test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.(*Fs).Shutdown(ctx)) })
	_, err = other.NewObject(ctx, "ready")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.Equal(t, "19700101T000000Z", fx.changesFrom[3])
}

func TestMetadataCacheInvalidChanges(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	for _, tc := range []struct {
		name      string
		timestamp int64
		data      any
		want      string
	}{
		{"MissingTime", 0, map[string]any{}, "invalid requesttime"},
		{"UnknownSource", 1700000000123, map[string]any{"unexpected": map[string]any{}}, "unsupported source"},
		{"UnknownStatus", 1700000000123, map[string]any{"file": map[string]any{"X": []int{1}}}, "unsupported status"},
		{"InvalidID", 1700000000123, map[string]any{"file": map[string]any{"N": []string{"invalid"}}}, "invalid changes"},
		{"MissingData", 1700000000123, nil, "invalid changes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			fx.requestTime, fx.changeData = tc.timestamp, tc.data
			m := fx.config(t)
			m["metadata_cache"] = "true"
			_, err := NewFs(context.Background(), "invalid-test", "", m)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSessionRenewal(t *testing.T) {
	var logins atomic.Int32
	var expire atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		assert.Equal(t, "OneMediaHub", r.UserAgent())
		if r.URL.Path == "/tenant/api/system/information" {
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
			return
		}
		if r.URL.Path == "/tenant/api/login" {
			assert.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, r.ParseForm())
			assert.Equal(t, "alice", r.PostForm.Get("login"))
			assert.Equal(t, "password&=", r.PostForm.Get("password"))
			assert.Empty(t, r.URL.Query().Get("password"))
			assert.Empty(t, r.Header.Get("Cookie"))
			n := logins.Add(1)
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": fmt.Sprintf("session%d", n), "validationkey": fmt.Sprintf("key%d", n)}})
			return
		}
		if expire.Swap(false) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		assert.Equal(t, fmt.Sprintf("key%d", logins.Load()), r.URL.Query().Get("validationkey"))
		cookie, err := r.Cookie("JSESSIONID")
		assert.NoError(t, err)
		if cookie != nil {
			assert.Equal(t, fmt.Sprintf("session%d", logins.Load()), cookie.Value)
		}
		switch r.URL.Path {
		case "/tenant/api/media/folder":
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": []any{}}})
		case "/tenant/api/media":
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []any{}}, "more": false})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL + "/tenant", "api_path": "/api", "auth_type": authPassword, "user": "alice", "password": obscure.MustObscure("password&=")})
	ctx := context.Background()
	_, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	remote, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	require.EqualValues(t, 1, logins.Load())
	expire.Store(true)
	entries, err := remote.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.EqualValues(t, 2, logins.Load())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _, err := remote.List(ctx, ""); assert.NoError(t, err) })
	}
	wg.Wait()
	assert.EqualValues(t, 2, logins.Load())
}

func TestOAuthRefreshAndRestart(t *testing.T) {
	var exchanges, logins atomic.Int32
	var deviceID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
		case "/token":
			assert.Empty(t, r.Header.Get("X-deviceid"), "storage identity must not reach the OAuth provider")
			require.NoError(t, r.ParseForm())
			assert.Equal(t, "refresh_token", r.PostForm.Get("grant_type"))
			assert.Equal(t, "refresh-old", r.PostForm.Get("refresh_token"))
			id, secret, ok := r.BasicAuth()
			assert.True(t, ok)
			assert.Equal(t, "client", id)
			assert.Equal(t, "secret", secret)
			exchanges.Add(1)
			jsonReply(t, w, map[string]any{"access_token": "access-new", "refresh_token": "refresh-new", "expires_in": 3600, "token_type": "Bearer"})
		case "/sapi/login/oauth":
			assert.Equal(t, "true", r.URL.Query().Get("responsetime"))
			assert.NotEmpty(t, r.Header.Get("X-deviceid"))
			assert.True(t, strings.HasPrefix(r.Header.Get("X-deviceid"), "fol-"))
			if deviceID == "" {
				deviceID = r.Header.Get("X-deviceid")
			}
			assert.Equal(t, deviceID, r.Header.Get("X-deviceid"))
			assert.Equal(t, "application/x-www-form-urlencoded; charset=UTF-8", r.Header.Get("Content-Type"))
			require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), oauthPrefix))
			b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Authorization"), oauthPrefix))
			require.NoError(t, err)
			var envelope struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(b, &envelope))
			n := logins.Add(1)
			if n == 1 {
				assert.Equal(t, "access-new", envelope.Data["accesstoken"])
				assert.Equal(t, "refresh-new", envelope.Data["refreshtoken"])
			} else {
				assert.Equal(t, "access-rotated", envelope.Data["accesstoken"])
				assert.Equal(t, "refresh-rotated", envelope.Data["refreshtoken"])
				assert.Equal(t, "provider-value", envelope.Data["extension"])
			}
			assert.Equal(t, "windows", envelope.Data["platform"])
			rotated := `{"data":{"accesstoken":"access-rotated","refreshtoken":"refresh-rotated","expiresin":3600,"extension":"provider-value"}}`
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(rotated)))
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "token_url": srv.URL + "/token", "client_id": "client", "client_secret": obscure.MustObscure("secret")})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access-old", RefreshToken: "refresh-old", Expiry: time.Now().Add(-time.Hour)}, false))
	for range 2 {
		_, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, exchanges.Load())
	assert.EqualValues(t, 1, logins.Load())
	m[sessionKey] = ""
	_, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 2, logins.Load())
	assert.NotEmpty(t, m["device_id"])
	assert.Equal(t, deviceID, m["device_id"])
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "refresh-rotated", token.RefreshToken)
	assert.True(t, token.Valid())
}

func TestSessionScope(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/system/information"):
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		case strings.HasSuffix(r.URL.Path, "/login"):
			logins.Add(1)
			assert.Empty(t, r.Header.Get("Cookie"))
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	for range 2 {
		_, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, logins.Load())
	require.NotEmpty(t, m[sessionKey])
	for key, value := range map[string]string{
		"url": srv.URL + "/tenant", "api_path": "/other", "user": "other",
		"password": obscure.MustObscure("other"), "device_id": "other",
		sessionKey: "invalid JSON",
	} {
		t.Run(key, func(t *testing.T) {
			changed := maps.Clone(m)
			changed[key] = value
			before := logins.Load()
			_, err := NewFs(context.Background(), "test", "", changed)
			require.NoError(t, err)
			assert.Equal(t, before+1, logins.Load())
		})
	}
}

func TestConfigOAuth(t *testing.T) {
	var verifier string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "authorization_code", r.PostForm.Get("grant_type"))
		assert.Equal(t, "test-code", r.PostForm.Get("code"))
		assert.Equal(t, verifier, r.PostForm.Get("code_verifier"))
		jsonReply(t, w, map[string]any{"access_token": "access", "refresh_token": "refresh", "expires_in": 3600, "token_type": "Bearer"})
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_url": srv.URL + "/authorize", "token_url": srv.URL + "/token", "redirect_url": srv.URL + "/callback", "client_id": "client"})
	ctx := context.Background()
	out, err := configure(ctx, "test", m, fs.ConfigIn{State: "authorize"})
	require.NoError(t, err)
	loginURL, err := url.Parse(strings.Split(out.Option.Help, "\n\n")[1])
	require.NoError(t, err)
	verifier = m[authVerifierKey]
	assert.Equal(t, "S256", loginURL.Query().Get("code_challenge_method"))
	assert.Equal(t, "offline", loginURL.Query().Get("access_type"))
	assert.NotEmpty(t, loginURL.Query().Get("code_challenge"))
	callback := srv.URL + "/callback?code=test-code&state=" + url.QueryEscape(m[authStateKey])
	_, err = configure(ctx, "test", m, fs.ConfigIn{State: "exchange", Result: strings.Replace(callback, "state=", "state=wrong", 1)})
	require.ErrorContains(t, err, "state mismatch")
	_, err = configure(ctx, "test", m, fs.ConfigIn{State: "exchange", Result: callback})
	require.NoError(t, err)
	assert.Empty(t, m[authStateKey])
	assert.Empty(t, m[authVerifierKey])
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "refresh", token.RefreshToken)
}

func TestProviderOAuth(t *testing.T) {
	for _, tc := range []struct {
		name, server, authURL, tokenURL, scope, param, value string
	}{
		{"Spain", "https://cloud.o2online.es", "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/authorize", "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/token", "openid", "acr_values", "2"},
		{"Germany", "https://cloud.o2.de", "https://mondia-lcm.o2online.de/v2/web/auth/dialog/oauth", "https://police.mondiamedia.com/v2/api/auth/token", "", "client_type", "omh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := testConfig(t, configmap.Simple{"url": tc.server + "/", "client_id": "registered-client"})
			opt, err := readOptions(m)
			require.NoError(t, err)
			c, err := opt.oauthConfig()
			require.NoError(t, err)
			assert.Equal(t, tc.authURL, c.Endpoint.AuthURL)
			assert.Equal(t, tc.tokenURL, c.Endpoint.TokenURL)
			assert.Equal(t, tc.server+"/ui/html/clientoauth.html", c.RedirectURL)
			assert.Equal(t, tc.scope, strings.Join(c.Scopes, " "))
			out, err := configure(context.Background(), "test", m, fs.ConfigIn{State: "authorize"})
			require.NoError(t, err)
			u, err := url.Parse(strings.Split(out.Option.Help, "\n\n")[1])
			require.NoError(t, err)
			assert.Equal(t, tc.value, u.Query().Get(tc.param))
			assert.Equal(t, "registered-client", u.Query().Get("client_id"))
			assert.Equal(t, tc.scope, u.Query().Get("scope"))
			assert.Equal(t, "S256", u.Query().Get("code_challenge_method"))

			opt.AuthURL, opt.TokenURL, opt.RedirectURL = "https://custom/auth", "https://custom/token", "https://custom/callback"
			opt.Scope = "custom offline"
			c, err = opt.oauthConfig()
			require.NoError(t, err)
			assert.Equal(t, opt.AuthURL, c.Endpoint.AuthURL)
			assert.Equal(t, opt.TokenURL, c.Endpoint.TokenURL)
			assert.Equal(t, opt.RedirectURL, c.RedirectURL)
			assert.Equal(t, []string{"custom", "offline"}, c.Scopes)
		})
	}
}

func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{RemoteName: "TestOneMediaHub:", NilObject: (*Object)(nil)})
}

type fixtureMode int

const (
	specMode fixtureMode = iota
	o2Mode
)

// fixture models SAPI 14.5, with separately enabled O2 compatibility behavior.
type fixture struct {
	mu            sync.Mutex
	mode          fixtureMode
	pendingReads  int
	folders       []api.Folder
	media         []api.Media
	content       map[api.ID]string
	nextID        int
	server        *httptest.Server
	changes       map[string]api.Changes
	requestTime   int64
	requests      map[string]int
	failMedia     bool
	changesFrom   []string
	mediaBatches  []int
	folderBatches [][]api.ID
	failFolders   bool
	accountID     string
	changeData    any
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fx := &fixture{folders: []api.Folder{}, media: []api.Media{}, content: map[api.ID]string{}, nextID: 1}
	fx.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fx.serve(t, w, r) }))
	t.Cleanup(fx.server.Close)
	return fx
}

func (fx *fixture) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	if fx.requests == nil {
		fx.requests = map[string]int{}
	}
	fx.requests[r.URL.Path]++
	if fx.mode == o2Mode && r.URL.Query().Get("offset") == "0" {
		jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1021", "message": "Invalid offset"}})
		return
	}

	if r.URL.Path == "/sapi/system/information" {
		assert.Equal(t, http.MethodGet, r.Method)
		jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/content/") {
		assert.Empty(t, r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("Cookie"))
		assert.Empty(t, r.URL.Query().Get("validationkey"))
		id := api.ID(strings.TrimPrefix(r.URL.Path, "/content/"))
		http.ServeContent(w, r, "content", time.Time{}, strings.NewReader(fx.content[id]))
		return
	}
	if r.URL.Path == "/sapi/login" {
		assert.Equal(t, http.MethodPost, r.Method)
		jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		return
	}
	assert.Equal(t, "key", r.URL.Query().Get("validationkey"))
	cookie, err := r.Cookie("JSESSIONID")
	require.NoError(t, err)
	assert.Equal(t, "session", cookie.Value)
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	action := r.URL.Query().Get("action")
	switch {
	case r.URL.Path == "/sapi/profile":
		id := fx.accountID
		if id == "" {
			id = "test-account"
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"user": map[string]any{"generic": map[string]string{"userid": id}}}})
	case r.URL.Path == "/sapi/profile/changes":
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "folder,file,picture,video,audio", r.URL.Query().Get("type"))
		assert.Equal(t, "true", r.URL.Query().Get("responsetime"))
		assert.Equal(t, "true", r.URL.Query().Get("locked"))
		assert.Equal(t, "creationdate", r.URL.Query().Get("sortby"))
		assert.Equal(t, "descending", r.URL.Query().Get("sortorder"))
		_, err := time.Parse(dateFormat, r.URL.Query().Get("from"))
		assert.NoError(t, err)
		fx.changesFrom = append(fx.changesFrom, r.URL.Query().Get("from"))
		data := fx.changeData
		if data == nil {
			data = fx.changes
		}
		jsonReply(t, w, map[string]any{"data": data, "requesttime": strconv.FormatInt(fx.requestTime, 10)})
	case r.URL.Path == "/sapi/media/folder" && action == "get":
		if fx.failFolders {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "failed folders"}})
			return
		}
		if r.Method == http.MethodPost {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
			var data struct {
				IDs []api.ID `json:"ids"`
			}
			require.NoError(t, json.Unmarshal(envelope.Data, &data))
			fx.folderBatches = append(fx.folderBatches, data.IDs)
			var folders []api.Folder
			for _, folder := range fx.folders {
				if slices.Contains(data.IDs, folder.ID) {
					folders = append(folders, folder)
				}
			}
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": folders}})
			return
		}
		assert.Equal(t, http.MethodGet, r.Method)
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		end := min(offset+limit, len(fx.folders))
		jsonReply(t, w, map[string]any{"data": map[string]any{"folders": fx.folders[min(offset, end):end]}})
	case r.URL.Path == "/sapi/media/folder" && action == "save":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		// §5.21 requires a name and numeric parent ID, without an ID on creation.
		var data struct {
			ID       json.RawMessage `json:"id"`
			Name     string          `json:"name"`
			ParentID *int64          `json:"parentid"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Empty(t, data.ID)
		require.NotEmpty(t, data.Name)
		folder := api.Folder{Name: data.Name, Status: "U", Date: time.Now().UnixMilli()}
		if data.ParentID != nil {
			folder.ParentID = api.ID(strconv.FormatInt(*data.ParentID, 10))
		}
		folder.ID = api.ID(strconv.Itoa(fx.nextID))
		fx.nextID++
		fx.folders = append(fx.folders, folder)
		jsonReply(t, w, map[string]any{"id": folder.ID, "success": "Folder saved successfully"})
	case r.URL.Path == "/sapi/media/folder" && action == "delete":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Folders []int64 `json:"folders"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.Len(t, data.Folders, 1)
		for i, folder := range fx.folders {
			if string(folder.ID) == strconv.FormatInt(data.Folders[0], 10) {
				fx.folders = append(fx.folders[:i], fx.folders[i+1:]...)
				break
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get":
		if fx.failMedia {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "failed metadata"}})
			return
		}
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			IDs    []int64  `json:"ids"`
			Fields []string `json:"fields"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Subset(t, data.Fields, []string{"name", "size", "modificationdate", "url", "folderid"})
		fx.mediaBatches = append(fx.mediaBatches, len(data.IDs))
		items := fx.media
		more := false
		if data.IDs != nil {
			assert.NotContains(t, r.URL.Query(), "limit", "§3.5.48 forbids pagination with IDs")
			assert.NotContains(t, r.URL.Query(), "offset")
			items = []api.Media{}
			if fx.pendingReads > 0 {
				fx.pendingReads--
				jsonReply(t, w, map[string]any{"data": map[string]any{"media": []api.Media{}}})
				return
			}
			for _, item := range fx.media {
				for _, id := range data.IDs {
					if strconv.FormatInt(id, 10) == string(item.ID) {
						items = append(items, item)
					}
				}
			}
		} else {
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			end := min(offset+limit, len(items))
			more = end < len(items)
			items = items[min(offset, end):end]
		}
		payload := map[string]any{"media": items}
		result := map[string]any{"data": payload}
		if data.IDs == nil {
			if fx.mode == o2Mode {
				payload["more"] = more
			} else {
				result["more"] = more
			}
		}
		jsonReply(t, w, result)
	case (r.URL.Path == "/sapi/upload" || r.URL.Path == "/sapi/upload/file") && action == "save":
		assert.Equal(t, http.MethodPost, r.Method)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, "invalid upload", http.StatusBadRequest)
			return
		}
		defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
		require.NoError(t, json.Unmarshal([]byte(r.FormValue("data")), &envelope))
		// §5.59 uses a string media ID, numeric folder ID/size and RFC 2445 dates.
		var data struct {
			ID          string `json:"id"`
			FolderID    *int64 `json:"folderid"`
			Name        string `json:"name"`
			Size        int64  `json:"size"`
			ContentType string `json:"contenttype"`
			Created     string `json:"creationdate"`
			Modified    string `json:"modificationdate"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.NotEmpty(t, data.Name)
		require.NotEmpty(t, data.ContentType)
		_, err := time.Parse("20060102T150405Z", data.Created)
		require.NoError(t, err)
		if fx.mode == o2Mode && strings.HasPrefix(data.Name, ".") {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "Missing name"}})
			return
		}
		file, header, err := r.FormFile("file")
		require.NoError(t, err)
		content, err := io.ReadAll(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		if int64(len(content)) != data.Size {
			http.Error(w, "incorrect upload size", http.StatusBadRequest)
			return
		}
		assert.Equal(t, data.Name, header.Filename)
		id := api.ID(data.ID)
		if id == "" {
			assert.Equal(t, "/sapi/upload", r.URL.Path)
			id = api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
		} else {
			assert.Equal(t, "/sapi/upload/file", r.URL.Path)
		}
		modified, err := time.Parse("20060102T150405Z", data.Modified)
		require.NoError(t, err)
		name := data.Name
		if fx.mode == o2Mode {
			name = strings.TrimRight(name, ".")
		}
		item := api.Media{ID: id, Name: name, Size: data.Size, Modified: modified.UnixMilli(), URL: fx.server.URL + "/content/" + string(id), Type: "file", Status: "U"}
		if data.FolderID != nil {
			item.FolderID = api.ID(strconv.FormatInt(*data.FolderID, 10))
		}
		index := len(fx.media)
		for i, existing := range fx.media {
			if existing.ID == id {
				index = i
				break
			}
		}
		if index == len(fx.media) {
			fx.media = append(fx.media, item)
		} else {
			fx.media[index] = item
		}
		fx.content[id] = string(content)
		jsonReply(t, w, map[string]any{"id": id, "success": "Media uploaded successfully"})
	case r.URL.Path == "/sapi/media/file" && action == "delete":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Files []int64 `json:"files"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
		for _, id := range data.Files {
			for i, item := range fx.media {
				if string(item.ID) == strconv.FormatInt(id, 10) {
					fx.media = append(fx.media[:i], fx.media[i+1:]...)
					delete(fx.content, item.ID)
					break
				}
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get-storage-space":
		assert.Equal(t, http.MethodGet, r.Method)
		jsonReply(t, w, map[string]any{"data": map[string]int64{"quota": 10000, "free": 9000}})
	default:
		t.Errorf("unexpected %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (fx *fixture) config(t *testing.T) configmap.Simple {
	return testConfig(t, configmap.Simple{"url": fx.server.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
}

func TestFileOperations(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()
	m := fx.config(t)
	remote, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	require.NoError(t, f.Mkdir(ctx, "parent/empty"))
	modified := time.Date(2023, 4, 5, 6, 7, 8, 0, time.UTC)
	src := object.NewStaticObjectInfo("parent/file.txt", modified, 5, true, nil, f)
	obj, err := f.Put(ctx, strings.NewReader("hello"), src)
	require.NoError(t, err)
	assert.True(t, modified.Equal(obj.ModTime(ctx)))
	assert.EqualValues(t, 5, obj.Size())
	entries, err := f.List(ctx, "parent")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.ErrorIs(t, f.Rmdir(ctx, "parent"), fs.ErrorDirectoryNotEmpty)
	body, err := obj.Open(ctx, &fs.RangeOption{Start: 1, End: 3})
	require.NoError(t, err)
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	assert.Equal(t, "ell", string(content))
	oldID := obj.(*Object).ID()
	src = object.NewStaticObjectInfo("parent/file.txt", modified, 3, true, nil, f)
	obj, err = f.Put(ctx, strings.NewReader("new"), src)
	require.NoError(t, err)
	assert.Equal(t, oldID, obj.(*Object).ID())
	fileRoot, err := NewFs(ctx, "test", "parent/file.txt", m)
	require.ErrorIs(t, err, fs.ErrorIsFile)
	assert.Equal(t, "parent", fileRoot.Root())
	missing, err := NewFs(ctx, "test", "new/deep/root", m)
	require.NoError(t, err)
	require.NoError(t, missing.Mkdir(ctx, ""))
	usage, err := f.About(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1000, *usage.Used)
	require.NoError(t, obj.Remove(ctx))
	_, err = f.NewObject(ctx, "parent/file.txt")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	require.NoError(t, f.Rmdir(ctx, "parent/empty"))
	require.NoError(t, f.Rmdir(ctx, "parent"))
}

func TestUploadVisibility(t *testing.T) {
	fx := newFixture(t)
	fx.pendingReads = 1
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
	obj, err := f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	assert.EqualValues(t, 1, obj.Size())
}

func TestAsyncUpload(t *testing.T) {
	for _, size := range []int{0, 5} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			fx := newFixture(t)
			content := strings.Repeat("x", size)
			modified := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			var stages []string
			var metadata api.Upload
			var polls int
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.URL.Query().Get("action")
				if r.URL.Path == "/sapi/upload/file" {
					assert.Equal(t, http.MethodPost, r.Method)
					assert.Equal(t, "key", r.URL.Query().Get("validationkey"))
					stages = append(stages, action)
					if action == "save-metadata" {
						assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
						var request struct {
							Data api.Upload `json:"data"`
						}
						require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
						metadata = request.Data
						assert.Equal(t, "file.txt", metadata.Name)
						assert.EqualValues(t, size, metadata.Size)
						assert.Equal(t, modified.Format(dateFormat), metadata.Modified)
						jsonReply(t, w, map[string]any{"id": "42", "status": "U"})
						return
					}
					assert.Equal(t, "save", action)
					assert.Equal(t, "true", r.URL.Query().Get("acceptasynchronous"))
					assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
					assert.Equal(t, strconv.Itoa(size), r.Header.Get("X-funambol-file-size"))
					assert.EqualValues(t, size, r.ContentLength)
					assert.Equal(t, metadata.ContentType, r.Header.Get("Content-Type"))
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, content, string(body))
					fx.mu.Lock()
					fx.media = []api.Media{{ID: "42", Name: "file.txt", Size: int64(size), Modified: modified.UnixMilli(), Type: "file", Status: "U"}}
					fx.content["42"] = content
					fx.mu.Unlock()
					w.WriteHeader(http.StatusAccepted)
					jsonReply(t, w, map[string]any{"id": 42, "status": "A"})
					return
				}
				if action == "get-validation-status" {
					assert.Equal(t, "/sapi/media", r.URL.Path)
					assert.Equal(t, http.MethodPost, r.Method)
					var request struct {
						Data struct {
							IDs []struct {
								ID       string `json:"id"`
								FolderID string `json:"folder_id"`
							} `json:"ids"`
						} `json:"data"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					require.Len(t, request.Data.IDs, 1)
					assert.Equal(t, "42", request.Data.IDs[0].ID)
					polls++
					status := "V"
					if polls == 1 {
						status = "U"
					}
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]any{{"id": "42", "status": status}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			ctx := context.Background()
			f, err := NewFs(ctx, "test", "", m)
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("file.txt", modified, int64(size), true, nil, f)
			obj, err := f.Put(ctx, io.NopCloser(strings.NewReader(content)), src)
			require.NoError(t, err)
			assert.EqualValues(t, size, obj.Size())
			assert.True(t, modified.Equal(obj.ModTime(ctx)))
			assert.Equal(t, []string{"save-metadata", "save"}, stages)
			assert.Equal(t, 2, polls)
			stages = nil
			polls = 0
			require.NoError(t, obj.Update(ctx, strings.NewReader(content), src))
			assert.Equal(t, "42", metadata.ID)
			assert.Equal(t, []string{"save-metadata", "save"}, stages)
		})
	}
}

func TestAsyncUploadTimeoutOption(t *testing.T) {
	for _, timeout := range []string{"0s", "-1s"} {
		m := testConfig(t, configmap.Simple{"async_upload": "true", "upload_timeout": timeout})
		_, err := readOptions(m)
		require.ErrorContains(t, err, "upload_timeout must be positive")
	}
}

func TestAsyncUploadsParallel(t *testing.T) {
	fx := newFixture(t)
	var arrived atomic.Int32
	var validationCalls atomic.Int32
	ready := make(chan struct{})
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/sapi/upload/file", r.URL.Path)
		switch r.URL.Query().Get("action") {
		case "save-metadata":
			var request struct {
				Data api.Upload `json:"data"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			fx.mu.Lock()
			id := api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
			fx.media = append(fx.media, api.Media{ID: id, Name: request.Data.Name, Size: request.Data.Size, Type: "file"})
			fx.mu.Unlock()
			jsonReply(t, w, map[string]any{"id": id})
		case "save":
			if arrived.Add(1) == 2 {
				close(ready)
			}
			select {
			case <-ready:
			case <-r.Context().Done():
				return
			}
			content, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			fx.mu.Lock()
			fx.content[api.ID(r.Header.Get("X-funambol-id"))] = string(content)
			fx.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected upload action %s", r.URL.Query().Get("action"))
		}
	}))
	defer upload.Close()
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
			return
		}
		var request struct {
			Data struct {
				IDs []struct {
					ID string `json:"id"`
				} `json:"ids"`
			} `json:"data"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		validationCalls.Add(1)
		require.Len(t, request.Data.IDs, 2)
		var ids []map[string]string
		for _, item := range request.Data.IDs {
			ids = append(ids, map[string]string{"id": item.ID, "status": "V"})
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"ids": ids}})
	})
	m := fx.config(t)
	m["async_upload"], m["upload_url"] = "true", upload.URL
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.(*Fs).Shutdown(context.Background())) })
	errors := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		go func() {
			src := object.NewStaticObjectInfo(name+".txt", time.Now(), int64(len(name)), true, nil, f)
			_, err := f.Put(ctx, strings.NewReader(name), src)
			errors <- err
		}()
	}
	for range 2 {
		require.NoError(t, <-errors)
	}
	assert.EqualValues(t, 2, arrived.Load())
	assert.EqualValues(t, 1, validationCalls.Load())
	fx.mu.Lock()
	defer fx.mu.Unlock()
	for _, item := range fx.media {
		assert.Equal(t, strings.TrimSuffix(item.Name, ".txt"), fx.content[item.ID])
	}
}

func TestAsyncUploadErrors(t *testing.T) {
	for _, tc := range []struct {
		name, metadata, content, status, want string
	}{
		{"metadata failure", `{"error":{"code":"COM-1011","message":"invalid metadata"}}`, "", "", "register upload metadata: COM-1011"},
		{"missing ID", `{}`, "", "", "no media ID"},
		{"wrong metadata ID", `{"id":43}`, "", "", "expected 42"},
		{"content failure", `{"id":42}`, `{"error":{"code":"MED-1000","message":"upload failed"}}`, "", "upload media 42: MED-1000"},
		{"wrong content ID", `{"id":42}`, `{"id":43}`, "", "expected 42"},
		{"processing failure", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"F"}]}}`, `returned status "F"`},
		{"invalid status response", `{"id":42}`, `{}`, `{"data":[]}`, "decode upload processing status"},
		{"empty status", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":""}]}}`, `returned status ""`},
		{"duplicate status", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"V"},{"id":42,"status":"F"}]}}`, "duplicate upload processing status"},
		{"unexpected status ID", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":43,"status":"V"}]}}`, "unexpected upload processing ID"},
		{"accepted remains pending", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"A"}]}}`, "context deadline exceeded"},
		{"missing status remains pending", `{"id":42}`, `{}`, `{"data":{"ids":[]}}`, "context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var metadataCalls, contentCalls, statusCalls int
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body string
				switch r.URL.Query().Get("action") {
				case "save-metadata":
					metadataCalls++
					body = tc.metadata
				case "save":
					contentCalls++
					body = tc.content
				case "get-validation-status":
					statusCalls++
					body = tc.status
				default:
					handler.ServeHTTP(w, r)
					return
				}
				_, err := io.WriteString(w, body)
				assert.NoError(t, err)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			m["upload_timeout"] = "50ms"
			ctx := context.Background()
			f, err := NewFs(ctx, "test", "", m)
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
			if tc.name == "wrong metadata ID" {
				obj := &Object{fs: f.(*Fs), remote: "file", info: api.Media{ID: "42"}}
				err = obj.Update(ctx, strings.NewReader("x"), src)
				assert.Equal(t, "42", obj.ID())
			} else {
				_, err = f.Put(ctx, strings.NewReader("x"), src)
			}
			require.ErrorContains(t, err, tc.want)
			assert.Equal(t, 1, metadataCalls)
			if tc.content != "" {
				assert.Equal(t, 1, contentCalls, "raw uploads must not be replayed")
			} else {
				assert.Zero(t, contentCalls)
			}
			if tc.status != "" {
				assert.Equal(t, 1, statusCalls)
			} else {
				assert.Zero(t, statusCalls)
			}
		})
	}
}

func TestValidationCancellation(t *testing.T) {
	fx := newFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
			return
		}
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "1", "status": "V"}, {"id": "2", "status": "V"}}}})
	})
	m := fx.config(t)
	m["async_upload"] = "true"
	remote, err := NewFs(context.Background(), "cancellation", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(ctx, "") }()
	go func() { second <- (&Object{fs: f, info: api.Media{ID: "2"}}).waitUpload(context.Background(), "") }()
	<-started
	cancel()
	select {
	case err := <-first:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled upload blocked on another upload")
	}
	close(release)
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("one cancellation stopped the shared request")
	}
}

func TestValidationShutdown(t *testing.T) {
	fx := newFixture(t)
	started := make(chan struct{})
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
			return
		}
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		close(started)
		<-r.Context().Done()
	})
	m := fx.config(t)
	m["async_upload"] = "true"
	remote, err := NewFs(context.Background(), "shutdown", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	done := make(chan error, 1)
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(context.Background(), "") }()
	<-started
	require.NoError(t, f.Shutdown(context.Background()))
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("shutdown left an upload waiting")
	}
}

func TestO2RootLayout(t *testing.T) {
	fx := newFixture(t)
	fx.folders = []api.Folder{
		{ID: "1", Name: "/"},
		{ID: "2", ParentID: "1", Name: "Backups"},
		{ID: "4", Name: "Other"},
	}
	fx.media = []api.Media{{ID: "3", FolderID: "1", Name: "root.txt"}}
	ctx := context.Background()
	m := fx.config(t)
	nested, err := NewFs(ctx, "test", "／", m)
	require.NoError(t, err)
	entries, err := nested.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"／", "Other"}, names)
	entries, err = f.List(ctx, "／")
	require.NoError(t, err)
	names = nil
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"／/Backups", "／/root.txt"}, names)
	_, err = f.NewObject(ctx, "／/root.txt")
	require.NoError(t, err)
	require.NoError(t, f.Mkdir(ctx, "／/new"))
	assert.Equal(t, api.ID("1"), fx.folders[len(fx.folders)-1].ParentID)

	m["root_folder_id"] = "2"
	f, err = NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestPagination(t *testing.T) {
	for name, mode := range map[string]fixtureMode{"Spec": specMode, "O2": o2Mode} {
		t.Run(name, func(t *testing.T) { testPagination(t, mode) })
	}
}

func testPagination(t *testing.T, mode fixtureMode) {
	fx := newFixture(t)
	fx.mode = mode
	for i := range pageSize + 1 {
		id := api.ID(strconv.Itoa(i + 1))
		fx.folders = append(fx.folders, api.Folder{ID: id, Name: fmt.Sprintf("folder%d", i)})
		fx.media = append(fx.media, api.Media{ID: id, Name: fmt.Sprintf("file%d", i), Type: "file", Status: "U"})
	}
	if mode == o2Mode {
		// O2 repeats the root folder at the end of its last page.
		fx.folders = append(fx.folders, fx.folders[0])
	}
	f, err := NewFs(context.Background(), "test", "", fx.config(t))
	require.NoError(t, err)
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 2*(pageSize+1))
	_, err = f.NewObject(context.Background(), fmt.Sprintf("file%d", pageSize))
	require.NoError(t, err)
}

func TestO2Filenames(t *testing.T) {
	fx := newFixture(t)
	fx.mode = o2Mode
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	for _, name := range []string{".hidden", "trailing."} {
		t.Run(name, func(t *testing.T) {
			src := object.NewStaticObjectInfo(name, time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
			require.NoError(t, err)
			_, err = f.NewObject(ctx, name)
			require.NoError(t, err)
		})
	}
}

// SAPI 14.5 §§3.5.25, 3.5.48 and 5.21: literal wire examples, independent of api types.
func TestSpecListing(t *testing.T) {
	fx := newFixture(t)
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/media/folder":
			jsonReply(t, w, json.RawMessage(`{"data":{"folders":[
				{"id":2500,"name":"onemediahub","magic":true,"status":"U","date":1231323524524},
				{"id":"2501","name":"pictures","parentid":2500,"status":"U","date":1231323524524},
				{"id":2502,"name":"Other","status":"U","date":1231323524524}
			]}}`))
		case "/sapi/media":
			assert.Equal(t, http.MethodPost, r.Method)
			pages.Add(1)
			if r.URL.Query().Get("offset") == "" {
				jsonReply(t, w, json.RawMessage(`{"data":{"media":[
					{"id":"0","name":"unfiled.txt","mediatype":"file","status":"U","date":1399985134672,"modificationdate":1161931347000,"size":5}
				]},"more":true}`))
				return
			}
			assert.Equal(t, "1", r.URL.Query().Get("offset"))
			jsonReply(t, w, json.RawMessage(`{"data":{"media":[
				{"id":"7","name":"100_1924.jpg","folder":2501,"mediatype":"picture","status":"U","date":1399985134672,"modificationdate":1161931347000,"size":722942}
			]},"more":false}`))
		default:
			fx.serve(t, w, r)
		}
	}))
	defer srv.Close()
	m := fx.config(t)
	m["url"] = srv.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"onemediahub", "Other", "unfiled.txt"}, names)
	entries, err = f.List(ctx, "onemediahub/pictures")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "onemediahub/pictures/100_1924.jpg", entries[0].Remote())
	assert.EqualValues(t, 722942, entries[0].Size())
	assert.Equal(t, time.UnixMilli(1161931347000), entries[0].ModTime(ctx))
	assert.EqualValues(t, 4, pages.Load())
}

// SAPI 14.5 §§3.5.3, 3.5.9, 3.5.14 and 3.5.18 use type-specific numeric ID arrays.
func TestSpecDelete(t *testing.T) {
	for _, mediaType := range []string{"picture", "video", "audio", "file"} {
		t.Run(mediaType, func(t *testing.T) {
			fx := newFixture(t)
			var deletes atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") != "delete" {
					fx.serve(t, w, r)
					return
				}
				deletes.Add(1)
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/sapi/media/"+mediaType, r.URL.Path)
				assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
				b, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, `{"data":{"`+mediaType+`s":[23508]}}`, string(b))
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			m := fx.config(t)
			m["url"] = srv.URL
			f, err := NewFs(context.Background(), "test", "", m)
			require.NoError(t, err)
			obj := &Object{fs: f.(*Fs), info: api.Media{ID: "23508", Type: mediaType}}
			require.NoError(t, obj.Remove(context.Background()))
			assert.EqualValues(t, 1, deletes.Load())
		})
	}
}

func TestProtocolIntegration(t *testing.T) {
	fx := newFixture(t)
	t.Setenv("RCLONE_ONEMEDIAHUB_URL", fx.server.URL)
	t.Setenv("RCLONE_ONEMEDIAHUB_AUTH_TYPE", authPassword)
	t.Setenv("RCLONE_ONEMEDIAHUB_USER", "test")
	t.Setenv("RCLONE_ONEMEDIAHUB_PASSWORD", obscure.MustObscure("test"))
	fstests.Run(t, &fstests.Opt{
		RemoteName: ":onemediahub:", NilObject: (*Object)(nil), QuickTestOK: true,
	})
}

// O2 clients send OAuth credentials on SAPI requests to receive refreshed tokens.
func TestOAuthRequestHeaders(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/system/information" {
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
			return
		}
		header := r.Header.Get("Authorization")
		assert.Equal(t, "configured-device", r.Header.Get("X-deviceid"))
		assert.True(t, strings.HasPrefix(header, oauthPrefix), "desktop OAuth is required on each SAPI request")
		switch r.URL.Path {
		case "/sapi/login/oauth":
			logins.Add(1)
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/media/folder":
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"rotated","refreshtoken":"rotated-refresh","expiresin":3600}}`)))
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": []any{}}})
		case "/sapi/media":
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []any{}}})
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "device_id": "configured-device"})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}, false))
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.NoError(t, err)
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "rotated-refresh", token.RefreshToken)
	_, err = NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 1, logins.Load())
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "other", RefreshToken: "other-refresh"}, false))
	_, err = NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 2, logins.Load())
}

// SAPI 14.5 §§4.1.3 and 6.3.2: validation keys depend on server configuration.
func TestSessionWithoutKey(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, json.RawMessage(`{"sapiversion":"14.5"}`))
		case "/sapi/login":
			logins.Add(1)
			jsonReply(t, w, json.RawMessage(`{"data":{"jsessionid":"session","roles":[]}}`))
		case "/sapi/media":
			assert.NotContains(t, r.URL.Query(), "validationkey")
			cookie, err := r.Cookie("JSESSIONID")
			if assert.NoError(t, err) {
				assert.Equal(t, "session", cookie.Value)
			}
			jsonReply(t, w, json.RawMessage(`{"data":{"quota":1024000,"free":534123}}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	for range 2 {
		f, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
		_, err = f.(*Fs).About(context.Background())
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, logins.Load())
}

// The Zefiro APK uses the native fac- device prefix; fol- logins are rejected.
func TestPasswordDeviceID(t *testing.T) {
	fx := newFixture(t)
	var logins atomic.Int32
	var expired atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/login":
			if !strings.HasPrefix(r.Header.Get("X-deviceid"), "fac-") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			logins.Add(1)
		case "/sapi/media/folder":
			if expired.Swap(false) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	m := fx.config(t)
	m["url"] = srv.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	device := m["device_id"]
	require.NotEmpty(t, device)
	expired.Store(true)
	_, err = f.List(ctx, "")
	require.NoError(t, err)
	_, err = NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	assert.Equal(t, device, m["device_id"])
	assert.EqualValues(t, 2, logins.Load())
}

func TestEmptyDownload(t *testing.T) {
	fx := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/empty" {
			// Zefiro accepts zero-byte uploads but returns 403 from their content URL.
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "empty", Size: 0, Type: "file", URL: srv.URL + "/empty"}}
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	obj, err := f.NewObject(ctx, "empty")
	require.NoError(t, err)
	body, err := obj.Open(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, body.Close()) }()
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Empty(t, content)
}

func TestUploadDiscovery(t *testing.T) {
	fx := newFixture(t)
	uploaded := false
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploaded = true
		assert.Equal(t, "/sapi/upload", r.URL.Path)
		fx.serve(t, w, r)
	}))
	defer upload.Close()
	main := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/system/information" {
			assert.Empty(t, r.Header.Get("Cookie"))
			assert.Empty(t, r.Header.Get("Authorization"))
			jsonReply(t, w, map[string]string{"sapi.upload.endpoint": upload.URL})
			return
		}
		assert.False(t, strings.HasPrefix(r.URL.Path, "/sapi/upload"))
		fx.serve(t, w, r)
	}))
	defer main.Close()
	m := fx.config(t)
	m["url"] = main.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
	_, err = f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	assert.True(t, uploaded)
}

func TestAPIErrors(t *testing.T) {
	var reads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		case "/sapi/login":
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/media/folder":
			if reads.Add(1) == 1 {
				jsonReply(t, w, map[string]any{"error": map[string]string{"code": invalidKeyCode, "message": "Invalid mandatory validation key"}})
				return
			}
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "Invalid request"}})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.ErrorContains(t, err, "COM-1011")
	assert.EqualValues(t, 2, reads.Load())
}

func TestHTTPErrorContext(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://cloud.example.com/sapi/upload?action=save&validationkey=secret", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		request *http.Request
		body    string
		want    string
	}{
		{name: "Request", request: req, body: "<html>secret</html>", want: `OneMediaHub HTTP error: 403 Forbidden (POST cloud.example.com/sapi/upload action="save")`},
		{name: "NoRequest", body: "<html>secret</html>", want: "OneMediaHub HTTP error: 403 Forbidden"},
		{name: "APIError", request: req, body: `{"error":{"code":"COM-1011","message":"Invalid request"}}`, want: "COM-1011: Invalid request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Request: tc.request, Body: io.NopCloser(strings.NewReader(tc.body))}
			err := errorHandler(resp)
			assert.EqualError(t, err, tc.want)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestTokenResponse(t *testing.T) {
	m := configmap.Simple{}
	a := &auth{name: "test", m: m, token: &oauth2.Token{AccessToken: "old", RefreshToken: "keep"}}
	header := oauthPrefix + base64.RawStdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"new","expiresin":"3600","valid":"true"}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, "keep", a.token.RefreshToken)
	before := m["token"]
	for _, bad := range []string{"oauth invalid!", "Bearer access", oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{}}`))} {
		assert.Error(t, a.saveHeaderLocked(bad))
		assert.Equal(t, before, m["token"])
	}
	resp := &http.Response{Header: http.Header{"Authorization": {oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"stale","refreshtoken":"stale"}}`))}}}
	require.NoError(t, a.saveHeader(resp, "old"))
	assert.Equal(t, before, m["token"])
}

// Both O2 APKs preserve tokens when SAPI sends an empty replacement.
func TestTokenEmptyAccess(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	a := &auth{name: "test", m: configmap.Simple{}, token: &oauth2.Token{AccessToken: "keep", RefreshToken: "old", Expiry: expiry}}
	header := oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"","refreshtoken":"rotated","expiresin":"3600"}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, "keep", a.token.AccessToken)
	assert.Equal(t, "rotated", a.token.RefreshToken)
	assert.Equal(t, expiry, a.token.Expiry)

	a.token = nil
	require.ErrorContains(t, a.saveHeaderLocked(header), "no access token")
}

func TestMissingRootFolder(t *testing.T) {
	fx := newFixture(t)
	m := fx.config(t)
	m["root_folder_id"] = "99"
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.ErrorIs(t, err, fs.ErrorDirNotFound)
}

func TestDownloadRedirect(t *testing.T) {
	fx := newFixture(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-deviceid"))
		assert.Empty(t, r.Header.Get("Cookie"), "download redirects must not forward the SAPI session")
		assert.Empty(t, r.Header.Get("Referer"), "download redirects must not expose the validation key")
		assert.Empty(t, r.Header.Get("Authorization"))
		_, err := io.WriteString(w, "x")
		assert.NoError(t, err)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/download/file" {
			http.Redirect(w, r, target.URL, http.StatusFound)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: srv.URL + "/sapi/download/file"}}
	m := fx.config(t)
	m["url"] = srv.URL
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	body, err := obj.Open(context.Background())
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestTokenExpiryDoesNotSlide(t *testing.T) {
	expiry := time.Now().Add(time.Minute)
	a := &auth{name: "test", m: configmap.Simple{}, token: &oauth2.Token{AccessToken: "same", RefreshToken: "refresh", Expiry: expiry}}
	header := oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"same","expiresin":3600}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, expiry, a.token.Expiry, "echoed credentials must not extend the token's lifetime")
}

func TestConcurrentOpen(t *testing.T) {
	fx := newFixture(t)
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: fx.server.URL + "/content/1"}}
	fx.content["1"] = "x"
	f, err := NewFs(context.Background(), "test", "", fx.config(t))
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			_ = obj.Size()
		}
	}()
	defer func() { stop.Store(true); <-done }()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			body, err := obj.Open(context.Background())
			if !assert.NoError(t, err) {
				return
			}
			_, err = io.Copy(io.Discard, body)
			assert.NoError(t, err)
			assert.NoError(t, body.Close())
		})
	}
	wg.Wait()
}

func TestDownloadRenewal(t *testing.T) {
	fx := newFixture(t)
	var downloads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/login/oauth" {
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
			return
		}
		if r.URL.Path == "/sapi/download/file" {
			if downloads.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"download-token","refreshtoken":"download-refresh"}}`)))
			_, err := io.WriteString(w, "x")
			assert.NoError(t, err)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: srv.URL + "/sapi/download/file"}}
	m := testConfig(t, configmap.Simple{"url": srv.URL})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}, false))
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	body, err := obj.Open(context.Background())
	require.NoError(t, err)
	require.NoError(t, body.Close())
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "download-refresh", token.RefreshToken)
}

// SAPI 14.5 §4.1.1 returns JSON errors with HTTP 200, including on download APIs.
func TestDownloadJSON(t *testing.T) {
	const expired = `{"error":{"code":"SEC-1003","message":"Invalid mandatory validation key"}}`
	for _, tc := range []struct {
		name        string
		content     string
		disposition string
		err         string
	}{
		{name: "Renew", content: `{"value":"file content"}`},
		{name: "Error", content: `{"error":{"code":"COM-1005","message":"Unsupported operation"}}`, err: "COM-1005"},
		{name: "Attachment", content: expired, disposition: `attachment; filename="error.json"`},
		{name: "LargeJSON", content: `{"value":"` + strings.Repeat("x", 128*1024) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var downloads atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sapi/download/file" {
					fx.serve(t, w, r)
					return
				}
				assert.Empty(t, r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				if downloads.Add(1) == 1 {
					_, err := io.WriteString(w, expired)
					assert.NoError(t, err)
					return
				}
				w.Header().Set("Content-Disposition", tc.disposition)
				_, err := io.WriteString(w, tc.content)
				assert.NoError(t, err)
			}))
			defer srv.Close()
			m := fx.config(t)
			m["url"] = srv.URL
			f, err := NewFs(context.Background(), "test", "", m)
			require.NoError(t, err)
			u, err := url.Parse(srv.URL + "/sapi/download/file")
			require.NoError(t, err)
			body, err := f.(*Fs).open(context.Background(), u, nil)
			if body != nil {
				defer func() { assert.NoError(t, body.Close()) }()
			}
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				content, err := io.ReadAll(body)
				require.NoError(t, err)
				assert.Equal(t, tc.content, string(content))
			}
			assert.EqualValues(t, 2, downloads.Load())
		})
	}
}
