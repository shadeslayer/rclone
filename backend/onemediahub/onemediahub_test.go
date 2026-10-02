package onemediahub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
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
	remote, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
		case "/token":
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
	assert.EqualValues(t, 2, logins.Load())
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "refresh-rotated", token.RefreshToken)
	assert.True(t, token.Valid())
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

func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{RemoteName: "TestOneMediaHub:", NilObject: (*Object)(nil)})
}

// fixture implements the documented wire protocol and keeps binary data separately.
type fixture struct {
	mu      sync.Mutex
	folders []api.Folder
	media   []api.Media
	content map[api.ID]string
	nextID  int
	server  *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fx := &fixture{content: map[api.ID]string{}, nextID: 1}
	fx.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fx.serve(t, w, r) }))
	t.Cleanup(fx.server.Close)
	return fx
}

func (fx *fixture) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	if r.URL.Path == "/sapi/system/information" {
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
	case r.URL.Path == "/sapi/media/folder" && action == "get":
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		end := min(offset+limit, len(fx.folders))
		jsonReply(t, w, map[string]any{"data": map[string]any{"folders": fx.folders[min(offset, end):end]}})
	case r.URL.Path == "/sapi/media/folder" && action == "save":
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var folder api.Folder
		require.NoError(t, json.Unmarshal(envelope.Data, &folder))
		folder.ID = api.ID(strconv.Itoa(fx.nextID))
		fx.nextID++
		fx.folders = append(fx.folders, folder)
		jsonReply(t, w, map[string]any{"id": folder.ID, "success": "Folder saved successfully"})
	case r.URL.Path == "/sapi/media/folder" && action == "delete":
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Folders []api.ID `json:"folders"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.Len(t, data.Folders, 1)
		for i, folder := range fx.folders {
			if folder.ID == data.Folders[0] {
				fx.folders = append(fx.folders[:i], fx.folders[i+1:]...)
				break
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get":
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			IDs []api.ID `json:"ids"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		items := fx.media
		more := false
		if data.IDs != nil {
			items = nil
			for _, item := range fx.media {
				for _, id := range data.IDs {
					if id == item.ID {
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
		jsonReply(t, w, map[string]any{"data": map[string]any{"media": items}, "more": more})
	case (r.URL.Path == "/sapi/upload" || r.URL.Path == "/sapi/upload/file") && action == "save":
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, "invalid upload", http.StatusBadRequest)
			return
		}
		defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
		require.NoError(t, json.Unmarshal([]byte(r.FormValue("data")), &envelope))
		var data api.Upload
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
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
			id = api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
		} else {
			assert.Equal(t, "/sapi/upload/file", r.URL.Path)
		}
		modified, err := time.Parse(dateFormat, data.Modified)
		require.NoError(t, err)
		item := api.Media{ID: id, FolderID: data.FolderID, Name: data.Name, Size: data.Size, Modified: modified.UnixMilli(), URL: fx.server.URL + "/content/" + string(id), Type: "file", Status: "U"}
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
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Files      []api.ID `json:"files"`
			SoftDelete bool     `json:"softdelete"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
		for _, id := range data.Files {
			for i, item := range fx.media {
				if item.ID == id {
					fx.media = append(fx.media[:i], fx.media[i+1:]...)
					delete(fx.content, id)
					break
				}
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get-storage-space":
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

func TestPagination(t *testing.T) {
	fx := newFixture(t)
	for i := range pageSize + 1 {
		id := api.ID(strconv.Itoa(i + 1))
		fx.folders = append(fx.folders, api.Folder{ID: id, Name: fmt.Sprintf("folder%d", i)})
		fx.media = append(fx.media, api.Media{ID: id, Name: fmt.Sprintf("file%d", i), Type: "file", Status: "U"})
	}
	f, err := NewFs(context.Background(), "test", "", fx.config(t))
	require.NoError(t, err)
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 2*(pageSize+1))
	_, err = f.NewObject(context.Background(), fmt.Sprintf("file%d", pageSize))
	require.NoError(t, err)
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

func TestOAuthRequestHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/system/information" {
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
			return
		}
		header := r.Header.Get("Authorization")
		assert.True(t, strings.HasPrefix(header, oauthPrefix), "desktop OAuth is required on each SAPI request")
		switch r.URL.Path {
		case "/sapi/login/oauth":
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/media/folder":
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"rotated","refreshtoken":"rotated-refresh","expiresin":3600}}`)))
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": []any{}}})
		case "/sapi/media":
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []any{}}})
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}, false))
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.NoError(t, err)
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "rotated-refresh", token.RefreshToken)
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
