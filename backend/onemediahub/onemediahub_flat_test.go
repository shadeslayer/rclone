package onemediahub

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type flatFixture struct {
	*fixture
	folderWrites atomic.Int32
	handlerMu    sync.RWMutex
	handler      http.Handler
}

func newFlatFixture(t *testing.T) *flatFixture {
	t.Helper()
	f := &flatFixture{fixture: newFixture(t)}
	f.requestTime = 1700000000123
	f.folders = []api.Folder{{ID: "1", Name: "physical"}, {ID: "2", ParentID: "1", Name: "legacy-folder"}, {ID: "3", Name: "elsewhere"}}
	handler := f.server.Config.Handler
	f.server.Close()
	f.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/media/folder" && (r.URL.Query().Get("action") == "save" || r.URL.Query().Get("action") == "delete") {
			f.folderWrites.Add(1)
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "FOL-1035", "message": "Total folder limit reached"}})
			return
		}
		handler.ServeHTTP(w, r)
	})
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.handlerMu.RLock()
		handler := f.handler
		f.handlerMu.RUnlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *flatFixture) wrapHandler(wrap func(http.Handler) http.Handler) {
	f.handlerMu.Lock()
	f.handler = wrap(f.handler)
	f.handlerMu.Unlock()
}

func gateFlatContent(fx *flatFixture, id string) (<-chan struct{}, func()) {
	started, release := make(chan struct{}), make(chan struct{})
	var reading, released atomic.Bool
	unblock := func() {
		if !released.Swap(true) {
			close(release)
		}
	}
	fx.wrapHandler(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/content/"+id && reading.CompareAndSwap(false, true) {
				close(started)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			handler.ServeHTTP(w, r)
		})
	})
	return started, unblock
}

func truncateFlatUploadNames(fx *flatFixture) {
	fx.wrapHandler(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.URL.Path == "/sapi/upload" || r.URL.Path == "/sapi/upload/file") && r.URL.Query().Get("action") == "save" {
				reply := httptest.NewRecorder()
				handler.ServeHTTP(reply, r)
				fx.mu.Lock()
				for i := range fx.media {
					if len(fx.media[i].Name) > 255 {
						fx.media[i].Name = fx.media[i].Name[:255]
					}
				}
				fx.mu.Unlock()
				for key, values := range reply.Header() {
					w.Header()[key] = values
				}
				w.WriteHeader(reply.Code)
				_, _ = w.Write(reply.Body.Bytes())
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
}

func flatFixtureName(remote string, directory bool) string {
	prefix := "rclone-flat-v1-f-"
	if directory {
		prefix = "rclone-flat-v1-d-"
	}
	return prefix + base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(remote))
}

func flatHashedFixtureName(remote, kind string) string {
	digest := sha256.Sum256([]byte(remote))
	return "rclone-flat-v1-" + kind + "-" + fmt.Sprintf("%x", digest[:])
}

func flatMappingFixture(t *testing.T, remote string) []byte {
	t.Helper()
	payload := map[string]any{"version": 1, "path": remote}
	if !utf8.ValidString(remote) {
		delete(payload, "path")
		payload["path_bytes"] = base64.RawStdEncoding.EncodeToString([]byte(remote))
	}
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	return b
}

func flatTestFs(t *testing.T, fx *flatFixture, root string, cached bool, extra ...configmap.Simple) (*Fs, context.Context) {
	t.Helper()
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	m := fx.config(t)
	m["flat_namespace"], m["root_folder_id"], m["metadata_cache"] = "true", "1", strconv.FormatBool(cached)
	for _, values := range extra {
		for key, value := range values {
			m[key] = value
		}
	}
	ids := make([]api.ID, 0, len(fx.media))
	for _, item := range fx.media {
		ids = append(ids, item.ID)
	}
	fx.changes = map[string]api.Changes{"file": {New: ids}}
	remote, err := NewFs(ctx, "flat-test", root, m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	return f, ctx
}

func flatEntryNames(entries fs.DirEntries) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Remote()
		if _, directory := entry.(fs.Directory); directory {
			name += "/"
		}
		names = append(names, name)
	}
	return names
}

func TestFlatFileOperationsAtFolderCap(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			f, ctx := flatTestFs(t, fx, "", cached)
			require.NoError(t, f.Mkdir(ctx, "parent/empty"))
			require.NoError(t, f.Mkdir(ctx, "parent/empty"))
			modified := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			src := object.NewStaticObjectInfo("parent/file.txt", modified, 5, true, nil, f)
			obj, err := f.Put(ctx, strings.NewReader("hello"), src)
			require.NoError(t, err)
			assert.Equal(t, "parent/file.txt", obj.Remote())
			assert.EqualValues(t, 5, obj.Size())
			assert.True(t, modified.Equal(obj.ModTime(ctx)))
			entries, err := f.List(ctx, "parent")
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"parent/empty/", "parent/file.txt"}, flatEntryNames(entries))
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
			fx.mu.Lock()
			var markerCount int
			for _, item := range fx.media {
				assert.Equal(t, api.ID("1"), item.FolderID)
				assert.True(t, strings.HasPrefix(item.Name, "rclone-flat-v1-"))
				if item.ID == api.ID(oldID) {
					assert.Equal(t, flatFixtureName("parent/file.txt", false), item.Name)
					assert.Equal(t, "new", fx.content[item.ID])
				}
				if item.Name == flatFixtureName("parent/empty", true) {
					markerCount++
					assert.Zero(t, item.Size)
				}
			}
			fx.mu.Unlock()
			assert.Equal(t, 1, markerCount)
			require.NoError(t, obj.Remove(ctx))
			_, err = f.NewObject(ctx, "parent/file.txt")
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			require.NoError(t, f.Rmdir(ctx, "parent/empty"))
			entries, err = f.List(ctx, "parent")
			require.NoError(t, err)
			assert.Empty(t, entries)
			require.NoError(t, f.Rmdir(ctx, "parent"))
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatProjectionAndLogicalRoots(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			paths := []struct {
				id, remote, content string
				directory           bool
				parent              api.ID
			}{
				{"10", "project/readme.txt", "hello", false, "1"},
				{"11", "project/deep/data.bin", "abc", false, "1"},
				{"12", "project/empty", "", true, "1"},
				{"13", "elsewhere/empty", "", true, "1"},
				{"14", "elsewhere/skip.txt", "skip", false, "1"},
				{"15", "project2/leak.txt", "other prefix", false, "1"},
				{"16", "project/other-parent.txt", "outside", false, "3"},
			}
			for _, item := range paths {
				id := api.ID(item.id)
				fx.media = append(fx.media, api.Media{ID: id, FolderID: item.parent, Name: flatFixtureName(item.remote, item.directory), Size: int64(len(item.content)), Type: "file", URL: fx.server.URL + "/content/" + item.id})
				fx.content[id] = item.content
			}
			fx.media = append(fx.media, api.Media{ID: "17", FolderID: "1", Name: "ordinary.txt", Size: 8, Type: "file"})
			fx.media = append(fx.media, api.Media{ID: "18", FolderID: "3", Name: "rclone-flat-unsupported", Size: 0, Type: "file"})
			f, ctx := flatTestFs(t, fx, "project", cached)
			assert.Equal(t, "project", f.Root())
			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"readme.txt", "deep/", "empty/"}, flatEntryNames(entries))
			var recursive fs.DirEntries
			err = f.ListR(ctx, "", func(entries fs.DirEntries) error {
				recursive = append(recursive, entries...)
				return nil
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"readme.txt", "deep/", "deep/data.bin", "empty/"}, flatEntryNames(recursive))
			obj, err := f.NewObject(ctx, "readme.txt")
			require.NoError(t, err)
			assert.Equal(t, "readme.txt", obj.Remote())
			assert.Equal(t, "10", obj.(*Object).ID())
			body, err := obj.Open(ctx)
			require.NoError(t, err)
			b, err := io.ReadAll(body)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			assert.Equal(t, "hello", string(b))
			for _, remote := range []string{"empty", "ordinary.txt", "other-parent.txt", "../elsewhere/skip.txt"} {
				_, err := f.NewObject(ctx, remote)
				require.Error(t, err)
			}
			_, err = f.List(ctx, "missing")
			require.ErrorIs(t, err, fs.ErrorDirNotFound)
			require.NoError(t, f.Mkdir(ctx, "new/empty"))
			_, err = f.List(ctx, "new/empty")
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("new/file.txt", time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
			require.NoError(t, err)
			fx.mu.Lock()
			var found bool
			for _, item := range fx.media {
				if item.Name == flatFixtureName("project/new/file.txt", false) {
					found = true
					assert.Equal(t, api.ID("1"), item.FolderID)
				}
			}
			fx.mu.Unlock()
			assert.True(t, found)
			assert.Zero(t, fx.folderWrites.Load())
			if cached {
				f.metadata.mu.Lock()
				assert.Equal(t, flatFixtureName("project/readme.txt", false), f.metadata.state.Media["10"].Name)
				assert.Equal(t, "ordinary.txt", f.metadata.state.Media["17"].Name)
				assert.Equal(t, "rclone-flat-unsupported", f.metadata.state.Media["18"].Name)
				assert.Equal(t, api.ID("3"), f.metadata.state.Media["16"].FolderID)
				f.metadata.mu.Unlock()
			}
		})
	}
}

func TestFlatMalformedOwnedNames(t *testing.T) {
	names := []string{
		"rclone-flat-v2-f-84", "rclone-flat-v1-x-84", "rclone-flat-v1-f-", "rclone-flat-v1-f-85",
		"rclone-flat-v1-f-!", flatFixtureName("a", false) + "=", strings.ToLower(flatFixtureName("hello", false)),
	}
	for _, remote := range []string{".", "..", "../escape", "/absolute", "a//b", "a/./b", "a/../b", "a/"} {
		names = append(names, flatFixtureName(remote, false))
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{{ID: "10", FolderID: "1", Name: name, Type: "file"}}
			m := fx.config(t)
			m["flat_namespace"], m["root_folder_id"] = "true", "1"
			remote, err := NewFs(context.Background(), "malformed-flat", "", m)
			if err != nil {
				return
			}
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
			_, err = f.List(context.Background(), "")
			require.Error(t, err, "an owned malformed name must not disappear silently")
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatProviderNumberedDirectoryMarkers(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, full := range []string{"upload/upload", strings.Repeat("long-directory/", 16) + "empty"} {
			t.Run(fmt.Sprintf("cached=%v/long=%v", cached, len(full) > 100), func(t *testing.T) {
				fx := newFlatFixture(t)
				fx.nextID = 100
				name, err := flatName(full, true)
				require.NoError(t, err)
				fx.media = []api.Media{
					{ID: "20", FolderID: "1", Name: name, Type: "file"},
					{ID: "21", FolderID: "1", Name: name + " (1)", Type: "file", Status: "U"},
					{ID: "22", FolderID: "1", Name: name + " (2)", Type: "file", Status: "U"},
				}
				if strings.Contains(name, "-d-h-") {
					body := flatMappingFixture(t, full)
					fx.media = append(fx.media, api.Media{ID: "23", FolderID: "1", Name: flatMappingName(full), Size: int64(len(body)), Type: "file", URL: fx.server.URL + "/content/23"})
					fx.content["23"] = string(body)
				}
				f, ctx := flatTestFs(t, fx, "", cached)
				entries, err := f.List(ctx, path.Dir(full))
				require.NoError(t, err)
				assert.Equal(t, []string{full + "/"}, flatEntryNames(entries))
				require.NoError(t, f.Mkdir(ctx, full))
				src := object.NewStaticObjectInfo(full+"/preview.jpeg", time.Now(), 5, true, nil, f)
				obj, err := f.Put(ctx, strings.NewReader("hello"), src)
				require.NoError(t, err)
				require.ErrorIs(t, f.Rmdir(ctx, full), fs.ErrorDirectoryNotEmpty)
				require.NoError(t, obj.Remove(ctx))
				require.NoError(t, f.Rmdir(ctx, full))
				fx.mu.Lock()
				for _, item := range fx.media {
					assert.NotContains(t, []api.ID{"20", "21", "22"}, item.ID)
				}
				fx.mu.Unlock()
				assert.Zero(t, fx.folderWrites.Load())
			})
		}
	}
}

func TestFlatProviderAliasesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		size int64
	}{
		{flatFixtureName("empty", true) + " (1)", 1},
		{flatFixtureName("file", false) + " (1)", 0},
		{flatMappingName(strings.Repeat("long/", 20)) + " (1)", 0},
		{flatFixtureName("empty", true) + " (0)", 0},
		{flatFixtureName("empty", true) + " (01)", 0},
		{flatFixtureName("empty", true) + " (1) (2)", 0},
		{"rclone-flat-v1-d-85 (1)", 0},
		{flatFixtureName(strings.Repeat("a", 146), true) + " (10)", 0},
	} {
		t.Run(fmt.Sprintf("%s/size=%d", test.name, test.size), func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{{ID: "20", FolderID: "1", Name: test.name, Size: test.size, Type: "file"}}
			f, ctx := flatTestFs(t, fx, "", false)
			_, err := f.List(ctx, "")
			require.ErrorContains(t, err, "invalid")
			src := object.NewStaticObjectInfo("unrelated.txt", time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
			require.Error(t, err)
			fx.mu.Lock()
			assert.Len(t, fx.media, 1)
			assert.Equal(t, test.name, fx.media[0].Name)
			fx.mu.Unlock()
		})
	}
}

func TestFlatConcurrentMkdir(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.wrapHandler(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/sapi/upload" || r.URL.Query().Get("action") != "save" {
						handler.ServeHTTP(w, r)
						return
					}
					time.Sleep(30 * time.Millisecond)
					reply := httptest.NewRecorder()
					handler.ServeHTTP(reply, r)
					fx.mu.Lock()
					counts := map[string]int{}
					for i := range fx.media {
						name := fx.media[i].Name
						if counts[name] > 0 {
							fx.media[i].Name += fmt.Sprintf(" (%d)", counts[name])
						}
						counts[name]++
					}
					fx.mu.Unlock()
					for key, values := range reply.Header() {
						w.Header()[key] = values
					}
					w.WriteHeader(reply.Code)
					_, _ = w.Write(reply.Body.Bytes())
				})
			})
			f, ctx := flatTestFs(t, fx, "", cached)
			errs := make(chan error, 8)
			start := make(chan struct{})
			for range cap(errs) {
				go func() {
					<-start
					errs <- f.Mkdir(ctx, "upload/upload")
				}()
			}
			close(start)
			for range cap(errs) {
				assert.NoError(t, <-errs)
			}
			fx.mu.Lock()
			assert.Len(t, fx.media, 2, "each virtual directory must be created once")
			fx.mu.Unlock()
		})
	}
}

func TestFlatDefaultViewIsUnchanged(t *testing.T) {
	fx := newFlatFixture(t)
	fx.media = []api.Media{{ID: "10", FolderID: "1", Name: flatFixtureName("logical/file.txt", false), Size: 1, Type: "file"}, {ID: "11", FolderID: "1", Name: "ordinary.txt", Size: 2, Type: "file"}}
	m := fx.config(t)
	m["root_folder_id"] = "1"
	remote, err := NewFs(context.Background(), "raw-flat-neighbor", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{flatFixtureName("logical/file.txt", false), "ordinary.txt", "legacy-folder/"}, flatEntryNames(entries))
}

func TestFlatPendingUploadsAndEmptyMarkers(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{
				{ID: "41", FolderID: "1", Name: flatFixtureName("visible", true), Type: "file"},
				{ID: "42", FolderID: "1", Name: flatFixtureName("visible/unfinished.txt", false), Type: "file", Size: 6},
				{ID: "43", FolderID: "1", Name: flatFixtureName("unrelated", true), Type: "file"},
			}
			f, ctx := flatTestFs(t, fx, "", cached, configmap.Simple{"async_upload": "true", "resume_uploads": "true"})
			source, _ := recoverySource(t)
			r, err := (&Object{fs: f, remote: "visible/unfinished.txt"}).prepareUploadRecovery(ctx, api.Upload{FolderID: "1", Name: flatFixtureName("visible/unfinished.txt", false), Size: 6}, source, strings.NewReader("abcdef"))
			require.NoError(t, err)
			r.record.ID = "42"
			require.NoError(t, r.save())
			require.NoError(t, r.lock.Unlock())
			entries, err := f.List(ctx, "visible")
			require.NoError(t, err)
			assert.Empty(t, entries)
			_, err = f.NewObject(ctx, "visible/unfinished.txt")
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			var recursive fs.DirEntries
			require.NoError(t, f.ListR(ctx, "", func(entries fs.DirEntries) error {
				recursive = append(recursive, entries...)
				return nil
			}))
			assert.ElementsMatch(t, []string{"visible/", "unrelated/"}, flatEntryNames(recursive))
			require.ErrorIs(t, f.Rmdir(ctx, "visible"), fs.ErrorDirectoryNotEmpty)
			require.NoError(t, f.Rmdir(ctx, "unrelated"))
			require.NoError(t, f.journalDo(true, &uploadJournalOp{key: r.key, remove: true}))
			entries, err = f.List(ctx, "visible")
			require.NoError(t, err)
			assert.Equal(t, []string{"visible/unfinished.txt"}, flatEntryNames(entries))
			if cached {
				f.metadata.mu.Lock()
				assert.Equal(t, flatFixtureName("visible/unfinished.txt", false), f.metadata.state.Media["42"].Name)
				f.metadata.mu.Unlock()
			}
			assert.Nil(t, f.Features().DirMove)
			require.ErrorIs(t, f.DirMove(ctx, f, "visible", "renamed"), fs.ErrorCantDirMove)
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatExactNamesAndTraversal(t *testing.T) {
	fx := newFlatFixture(t)
	f, ctx := flatTestFs(t, fx, "", false)
	for _, remote := range []string{"photos/mañana/東京📷.jpg", ".hidden/child...", " spaces /a:b?c", "same-prefix/a", "same-prefix/ab"} {
		src := object.NewStaticObjectInfo(remote, time.Now(), 1, true, nil, f)
		obj, err := f.Put(ctx, strings.NewReader("x"), src)
		require.NoError(t, err)
		assert.Equal(t, remote, obj.Remote())
		found, err := f.NewObject(ctx, remote)
		require.NoError(t, err)
		assert.Equal(t, obj.(*Object).ID(), found.(*Object).ID())
		fx.mu.Lock()
		var name string
		for _, item := range fx.media {
			if string(item.ID) == obj.(*Object).ID() {
				name = item.Name
				break
			}
		}
		fx.mu.Unlock()
		assert.Equal(t, flatFixtureName(remote, false), name)
	}
	fx.mu.Lock()
	before := len(fx.media)
	fx.mu.Unlock()
	for _, remote := range []string{"", ".", "..", "../escape", "/absolute", "a//b", "a/./b", "a/../b", "a/"} {
		src := object.NewStaticObjectInfo(remote, time.Now(), 1, true, nil, f)
		_, err := f.Put(ctx, strings.NewReader("x"), src)
		require.Error(t, err, remote)
		if remote != "" {
			require.Error(t, f.Mkdir(ctx, remote), remote)
		}
	}
	fx.mu.Lock()
	assert.Len(t, fx.media, before)
	fx.mu.Unlock()
	assert.Zero(t, fx.folderWrites.Load())
}

func TestFlatListRCallbacksAndCancellation(t *testing.T) {
	fx := newFlatFixture(t)
	fx.media = []api.Media{{ID: "10", FolderID: "1", Name: flatFixtureName("a/b.txt", false), Size: 1, Type: "file"}}
	f, ctx := flatTestFs(t, fx, "", false)
	callbackErr := errors.New("flat callback stopped")
	err := f.ListR(ctx, "", func(fs.DirEntries) error { return callbackErr })
	require.ErrorIs(t, err, callbackErr)
	err = f.ListR(ctx, "", func(entries fs.DirEntries) error {
		for _, entry := range entries {
			if obj, ok := entry.(fs.Object); ok {
				found, err := f.NewObject(ctx, obj.Remote())
				require.NoError(t, err)
				assert.Equal(t, obj.(*Object).ID(), found.(*Object).ID())
			}
		}
		_, err := f.List(ctx, "")
		return err
	})
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, f.ListR(canceled, "", func(fs.DirEntries) error {
		t.Error("canceled traversal called its callback")
		return nil
	}), context.Canceled)
}

func TestFlatFileRootAndUnfiledParent(t *testing.T) {
	fx := newFlatFixture(t)
	fx.media = []api.Media{
		{ID: "10", Name: flatFixtureName("project/file.txt", false), Type: "file", Size: 1},
		{ID: "11", FolderID: "0", Name: flatFixtureName("project/other.txt", false), Type: "file", Size: 1},
		{ID: "12", FolderID: "1", Name: flatFixtureName("project/hidden.txt", false), Type: "file", Size: 1},
	}
	f, ctx := flatTestFs(t, fx, "project", false, configmap.Simple{"root_folder_id": ""})
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"file.txt", "other.txt"}, flatEntryNames(entries))
	m := fx.config(t)
	m["flat_namespace"] = "true"
	remote, err := NewFs(ctx, "flat-file-root", "project/file.txt", m)
	require.ErrorIs(t, err, fs.ErrorIsFile)
	require.NotNil(t, remote)
	assert.Equal(t, "project", remote.Root())
	t.Cleanup(func() { require.NoError(t, remote.(*Fs).Shutdown(ctx)) })
	src := object.NewStaticObjectInfo("new.txt", time.Now(), 1, true, nil, f)
	_, err = f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	fx.mu.Lock()
	var found bool
	for _, item := range fx.media {
		if item.Name == flatFixtureName("project/new.txt", false) {
			found = true
			assert.True(t, item.FolderID == "" || item.FolderID == "0")
		}
	}
	fx.mu.Unlock()
	assert.True(t, found)
	assert.Zero(t, fx.folderWrites.Load())
}

func TestFlatConflictsAndMarkerIntegrity(t *testing.T) {
	for _, tc := range []struct {
		name, root string
		items      []api.Media
	}{
		{"duplicate-file", "", []api.Media{{ID: "10", Name: flatFixtureName("a.txt", false)}, {ID: "11", Name: flatFixtureName("a.txt", false)}}},
		{"file-and-marker", "", []api.Media{{ID: "10", Name: flatFixtureName("a", false)}, {ID: "11", Name: flatFixtureName("a", true)}}},
		{"file-and-descendant", "", []api.Media{{ID: "10", Name: flatFixtureName("a", false)}, {ID: "11", Name: flatFixtureName("a/b/c.txt", false)}}},
		{"nonempty-marker", "", []api.Media{{ID: "10", Name: flatFixtureName("project", true), Size: 1}}},
		{"nonempty-root-marker", "project", []api.Media{{ID: "10", Name: flatFixtureName("project", true), Size: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFlatFixture(t)
			for _, item := range tc.items {
				item.FolderID, item.Type = "1", "file"
				fx.media = append(fx.media, item)
			}
			f, ctx := flatTestFs(t, fx, tc.root, false)
			_, err := f.List(ctx, "")
			require.Error(t, err)
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatMutationsPreflightPathConflicts(t *testing.T) {
	for _, tc := range []struct {
		name, existing string
		directory      bool
		mkdir          bool
		path           string
		want           error
	}{
		{"file-ancestor", "a", false, true, "a/b", fs.ErrorIsFile},
		{"implicit-directory", "a/b/c.txt", false, false, "a", fs.ErrorIsDir},
		{"explicit-directory", "a", true, false, "a", fs.ErrorIsDir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{{ID: "10", FolderID: "1", Name: flatFixtureName(tc.existing, tc.directory), Type: "file"}}
			f, ctx := flatTestFs(t, fx, "", false)
			var err error
			if tc.mkdir {
				err = f.Mkdir(ctx, tc.path)
			} else {
				src := object.NewStaticObjectInfo(tc.path, time.Now(), 1, true, nil, f)
				_, err = f.Put(ctx, strings.NewReader("x"), src)
			}
			require.ErrorIs(t, err, tc.want)
			fx.mu.Lock()
			assert.Len(t, fx.media, 1)
			assert.Zero(t, fx.requests["/sapi/upload"])
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatNotificationProjectionStability(t *testing.T) {
	fx := newFlatFixture(t)
	fx.media = []api.Media{
		{ID: "10", FolderID: "1", Name: flatFixtureName("project/a/one.txt", false), Type: "file", Size: 1, ETag: "one-v1"},
		{ID: "11", FolderID: "1", Name: flatFixtureName("project/a/two.txt", false), Type: "file", Size: 2, ETag: "two-v1"},
		{ID: "12", FolderID: "1", Name: flatFixtureName("project/empty", true), Type: "file", Date: 1000},
		{ID: "13", FolderID: "1", Name: "ordinary.txt", Type: "file", Size: 4},
		{ID: "14", FolderID: "3", Name: flatFixtureName("project/outside.txt", false), Type: "file", Size: 4},
	}
	f, ctx := flatTestFs(t, fx, "project", true)
	baseline, err := f.notificationState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "a/one.txt", baseline[notificationKey{"10", fs.EntryObject}].remote)
	assert.Equal(t, "a/two.txt", baseline[notificationKey{"11", fs.EntryObject}].remote)
	assert.NotContains(t, baseline, notificationKey{"13", fs.EntryObject})
	assert.NotContains(t, baseline, notificationKey{"14", fs.EntryObject})
	var dirsBefore = make(map[notificationKey]notificationEntry)
	for key, entry := range baseline {
		if key.kind == fs.EntryDirectory {
			dirsBefore[key] = entry
		}
	}
	require.Len(t, dirsBefore, 2)
	items := append([]api.Media(nil), fx.media...)
	items[2].Date = 99999
	items = append(items, api.Media{ID: "15", FolderID: "1", Name: flatFixtureName("project/a/three.txt", false), Type: "file", Size: 3})
	after, err := f.notificationEntries(ctx, fx.folders, items)
	require.NoError(t, err)
	var dirsAfter = make(map[notificationKey]notificationEntry)
	for key, entry := range after {
		if key.kind == fs.EntryDirectory {
			dirsAfter[key] = entry
		}
	}
	assert.Equal(t, dirsBefore, dirsAfter)
	assert.Equal(t, baseline[notificationKey{"10", fs.EntryObject}], after[notificationKey{"10", fs.EntryObject}])
	assert.Equal(t, "a/three.txt", after[notificationKey{"15", fs.EntryObject}].remote)
	assert.Equal(t, 5, len(after))
}

func TestFlatNewObjectRejectsDuplicatePhysicalNames(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{
				{ID: "10", FolderID: "1", Name: flatFixtureName("duplicate.txt", false), Type: "file", Size: 1},
				{ID: "11", FolderID: "1", Name: flatFixtureName("duplicate.txt", false), Type: "file", Size: 2},
			}
			f, ctx := flatTestFs(t, fx, "", cached)
			_, err := f.NewObject(ctx, "duplicate.txt")
			require.ErrorContains(t, err, "duplicate")
			fx.mu.Lock()
			assert.Zero(t, fx.requests["/sapi/upload"])
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
		})
	}
}

type flatRestartInput struct {
	URL, CacheDir, Mode string
	Remote              string
	Check               bool
}

// TestFlatCacheRestartProcess runs without the .test suffix so lib/kv preserves its cache.
func TestFlatCacheRestartProcess(t *testing.T) {
	input := os.Getenv("RCLONE_O2_FLAT_CACHE_TEST")
	if input == "" {
		t.Skip("subprocess helper")
	}
	var request flatRestartInput
	require.NoError(t, json.Unmarshal([]byte(input), &request))
	require.NoError(t, config.SetCacheDir(request.CacheDir))
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	m := testConfig(t, configmap.Simple{
		"url": request.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test"),
		"flat_namespace": "true", "metadata_cache": "true", "root_folder_id": "1",
	})
	remote, err := NewFs(ctx, "flat-cache-restart", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	defer func() { require.NoError(t, f.Shutdown(ctx)) }()
	if !request.Check {
		if request.Mode == "legacy-trash" {
			// Persist the shape of a cache populated before all trash statuses were filtered.
			f.metadata.state.Media["90"] = api.Media{ID: "90", FolderID: "1", Name: "rclone-flat-unsupported", Status: "S"}
			f.metadata.state.Media["91"] = api.Media{ID: "91", FolderID: "1", Name: flatFixtureName("gone.txt", false), Status: "S"}
			f.metadata.state.Media["92"] = api.Media{ID: "92", FolderID: "1", Name: flatFixtureName("keep.txt", false), Status: "S"}
			f.metadata.dirty = true
		}
		if request.Mode == "long-path" {
			_, err := f.NewObject(ctx, request.Remote)
			require.NoError(t, err)
		}
		return
	}
	switch request.Mode {
	case "legacy-trash":
		entries, err := f.List(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"keep.txt"}, flatEntryNames(entries))
		_, err = f.NewObject(ctx, "gone.txt")
		require.ErrorIs(t, err, fs.ErrorObjectNotFound)
		obj, err := f.NewObject(ctx, "keep.txt")
		require.NoError(t, err)
		assert.EqualValues(t, 5, obj.Size(), "a deleted high-ID duplicate must not shadow the live file")
	case "duplicate":
		_, err := f.NewObject(ctx, "duplicate.txt")
		require.ErrorContains(t, err, "duplicate")
	case "implicit-directory":
		for _, name := range []string{"a", "a/b"} {
			src := object.NewStaticObjectInfo(name, time.Now(), 1, true, nil, f)
			_, err := f.Put(ctx, strings.NewReader("x"), src)
			require.ErrorIs(t, err, fs.ErrorIsDir)
		}
	case "invalid-name":
		src := object.NewStaticObjectInfo("new.txt", time.Now(), 1, true, nil, f)
		_, err := f.Put(ctx, strings.NewReader("x"), src)
		require.ErrorContains(t, err, "invalid")
	case "long-path":
		obj, err := f.NewObject(ctx, request.Remote)
		require.NoError(t, err)
		assert.Equal(t, request.Remote, obj.Remote())
		body, err := obj.Open(ctx)
		require.NoError(t, err)
		content, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		assert.Equal(t, "hello", string(content))
		entries, err := f.List(ctx, path.Dir(request.Remote))
		require.NoError(t, err)
		assert.Equal(t, []string{request.Remote}, flatEntryNames(entries))
	default:
		t.Fatalf("unknown restart fixture %q", request.Mode)
	}
}

func TestFlatPersistentCacheRebuildsDerivedIndexes(t *testing.T) {
	helper := recoveryHelper(t)
	for _, mode := range []string{"duplicate", "implicit-directory", "invalid-name", "legacy-trash"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFlatFixture(t)
			switch mode {
			case "legacy-trash":
				fx.media = []api.Media{{ID: "3", FolderID: "1", Name: flatFixtureName("keep.txt", false), Type: "file", Size: 5}}
			case "duplicate":
				fx.media = []api.Media{
					{ID: "10", FolderID: "1", Name: flatFixtureName("duplicate.txt", false), Type: "file", Size: 1},
					{ID: "11", FolderID: "1", Name: flatFixtureName("duplicate.txt", false), Type: "file", Size: 2},
				}
			case "implicit-directory":
				fx.media = []api.Media{{ID: "10", FolderID: "1", Name: flatFixtureName("a/b/c.txt", false), Type: "file", Size: 1}}
			case "invalid-name":
				fx.media = []api.Media{{ID: "10", FolderID: "1", Name: "rclone-flat-unsupported", Type: "file", Size: 1}}
			}
			var ids []api.ID
			for _, item := range fx.media {
				ids = append(ids, item.ID)
			}
			fx.changes = map[string]api.Changes{"file": {New: ids}}
			input := flatRestartInput{URL: fx.server.URL, CacheDir: t.TempDir(), Mode: mode}
			run := func() {
				b, err := json.Marshal(input)
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, helper, "-test.run=^TestFlatCacheRestartProcess$", "-test.v")
				cmd.Env = append(os.Environ(), "RCLONE_O2_FLAT_CACHE_TEST="+string(b))
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			}
			run()
			fx.mu.Lock()
			fx.changes = map[string]api.Changes{}
			before := len(fx.mediaBatches)
			fx.mu.Unlock()
			input.Check = true
			run()
			fx.mu.Lock()
			assert.Len(t, fx.mediaBatches, before, "restart should use persisted media when the changes feed is empty")
			assert.Zero(t, fx.requests["/sapi/upload"])
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatLongPathSurvivesProviderNameTruncation(t *testing.T) {
	fx := newFlatFixture(t)
	truncateFlatUploadNames(fx)
	f, ctx := flatTestFs(t, fx, "", false)
	remote := "root/" + strings.Repeat("x", 4096) + ".bin"
	src := object.NewStaticObjectInfo(remote, time.Now(), 5, true, nil, f)
	_, err := f.Put(ctx, strings.NewReader("hello"), src)
	require.NoError(t, err)
	obj, err := f.NewObject(ctx, remote)
	require.NoError(t, err)
	assert.Equal(t, remote, obj.Remote())
	entries, err := f.List(ctx, "root")
	require.NoError(t, err)
	assert.Equal(t, []string{remote}, flatEntryNames(entries))
	assert.Zero(t, fx.folderWrites.Load())
}

func TestFlatLongPathLifecycle(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			truncateFlatUploadNames(fx)
			f, ctx := flatTestFs(t, fx, "", cached)
			remote := "project/" + strings.Repeat("long-name", 600) + ".bin"
			src := object.NewStaticObjectInfo(remote, time.Unix(1700000000, 0), 5, true, nil, f)
			obj, err := f.Put(ctx, strings.NewReader("hello"), src)
			require.NoError(t, err)
			id := obj.(*Object).ID()
			body, err := obj.Open(ctx)
			require.NoError(t, err)
			b, err := io.ReadAll(body)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			assert.Equal(t, "hello", string(b))
			entries, err := f.List(ctx, "project")
			require.NoError(t, err)
			assert.Equal(t, []string{remote}, flatEntryNames(entries))
			var recursive fs.DirEntries
			require.NoError(t, f.ListR(ctx, "", func(entries fs.DirEntries) error {
				recursive = append(recursive, entries...)
				return nil
			}))
			assert.ElementsMatch(t, []string{"project/", remote}, flatEntryNames(recursive))
			fx.mu.Lock()
			var ids []api.ID
			for _, item := range fx.media {
				ids = append(ids, item.ID)
			}
			fx.changes = map[string]api.Changes{"file": {New: ids}}
			fx.mu.Unlock()
			m := fx.config(t)
			m["flat_namespace"], m["root_folder_id"], m["metadata_cache"] = "true", "1", strconv.FormatBool(cached)
			freshRemote, err := NewFs(ctx, "fresh-long-file-root", "project", m)
			require.NoError(t, err)
			fresh := freshRemote.(*Fs)
			t.Cleanup(func() { require.NoError(t, fresh.Shutdown(ctx)) })
			entries, err = fresh.List(ctx, "")
			require.NoError(t, err)
			assert.Equal(t, []string{strings.TrimPrefix(remote, "project/")}, flatEntryNames(entries))
			fileRemote, err := NewFs(ctx, "fresh-long-exact-file", remote, m)
			require.ErrorIs(t, err, fs.ErrorIsFile)
			fileFs := fileRemote.(*Fs)
			t.Cleanup(func() { require.NoError(t, fileFs.Shutdown(ctx)) })
			assert.Equal(t, "project", fileFs.Root())
			src = object.NewStaticObjectInfo(remote, time.Unix(1700000000, 0), 3, true, nil, f)
			require.NoError(t, obj.Update(ctx, strings.NewReader("new"), src))
			assert.Equal(t, id, obj.(*Object).ID())
			fx.mu.Lock()
			var mappingIndex, dataIndex = -1, -1
			for i, item := range fx.media {
				assert.LessOrEqual(t, len(item.Name), 255)
				if item.Name == flatHashedFixtureName(remote, "p") {
					mappingIndex = i
					var payload struct {
						Version int    `json:"version"`
						Path    string `json:"path"`
					}
					require.NoError(t, json.Unmarshal([]byte(fx.content[item.ID]), &payload))
					assert.Equal(t, 1, payload.Version)
					assert.Equal(t, remote, payload.Path)
				}
				if string(item.ID) == id {
					dataIndex = i
					assert.Equal(t, flatHashedFixtureName(remote, "f-h"), item.Name)
					assert.Equal(t, "new", fx.content[item.ID])
				}
			}
			fx.mu.Unlock()
			require.GreaterOrEqual(t, mappingIndex, 0)
			require.Greater(t, dataIndex, mappingIndex, "mapping must be saved before data")
			require.NoError(t, obj.Remove(ctx))
			_, err = f.NewObject(ctx, remote)
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			fx.mu.Lock()
			var retained int
			for _, item := range fx.media {
				if item.Name == flatHashedFixtureName(remote, "p") {
					retained++
				}
			}
			fx.mu.Unlock()
			assert.Equal(t, 1, retained)
			entries, err = f.List(ctx, "project")
			require.NoError(t, err)
			assert.Empty(t, entries)
			_, err = f.Put(ctx, strings.NewReader("new"), src)
			require.NoError(t, err)
			fx.mu.Lock()
			var reused int
			for _, item := range fx.media {
				if item.Name == flatHashedFixtureName(remote, "p") {
					reused++
				}
			}
			fx.mu.Unlock()
			assert.Equal(t, 1, reused)
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatLongEmptyDirectoryAndFreshRoot(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			truncateFlatUploadNames(fx)
			f, ctx := flatTestFs(t, fx, "", cached)
			dir := "parent/" + strings.Repeat("directory", 600)
			require.NoError(t, f.Mkdir(ctx, dir))
			require.NoError(t, f.Mkdir(ctx, dir))
			entries, err := f.List(ctx, "parent")
			require.NoError(t, err)
			assert.Equal(t, []string{dir + "/"}, flatEntryNames(entries))
			fx.mu.Lock()
			var ids []api.ID
			for _, item := range fx.media {
				ids = append(ids, item.ID)
			}
			fx.changes = map[string]api.Changes{"file": {New: ids}}
			fx.mu.Unlock()
			m := fx.config(t)
			m["flat_namespace"], m["root_folder_id"], m["metadata_cache"] = "true", "1", strconv.FormatBool(cached)
			remote, err := NewFs(ctx, "fresh-long-root", dir, m)
			require.NoError(t, err)
			fresh := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, fresh.Shutdown(ctx)) })
			entries, err = fresh.List(ctx, "")
			require.NoError(t, err)
			assert.Empty(t, entries)
			fx.mu.Lock()
			var markers, mappings int
			for _, item := range fx.media {
				assert.LessOrEqual(t, len(item.Name), 255)
				if item.Name == flatHashedFixtureName(dir, "d-h") {
					markers++
					assert.Zero(t, item.Size)
				}
				if item.Name == flatHashedFixtureName(dir, "p") {
					mappings++
					assert.JSONEq(t, string(flatMappingFixture(t, dir)), fx.content[item.ID])
				}
			}
			fx.mu.Unlock()
			assert.Equal(t, 1, markers)
			assert.Equal(t, 1, mappings)
			require.NoError(t, f.Rmdir(ctx, dir))
			fx.mu.Lock()
			var retained int
			for _, item := range fx.media {
				assert.NotEqual(t, flatHashedFixtureName(dir, "d-h"), item.Name)
				if item.Name == flatHashedFixtureName(dir, "p") {
					retained++
				}
			}
			fx.mu.Unlock()
			assert.Equal(t, 1, retained)
			entries, err = f.List(ctx, "parent")
			require.NoError(t, err)
			assert.Empty(t, entries)
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatUploadRequiresPreservedPhysicalName(t *testing.T) {
	fx := newFlatFixture(t)
	fx.wrapHandler(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.URL.Path == "/sapi/upload" || r.URL.Path == "/sapi/upload/file") && r.URL.Query().Get("action") == "save" {
				reply := httptest.NewRecorder()
				handler.ServeHTTP(reply, r)
				fx.mu.Lock()
				for i := range fx.media {
					if fx.media[i].Name == flatFixtureName("short.txt", false) {
						fx.media[i].Name = flatFixtureName("provider-renamed.txt", false)
					}
				}
				fx.mu.Unlock()
				for key, values := range reply.Header() {
					w.Header()[key] = values
				}
				w.WriteHeader(reply.Code)
				_, _ = w.Write(reply.Body.Bytes())
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
	f, ctx := flatTestFs(t, fx, "", false)
	src := object.NewStaticObjectInfo("short.txt", time.Now(), 1, true, nil, f)
	_, err := f.Put(ctx, strings.NewReader("x"), src)
	require.Error(t, err, "provider-renamed content cannot confirm the requested upload")
	assert.Zero(t, fx.folderWrites.Load())
}

func TestFlatLongBytePathRoundTrip(t *testing.T) {
	fx := newFlatFixture(t)
	truncateFlatUploadNames(fx)
	f, ctx := flatTestFs(t, fx, "", false)
	remote := "project/" + strings.Repeat("x", 4096) + string([]byte{0xff, 0xfe}) + ".bin"
	src := object.NewStaticObjectInfo(remote, time.Now(), 1, true, nil, f)
	_, err := f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	obj, err := f.NewObject(ctx, remote)
	require.NoError(t, err)
	assert.Equal(t, remote, obj.Remote())
	fx.mu.Lock()
	var payload map[string]any
	for _, item := range fx.media {
		if item.Name == flatHashedFixtureName(remote, "p") {
			require.NoError(t, json.Unmarshal([]byte(fx.content[item.ID]), &payload))
		}
	}
	fx.mu.Unlock()
	require.NotNil(t, payload)
	assert.NotContains(t, payload, "path")
	assert.Equal(t, base64.RawStdEncoding.EncodeToString([]byte(remote)), payload["path_bytes"])
}

func TestFlatMappingValidationFailsClosed(t *testing.T) {
	remote := "project/" + strings.Repeat("x", 4096) + ".bin"
	valid := string(flatMappingFixture(t, remote))
	for _, tc := range []struct {
		name, body string
		missing    bool
		duplicate  bool
	}{
		{name: "missing", missing: true},
		{name: "malformed-json", body: "{"},
		{name: "wrong-version", body: strings.Replace(valid, "\"version\":1", "\"version\":2", 1)},
		{name: "hash-mismatch", body: string(flatMappingFixture(t, "other/"+strings.Repeat("x", 4096)+".bin"))},
		{name: "traversal", body: `{"version":1,"path":"../escape"}`},
		{name: "invalid-base64", body: `{"version":1,"path_bytes":"!"}`},
		{name: "both-path-fields", body: `{"version":1,"path":"a","path_bytes":"YQ"}`},
		{name: "unknown-field", body: strings.TrimSuffix(valid, "}") + `,"unknown":true}`},
		{name: "trailing-json", body: valid + "{}"},
		{name: "oversized", body: strings.Repeat("x", (1<<20)+1)},
		{name: "corrupt-duplicate-mapping", body: valid, duplicate: true},
	} {
		for _, cached := range []bool{false, true} {
			t.Run(tc.name+"/"+strconv.FormatBool(cached), func(t *testing.T) {
				fx := newFlatFixture(t)
				fx.media = []api.Media{{ID: "10", FolderID: "1", Name: flatHashedFixtureName(remote, "f-h"), Type: "file", Size: 1, URL: fx.server.URL + "/content/10"}}
				fx.content["10"] = "x"
				if !tc.missing {
					fx.media = append(fx.media, api.Media{ID: "11", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(tc.body)), URL: fx.server.URL + "/content/11"})
					fx.content["11"] = tc.body
				}
				if tc.duplicate {
					other := string(flatMappingFixture(t, "other/"+strings.Repeat("x", 4096)+".bin"))
					fx.media = append(fx.media, api.Media{ID: "12", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(other)), URL: fx.server.URL + "/content/12"})
					fx.content["12"] = other
				}
				f, ctx := flatTestFs(t, fx, "", cached)
				_, err := f.List(ctx, "")
				require.Error(t, err)
				require.Error(t, f.ListR(ctx, "", func(fs.DirEntries) error { return nil }))
				src := object.NewStaticObjectInfo("safe.txt", time.Now(), 1, true, nil, f)
				_, err = f.Put(ctx, strings.NewReader("x"), src)
				require.Error(t, err)
				fx.mu.Lock()
				assert.Zero(t, fx.requests["/sapi/upload"])
				assert.Zero(t, fx.requests["/sapi/upload/file"])
				fx.mu.Unlock()
				assert.Zero(t, fx.folderWrites.Load())
			})
		}
	}
}

func TestFlatLongMappingsSurviveCacheRestart(t *testing.T) {
	fx := newFlatFixture(t)
	remote := "project/" + strings.Repeat("x", 4096) + ".bin"
	body := flatMappingFixture(t, remote)
	fx.media = []api.Media{
		{ID: "10", FolderID: "1", Name: flatHashedFixtureName(remote, "f-h"), Type: "file", Size: 5, URL: fx.server.URL + "/content/10"},
		{ID: "11", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(body)), URL: fx.server.URL + "/content/11"},
	}
	fx.content["10"], fx.content["11"] = "hello", string(body)
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"10", "11"}}}
	helper := recoveryHelper(t)
	input := flatRestartInput{URL: fx.server.URL, CacheDir: t.TempDir(), Mode: "long-path", Remote: remote}
	run := func() {
		b, err := json.Marshal(input)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, helper, "-test.run=^TestFlatCacheRestartProcess$", "-test.v")
		cmd.Env = append(os.Environ(), "RCLONE_O2_FLAT_CACHE_TEST="+string(b))
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	run()
	fx.mu.Lock()
	mappingDownloads := fx.requests["/content/11"]
	assert.Positive(t, mappingDownloads, "first process must validate the remote mapping")
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	input.Check = true
	run()
	fx.mu.Lock()
	assert.Equal(t, mappingDownloads, fx.requests["/content/11"], "restart must reuse the validated immutable mapping")
	assert.Zero(t, fx.requests["/sapi/upload"])
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestFlatCachedMappingChangesFailClosed(t *testing.T) {
	for _, mode := range []string{"replacement", "deletion"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFlatFixture(t)
			remote := "project/" + strings.Repeat("x", 4096) + ".bin"
			body := flatMappingFixture(t, remote)
			fx.media = []api.Media{
				{ID: "10", FolderID: "1", Name: flatHashedFixtureName(remote, "f-h"), Type: "file", Size: 1, URL: fx.server.URL + "/content/10"},
				{ID: "11", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(body)), ETag: "original", Modified: 1700000000000, URL: fx.server.URL + "/content/11"},
			}
			fx.content["10"], fx.content["11"] = "x", string(body)
			f, ctx := flatTestFs(t, fx, "", true)
			entries, err := f.List(ctx, "project")
			require.NoError(t, err)
			assert.Equal(t, []string{remote}, flatEntryNames(entries))
			_, err = f.NewObject(ctx, remote)
			require.NoError(t, err)
			fx.mu.Lock()
			if mode == "replacement" {
				fx.content["11"] = `{"version":1,"path":"wrong"}`
				fx.media[1].Size = int64(len(fx.content["11"]))
				fx.media[1].ETag = "replacement"
				fx.media[1].Modified++
				fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"11"}}}
			} else {
				fx.media = fx.media[:1]
				fx.changes = map[string]api.Changes{"file": {Deleted: []api.ID{"11"}}}
			}
			fx.mu.Unlock()
			f.expireMetadata()
			_, err = f.List(ctx, "project")
			require.Error(t, err, "old validated payload must not conceal changed remote mapping")
			_, err = f.NewObject(ctx, remote)
			require.Error(t, err)
			require.Error(t, f.ListR(ctx, "", func(fs.DirEntries) error { return nil }))
			src := object.NewStaticObjectInfo("safe.txt", time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
			require.Error(t, err)
			if mode == "deletion" {
				f.metadata.mu.Lock()
				assert.NotContains(t, f.metadata.state.FlatPaths, api.ID("11"))
				f.metadata.mu.Unlock()
			}
			fx.mu.Lock()
			assert.Zero(t, fx.requests["/sapi/upload"])
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatConcurrentMappingValidationFailsClosed(t *testing.T) {
	fx := newFlatFixture(t)
	remote := "project/" + strings.Repeat("x", 4096) + ".bin"
	body := flatMappingFixture(t, remote)
	fx.media = []api.Media{
		{ID: "10", FolderID: "1", Name: flatHashedFixtureName(remote, "f-h"), Type: "file", Size: 1, URL: fx.server.URL + "/content/10"},
		{ID: "11", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(body)), URL: fx.server.URL + "/content/11"},
	}
	fx.content["10"], fx.content["11"] = "x", string(body)
	started, unblock := gateFlatContent(fx, "11")
	defer unblock()
	f, ctx := flatTestFs(t, fx, "", true)
	result := make(chan error, 1)
	go func() {
		_, err := f.List(ctx, "")
		result <- err
	}()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("listing finished before validating the mapping: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("mapping validation did not start")
	}
	corrupt := `{"version":1,"path":"wrong"}`
	newMapping := api.Media{ID: "12", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(corrupt)), URL: fx.server.URL + "/content/12"}
	fx.mu.Lock()
	fx.media = append(fx.media, newMapping)
	fx.content["12"] = corrupt
	fx.mu.Unlock()
	f.cacheMedia(newMapping, false)
	unblock()
	select {
	case err := <-result:
		require.Error(t, err, "a newly observed corrupt sidecar must be validated before exposing its file")
	case <-time.After(5 * time.Second):
		t.Fatal("mapping validation did not finish")
	}
	_, err := f.NewObject(ctx, remote)
	require.Error(t, err)
	src := object.NewStaticObjectInfo("safe.txt", time.Now(), 1, true, nil, f)
	_, err = f.Put(ctx, strings.NewReader("x"), src)
	require.Error(t, err)
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload"])
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
	assert.Zero(t, fx.folderWrites.Load())
}

func TestFlatLongPendingUploadProtectsOnlyItsDirectory(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			remote := "visible/" + strings.Repeat("x", 4096) + ".txt"
			body := flatMappingFixture(t, remote)
			fx.media = []api.Media{
				{ID: "41", FolderID: "1", Name: flatFixtureName("visible", true), Type: "file"},
				{ID: "43", FolderID: "1", Name: flatFixtureName("unrelated", true), Type: "file"},
				{ID: "44", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(body)), URL: fx.server.URL + "/content/44"},
			}
			fx.content["44"] = string(body)
			f, ctx := flatTestFs(t, fx, "", cached, configmap.Simple{"async_upload": "true", "resume_uploads": "true"})
			source, _ := recoverySource(t)
			r, err := (&Object{fs: f, remote: remote}).prepareUploadRecovery(ctx, api.Upload{FolderID: "1", Name: flatHashedFixtureName(remote, "f-h"), Size: 6}, source, strings.NewReader("abcdef"))
			require.NoError(t, err)
			r.record.ID = "42"
			require.NoError(t, r.save())
			require.NoError(t, r.lock.Unlock())
			entries, err := f.List(ctx, "visible")
			require.NoError(t, err)
			assert.Empty(t, entries)
			_, err = f.NewObject(ctx, remote)
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			require.ErrorIs(t, f.Mkdir(ctx, remote), fs.ErrorIsFile)
			require.ErrorIs(t, f.Rmdir(ctx, "visible"), fs.ErrorDirectoryNotEmpty, "pending ID is not visible in remote metadata yet")
			require.NoError(t, f.Rmdir(ctx, "unrelated"))
			require.NoError(t, f.journalDo(true, &uploadJournalOp{key: r.key, remove: true}))
			require.NoError(t, f.Rmdir(ctx, "visible"))
			entries, err = f.List(ctx, "")
			require.NoError(t, err)
			assert.Empty(t, entries, "retained sidecar does not create a logical file or directory")
			fx.mu.Lock()
			assert.Len(t, fx.media, 1)
			assert.Equal(t, flatHashedFixtureName(remote, "p"), fx.media[0].Name)
			assert.Equal(t, string(body), fx.content["44"])
			assert.Zero(t, fx.requests["/sapi/upload"])
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestFlatConcurrentMappingPublicationFailsClosed(t *testing.T) {
	fx := newFlatFixture(t)
	remote := "project/" + strings.Repeat("x", 4096) + ".bin"
	body := flatMappingFixture(t, remote)
	fx.media = []api.Media{{ID: "11", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(body)), URL: fx.server.URL + "/content/11"}}
	fx.content["11"] = string(body)
	started, unblock := gateFlatContent(fx, "11")
	defer unblock()
	f, ctx := flatTestFs(t, fx, "", true)
	result := make(chan error, 1)
	go func() {
		src := object.NewStaticObjectInfo(remote, time.Now(), 1, true, nil, f)
		_, err := f.Put(ctx, strings.NewReader("x"), src)
		result <- err
	}()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("upload finished before validating the mapping: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("mapping validation did not start")
	}
	corrupt := `{"version":1,"path":"wrong"}`
	newMapping := api.Media{ID: "12", FolderID: "1", Name: flatHashedFixtureName(remote, "p"), Type: "file", Size: int64(len(corrupt)), URL: fx.server.URL + "/content/12"}
	fx.mu.Lock()
	fx.media = append(fx.media, newMapping)
	fx.content["12"] = corrupt
	fx.mu.Unlock()
	f.cacheMedia(newMapping, false)
	unblock()
	select {
	case err := <-result:
		require.Error(t, err, "an unvalidated newcomer must not authorize the first data upload")
	case <-time.After(5 * time.Second):
		t.Fatal("mapping validation did not finish")
	}
	fx.mu.Lock()
	for _, item := range fx.media {
		assert.NotEqual(t, flatHashedFixtureName(remote, "f-h"), item.Name)
	}
	assert.Equal(t, string(body), fx.content["11"])
	assert.Equal(t, corrupt, fx.content["12"])
	fx.mu.Unlock()
	assert.Zero(t, fx.folderWrites.Load())
}
