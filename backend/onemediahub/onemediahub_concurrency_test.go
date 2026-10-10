package onemediahub

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const concurrencyTimeout = 5 * time.Second

func TestMetadataIndexAllowsWrites(t *testing.T) {
	const entries = 200000
	for _, mode := range []string{"refresh", "mapping"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFlatFixture(t)
			f, ctx := flatTestFs(t, fx, "", true)
			c := f.metadata
			require.NoError(t, c.db.Stop(false))
			c.db = nil
			for i := range entries {
				id := api.ID(strconv.Itoa(i))
				item := api.Media{ID: id, FolderID: "1", Name: flatFixtureName("bulk/"+string(id), false), Type: "file", Size: 1}
				c.state.Media[id] = item
				c.byName[f.mediaKey(item)] = id
			}
			for _, id := range []api.ID{"rename", "delete", "duplicate-a", "duplicate-z"} {
				name := string(id)
				if strings.HasPrefix(name, "duplicate-") {
					name = "duplicate"
				}
				f.cacheMedia(api.Media{ID: id, FolderID: "1", Name: flatFixtureName(name+"/file.txt", false), Type: "file", Size: 1}, false)
			}
			f.rebuildFlatIndex(c)
			if mode == "refresh" {
				c.byName = nil // Force both indexes to rebuild during refresh.
			}
			c.checked = time.Now().Add(-time.Duration(f.opt.MetadataCacheTime))
			c.dirty = true
			items := slices.Collect(maps.Values(c.state.Media))
			result := make(chan error, 1)
			go func() {
				if mode == "refresh" {
					result <- f.syncMetadata(ctx)
					return
				}

				_, err := f.flatMappings(ctx, items)
				result <- err
			}()
			deadline := time.After(concurrencyTimeout)
			for {
				var stacks bytes.Buffer
				require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
				if bytes.Contains(stacks.Bytes(), []byte("onemediahub.(*Fs).rebuildFlatIndex")) {
					break
				}

				select {
				case err := <-result:
					t.Fatalf("index rebuild was not observed: %v", err)
				case <-deadline:
					t.Fatal("index rebuild did not start")
				case <-time.After(time.Millisecond):
				}
			}
			unlocked := c.mu.TryLock()
			if unlocked {
				c.mu.Unlock()
			}
			assert.True(t, unlocked, "index construction held the metadata mutex")
			item := api.Media{ID: "during-build", FolderID: "1", Name: flatFixtureName("concurrent/file.txt", false), Type: "file", Size: 1}
			f.cacheMedia(item, false)
			renamed := item
			renamed.ID = "rename"
			f.cacheMedia(renamed, false)
			f.cacheMedia(api.Media{ID: "delete"}, true)
			f.cacheMedia(api.Media{ID: "duplicate-z"}, true)
			waitConcurrent(t, result)
			assert.Equal(t, item, c.state.Media[item.ID])
			assert.Equal(t, renamed.ID, c.byName[f.mediaKey(item)])
			assert.Equal(t, renamed, c.state.Media[renamed.ID])
			assert.NotContains(t, c.state.Media, api.ID("delete"))
			assert.NotContains(t, c.flatDirs, mediaKey{parent: "1", name: "delete"})
			assert.NotContains(t, c.flatDirs, mediaKey{parent: "1", name: "rename"})
			assert.Equal(t, api.ID("duplicate-a"), c.byName[mediaKey{parent: "1", name: flatFixtureName("duplicate/file.txt", false)}])
			assert.Equal(t, 2, c.flatDirs[mediaKey{parent: "1", name: "concurrent"}])
		})
	}
}

func TestFlatUploadOverlapsMarkers(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, async := range []bool{false, true} {
			t.Run("cached="+strconv.FormatBool(cached)+"/async="+strconv.FormatBool(async), func(t *testing.T) {
				fx := newFlatFixture(t)
				uploadSourceFixture(t, fx)
				f, ctx := flatTestFs(t, fx, "", cached, configmap.Simple{"async_upload": strconv.FormatBool(async)})
				marker, releaseMarker := gateRequest(t, fx, func(r *http.Request, body []byte) bool {
					return strings.HasPrefix(r.URL.Path, "/sapi/upload") && bytes.Contains(body, []byte(flatFixtureName("parent", true)))
				})
				defer releaseMarker()
				content, releaseContent := gateRequest(t, fx, func(r *http.Request, body []byte) bool {
					return strings.HasPrefix(r.URL.Path, "/sapi/upload") && r.URL.Query().Get("action") == "save" && bytes.Contains(body, []byte("payload"))
				})
				defer releaseContent()
				result := make(chan error, 1)
				go func() {
					src := object.NewStaticObjectInfo("parent/file.txt", time.Now(), 7, true, nil, f)
					_, err := f.Put(ctx, strings.NewReader("payload"), src)
					result <- err
				}()
				select {
				case <-marker:
				case <-time.After(concurrencyTimeout):
					t.Fatal("marker upload did not start")
				}
				select {
				case <-content:
				case <-time.After(concurrencyTimeout):
					t.Error("file content waited for its parent marker")
					releaseMarker()
					select {
					case <-content:
					case <-time.After(concurrencyTimeout):
						t.Fatal("file upload did not start")
					}
				}
				select {
				case err := <-result:
					t.Fatalf("upload returned before its requests finished: %v", err)
				default:
				}
				releaseContent()
				select {
				case err := <-result:
					t.Fatalf("upload returned before its marker finished: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
				releaseMarker()
				waitConcurrent(t, result)
				entries, err := f.List(ctx, "parent")
				require.NoError(t, err)
				assert.Equal(t, []string{"parent/file.txt"}, flatEntryNames(entries))
			})
		}
	}
}

func TestFlatUploadMarkerFailure(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(strconv.FormatBool(async), func(t *testing.T) {
			fx := newFlatFixture(t)
			uploadSourceFixture(t, fx)
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.NoError(t, r.Body.Close())
					r.Body = io.NopCloser(bytes.NewReader(body))
					if strings.HasPrefix(r.URL.Path, "/sapi/upload") && bytes.Contains(body, []byte(flatFixtureName("parent", true))) {
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "marker failed"}})
						return
					}
					next.ServeHTTP(w, r)
				})
			})
			f, ctx := flatTestFs(t, fx, "", true, configmap.Simple{"async_upload": strconv.FormatBool(async)})
			marker, release := gateRequest(t, fx, func(r *http.Request, body []byte) bool {
				return strings.HasPrefix(r.URL.Path, "/sapi/upload") && bytes.Contains(body, []byte(flatFixtureName("parent", true)))
			})
			defer release()
			result := make(chan error, 1)
			go func() {
				src := object.NewStaticObjectInfo("parent/file.txt", time.Now(), 7, true, nil, f)
				_, err := f.Put(ctx, strings.NewReader("payload"), src)
				result <- err
			}()
			select {
			case <-marker:
			case <-time.After(concurrencyTimeout):
				t.Fatal("marker upload did not start")
			}
			require.Eventually(t, func() bool {
				f.metadata.mu.Lock()
				defer f.metadata.mu.Unlock()
				id := f.metadata.byName[mediaKey{parent: "1", name: flatFixtureName("parent/file.txt", false)}]
				return id != ""
			}, concurrencyTimeout, time.Millisecond)
			release()
			select {
			case err := <-result:
				require.ErrorContains(t, err, "marker failed")
			case <-time.After(concurrencyTimeout):
				t.Fatal("failed marker did not finish the upload")
			}
		})
	}
}

func gateRequest(t *testing.T, fx *flatFixture, match func(*http.Request, []byte) bool) (<-chan struct{}, func()) {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var gated atomic.Bool
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.NoError(t, r.Body.Close())
			r.Body = io.NopCloser(bytes.NewReader(body))
			if !match(r, body) || !gated.CompareAndSwap(false, true) {
				next.ServeHTTP(w, r)
				return
			}

			// Hold the response after taking its snapshot to expose stale refresh writes.
			reply := httptest.NewRecorder()
			next.ServeHTTP(reply, r)
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			for key, values := range reply.Header() {
				w.Header()[key] = values
			}
			w.WriteHeader(reply.Code)
			_, _ = w.Write(reply.Body.Bytes())
		})
	})
	return started, unblock
}

func waitConcurrent(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(concurrencyTimeout):
		t.Fatal("operation did not finish")
	}
}

func TestFlatIndependentCreates(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, kind := range []string{"directory", "mapping"} {
			t.Run(kind+"/cached="+strconv.FormatBool(cached), func(t *testing.T) {
				fx := newFlatFixture(t)
				f, ctx := flatTestFs(t, fx, "", cached)
				require.NoError(t, f.Mkdir(ctx, "shared"))
				slow, fast := "shared/slow", "shared/fast"
				create := f.Mkdir
				name := flatFixtureName(slow, true)
				if kind == "mapping" {
					slow += strings.Repeat("x", flatNameLimit)
					fast += strings.Repeat("y", flatNameLimit)
					name = flatMappingName(slow)
					create = f.ensureFlatMapping
				}
				started, unblock := gateRequest(t, fx, func(r *http.Request, body []byte) bool {
					return r.URL.Path == "/sapi/upload" && bytes.Contains(body, []byte(name))
				})
				defer unblock()
				slowResult, fastResult := make(chan error, 1), make(chan error, 1)
				go func() { slowResult <- create(ctx, slow) }()
				select {
				case <-started:
				case <-time.After(concurrencyTimeout):
					t.Fatal("slow upload did not start")
				}
				go func() { fastResult <- create(ctx, fast) }()
				select {
				case err := <-fastResult:
					assert.NoError(t, err)
				case <-time.After(concurrencyTimeout):
					t.Error("unrelated creation waited for the slow upload")
					unblock()
					waitConcurrent(t, fastResult)
				}
				unblock()
				waitConcurrent(t, slowResult)
			})
		}
	}
}

func TestFlatConcurrentFiles(t *testing.T) {
	const transfers = 16
	for _, cached := range []bool{false, true} {
		for _, prefix := range []string{"shared/", "shared/" + strings.Repeat("long", flatNameLimit) + "/"} {
			t.Run("cached="+strconv.FormatBool(cached)+"/long="+strconv.FormatBool(len(prefix) > flatNameLimit), func(t *testing.T) {
				fx := newFlatFixture(t)
				f, ctx := flatTestFs(t, fx, "", cached)
				results := make(chan error, transfers)
				for i := range transfers {
					go func() {
						content := strconv.Itoa(i)
						remote := prefix + content + "/file.txt"
						src := object.NewStaticObjectInfo(remote, time.Now(), int64(len(content)), true, nil, f)
						_, err := f.Put(ctx, strings.NewReader(content), src)
						results <- err
					}()
				}
				for range transfers {
					waitConcurrent(t, results)
				}
				for i := range transfers {
					content := strconv.Itoa(i)
					obj, err := f.NewObject(ctx, prefix+content+"/file.txt")
					require.NoError(t, err)
					body, err := obj.Open(ctx)
					require.NoError(t, err)
					actual, err := io.ReadAll(body)
					require.NoError(t, err)
					require.NoError(t, body.Close())
					assert.Equal(t, content, string(actual))
				}
				fx.mu.Lock()
				defer fx.mu.Unlock()
				seen := make(map[string]bool)
				for _, item := range fx.media {
					assert.False(t, seen[item.Name], "duplicate media name %q", item.Name)
					seen[item.Name] = true
				}
			})
		}
	}
}

func TestMetadataRefreshConcurrentWrites(t *testing.T) {
	for _, mode := range []string{"expired", "invalidated", "failed"} {
		t.Run(mode, func(t *testing.T) {
			testMetadataRefreshWrites(t, mode)
		})
	}
}

func testMetadataRefreshWrites(t *testing.T, mode string) {
	t.Helper()
	fx := newFlatFixture(t)
	fx.nextID = 100
	fx.media = []api.Media{
		{ID: "20", FolderID: "1", Name: flatFixtureName("old.txt", false), Type: "file", Size: 1},
		{ID: "21", FolderID: "1", Name: flatFixtureName("deleted.txt", false), Type: "file", Size: 1},
	}
	f, ctx := flatTestFs(t, fx, "", true)
	require.NoError(t, f.Mkdir(ctx, "shared"))
	_, err := f.List(ctx, "")
	require.NoError(t, err)
	started, unblock := gateRequest(t, fx, func(r *http.Request, _ []byte) bool {
		return r.URL.Path == "/sapi/media" && r.URL.Query().Get("action") == "get"
	})
	defer unblock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Now().Add(-time.Duration(f.opt.MetadataCacheTime))
	f.metadata.mu.Unlock()
	if mode != "expired" {
		f.expireMetadata()
	}

	if mode == "failed" {
		fx.mu.Lock()
		fx.failMedia = true
		fx.mu.Unlock()
	}
	refresh := make(chan error, 1)
	go func() { refresh <- f.syncMetadata(ctx) }()
	select {
	case <-started:
	case <-time.After(concurrencyTimeout):
		t.Fatal("metadata refresh did not start")
	}

	writes := make(chan error, 1)
	go func() {
		f.cacheMedia(api.Media{ID: "20", FolderID: "1", Name: flatFixtureName("renamed.txt", false), Type: "file", Size: 2}, false)
		f.cacheMedia(api.Media{ID: "21"}, true)
		f.cacheMedia(api.Media{ID: "22", FolderID: "1", Name: flatFixtureName("created.txt", false), Type: "file", Size: 3}, false)
		f.cacheFolder(api.Folder{ID: "2", Name: "renamed"}, false)
		f.cacheFolder(api.Folder{ID: "3"}, true)
		f.cacheFolder(api.Folder{ID: "4", Name: "created"}, false)
		var err error
		if mode == "expired" {
			src := object.NewStaticObjectInfo("shared/fast.txt", time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
		}
		f.expireMetadata()
		writes <- err
	}()
	select {
	case err := <-writes:
		assert.NoError(t, err)
	case <-time.After(concurrencyTimeout):
		t.Error("cache writes waited for the metadata refresh")
		unblock()
		waitConcurrent(t, writes)
	}
	unblock()
	select {
	case err := <-refresh:
		if mode == "failed" {
			require.ErrorContains(t, err, "failed metadata")
		} else {
			require.NoError(t, err)
		}
	case <-time.After(concurrencyTimeout):
		t.Fatal("metadata refresh did not finish")
	}
	f.metadata.mu.Lock()
	defer f.metadata.mu.Unlock()
	state := f.metadata.state
	assert.Equal(t, flatFixtureName("renamed.txt", false), state.Media["20"].Name)
	assert.NotContains(t, state.Media, api.ID("21"))
	assert.Contains(t, state.Media, api.ID("22"))
	assert.Contains(t, state.Folders, api.Folder{ID: "2", Name: "renamed"})
	assert.NotContains(t, state.Folders, api.Folder{ID: "3", Name: "elsewhere"})
	assert.Contains(t, state.Folders, api.Folder{ID: "4", Name: "created"})
	assert.True(t, f.metadata.dirty, "concurrent writes still need persistence")
	assert.True(t, f.metadata.checked.IsZero(), "an in-flight refresh must not clear a newer invalidation")
}

func TestMetadataRefreshWaitCancellation(t *testing.T) {
	fx := newFlatFixture(t)
	f, ctx := flatTestFs(t, fx, "", true)
	started, unblock := gateRequest(t, fx, func(r *http.Request, _ []byte) bool {
		return r.URL.Path == "/sapi/profile/changes"
	})
	defer unblock()
	f.expireMetadata()
	refresh := make(chan error, 1)
	go func() { refresh <- f.syncMetadata(ctx) }()
	select {
	case <-started:
	case <-time.After(concurrencyTimeout):
		t.Fatal("metadata refresh did not start")
	}
	waiting, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- f.updateMetadata(waiting, true) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(concurrencyTimeout):
		t.Error("cancelled reader waited for the metadata request")
		unblock()
		<-result
	}
	unblock()
	waitConcurrent(t, refresh)
}
