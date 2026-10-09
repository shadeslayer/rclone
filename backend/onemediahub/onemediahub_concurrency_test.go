package onemediahub

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const concurrencyTimeout = 5 * time.Second

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
