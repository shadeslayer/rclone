package onemediahub

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func asyncDeleteContext(t *testing.T) context.Context {
	t.Helper()
	previous := fs.CountError
	fs.CountError = func(ctx context.Context, err error) error { return accounting.Stats(ctx).Error(err) }
	t.Cleanup(func() { fs.CountError = previous })
	return accounting.WithStatsGroup(context.Background(), t.Name())
}

func TestAsyncDeleteQueueAndDrain(t *testing.T) {
	ctx := asyncDeleteContext(t)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		assert.Len(t, deleteTestIDs(t, r), 4)
		close(started)
		<-release
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "4"})
	f.metadata = &metadataCache{state: &metadataSnapshot{Media: map[api.ID]api.Media{}}, byName: map[mediaKey]api.ID{}}
	var objects []*Object
	for i := range 4 {
		info := api.Media{ID: api.ID(fmt.Sprint(i + 1)), Name: fmt.Sprint(i + 1), Type: "file"}
		f.cacheMedia(info, false)
		objects = append(objects, &Object{fs: f, info: info})
	}
	queued := make(chan error, 1)
	go func() {
		for _, obj := range objects {
			if err := obj.Remove(ctx); err != nil {
				queued <- err
				return
			}
		}
		queued <- nil
	}()
	select {
	case err := <-queued:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("async Remove waited for the server response")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch did not start")
	}
	f.metadata.mu.Lock()
	assert.Len(t, f.metadata.state.Media, 4)
	f.metadata.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- f.Shutdown(ctx) }()
	select {
	case <-done:
		t.Fatal("Shutdown did not wait for deletion confirmation")
	default:
	}
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not drain the queue")
	}
	f.metadata.mu.Lock()
	assert.Empty(t, f.metadata.state.Media)
	f.metadata.mu.Unlock()
	assert.Error(t, objects[0].Remove(ctx))
}

func TestAsyncDeleteServerLimit(t *testing.T) {
	ctx := asyncDeleteContext(t)
	var mu sync.Mutex
	var accepted []api.ID
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		ids := deleteTestIDs(t, r)
		if len(ids) > 2 {
			jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1025", Message: "over the limit"}})
			return
		}
		mu.Lock()
		accepted = append(accepted, ids...)
		mu.Unlock()
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "4"})
	for i := range 8 {
		require.NoError(t, (&Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}}).Remove(ctx))
	}
	require.NoError(t, f.Shutdown(ctx))
	assert.Zero(t, accounting.Stats(ctx).GetErrors())
	assert.False(t, accounting.Stats(ctx).HadFatalError())
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []api.ID{"1", "2", "3", "4", "5", "6", "7", "8"}, accepted)
}

func TestAsyncDeleteFailureAccounting(t *testing.T) {
	ctx := asyncDeleteContext(t)
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		assert.Len(t, deleteTestIDs(t, r), 2)
		jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1034", Message: "access denied"}})
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "2"})
	for i := range 2 {
		require.NoError(t, (&Object{fs: f, remote: fmt.Sprint(i), info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}}).Remove(ctx))
	}
	err := f.Shutdown(ctx)
	require.Error(t, err)
	assert.True(t, fserrors.IsFatalError(err))
	stats := accounting.Stats(ctx)
	assert.EqualValues(t, 2, stats.GetErrors())
	assert.True(t, stats.HadFatalError())
	assert.Error(t, (&Object{fs: f, info: api.Media{ID: "3", Type: "file"}}).Remove(ctx))
	assert.EqualValues(t, 2, stats.GetErrors())
	stats.ResetErrors()
	_ = stats.Error(fmt.Errorf("another retryable error"))
	require.Error(t, f.Shutdown(ctx))
	assert.EqualValues(t, 2, stats.GetErrors())
	assert.True(t, stats.HadFatalError())
}

func TestAsyncDeleteSurvivesCallerCancellation(t *testing.T) {
	parent := asyncDeleteContext(t)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		assert.Len(t, deleteTestIDs(t, r), 1)
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			t.Error("accepted deletion inherited caller cancellation")
		}
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "1"})
	done := make(chan error, 1)
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "1", Type: "file"}}).Remove(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("async deletion was not admitted")
	}
	<-started
	cancel()
	assert.ErrorIs(t, (&Object{fs: f, info: api.Media{ID: "2", Type: "file"}}).Remove(ctx), context.Canceled)
	unblock()
	require.NoError(t, f.Shutdown(parent))
}

func TestDeleteChangesConfirmation(t *testing.T) {
	for _, test := range []struct {
		name         string
		changes      map[string]api.Changes
		status       int
		wantRequests int
		wantError    bool
		firstError   bool
		errorCode    string
		invalidTime  bool
	}{
		{"trash", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}}, 0, 2, false, false, "", false},
		{"conflict", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}, Updated: []api.ID{"1"}}}, 0, 3, false, false, "", false},
		{"other type", map[string]api.Changes{"picture": {Deleted: []api.ID{"1"}}}, 0, 3, false, false, "", false},
		{"ambiguous response", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}}, http.StatusServiceUnavailable, 1, true, false, "", false},
		{"changed media type", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}, "picture": {Updated: []api.ID{"1"}}}, http.StatusServiceUnavailable, 1, true, true, "", false},
		{"unknown exception partial", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}}, 0, 1, true, false, "MED-1000", false},
		{"unknown exception empty", map[string]api.Changes{}, 0, 1, true, true, "MED-1000", false},
		{"unknown exception conflict", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}, "picture": {Updated: []api.ID{"1"}}}, 0, 1, true, true, "MED-1000", false},
		{"unknown exception invalid feed", map[string]api.Changes{"file": {Deleted: []api.ID{"1"}}}, 0, 1, true, true, "MED-1000", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var batches [][]api.ID
			f, fx := newDeleteTestFs(t, 2, func(w http.ResponseWriter, r *http.Request) {
				ids := deleteTestIDs(t, r)
				mu.Lock()
				batches = append(batches, ids)
				mu.Unlock()
				if test.status != 0 {
					w.WriteHeader(test.status)
					return
				}
				if test.errorCode != "" {
					jsonReply(t, w, map[string]any{"error": &api.Error{Code: test.errorCode, Message: "unknown exception"}})
					return
				}
				if len(ids) > 1 {
					jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1022", Message: "already in trash"}})
				}
			})
			fx.requestTime = 1700000001000
			if test.invalidTime {
				fx.requestTime = 0
			}
			fx.changes = test.changes
			f.metadata = &metadataCache{state: &metadataSnapshot{Anchor: 1700000000000, Media: map[api.ID]api.Media{}}, byName: map[mediaKey]api.ID{}}
			objects := []*Object{{fs: f, info: api.Media{ID: "1", Type: "file"}}, {fs: f, info: api.Media{ID: "2", Type: "file"}}}
			results := <-removeTogether(objects)
			if test.firstError {
				assert.Error(t, results[0])
			} else {
				assert.NoError(t, results[0])
			}
			if test.wantError {
				assert.Error(t, results[1])
				if test.errorCode != "" {
					var apiErr *api.Error
					require.ErrorAs(t, results[1], &apiErr)
					assert.Equal(t, test.errorCode, apiErr.Code)
				}
			} else {
				assert.NoError(t, results[1])
			}
			mu.Lock()
			assert.Len(t, batches, test.wantRequests)
			mu.Unlock()
			fx.mu.Lock()
			assert.Equal(t, 1, fx.requests["/sapi/profile/changes"])
			fx.mu.Unlock()
		})
	}
}

func TestAsyncDeleteRejectsTrashStatus(t *testing.T) {
	ctx := asyncDeleteContext(t)
	var fx *fixture
	f, fx := newDeleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, []api.ID{"20"}, deleteTestIDs(t, r))
		fx.mu.Lock()
		fx.media[0].Status = "S"
		fx.mu.Unlock()
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "1"})
	fx.media = []api.Media{{ID: "20", Name: "file.txt", Type: "file", Status: "U"}}
	obj, err := f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	require.NoError(t, obj.Remove(ctx))
	src := object.NewStaticObjectInfo("file.txt", time.Now(), 1, true, nil, f)
	assert.ErrorIs(t, obj.Update(ctx, strings.NewReader("x"), src), fs.ErrorObjectNotFound)
	_, err = obj.Open(ctx)
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestAsyncDeleteFileReplacementAndRmdir(t *testing.T) {
	ctx := asyncDeleteContext(t)
	fx := newFixture(t)
	fx.folders = []api.Folder{{ID: "10", Name: "directory"}}
	fx.media = []api.Media{{ID: "20", FolderID: "10", Name: "file.txt", Type: "file", Size: 1}}
	fx.content["20"] = "x"
	fx.nextID = 30
	m := fx.config(t)
	m["async_delete"] = "true"
	r, err := NewFs(ctx, "async-files", "", m)
	require.NoError(t, err)
	f := r.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	obj, err := f.NewObject(ctx, "directory/file.txt")
	require.NoError(t, err)
	require.NoError(t, obj.Remove(ctx))
	src := object.NewStaticObjectInfo("directory/file.txt", time.Now(), 1, true, nil, f)
	assert.ErrorIs(t, obj.Update(ctx, strings.NewReader("z"), src), fs.ErrorObjectNotFound)
	_, err = obj.Open(ctx)
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	replacement, err := f.Put(ctx, strings.NewReader("y"), src)
	require.NoError(t, err)
	assert.NotEqual(t, obj.(*Object).ID(), replacement.(*Object).ID())
	require.NoError(t, replacement.Remove(ctx))
	require.NoError(t, f.Rmdir(ctx, "directory"))
	fx.mu.Lock()
	defer fx.mu.Unlock()
	assert.Empty(t, fx.media)
	assert.Empty(t, fx.folders)
}

func TestAsyncDeleteFlatRmdir(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			fx := newFlatFixture(t)
			f, ctx := flatTestFs(t, fx, "", cached, configmap.Simple{"async_delete": "true"})
			require.NoError(t, f.Mkdir(ctx, "empty"))
			require.NoError(t, f.Rmdir(ctx, "empty"))
			fx.mu.Lock()
			assert.Empty(t, fx.media)
			fx.mu.Unlock()
			_, err := f.List(ctx, "empty")
			assert.ErrorIs(t, err, fs.ErrorDirNotFound)
			assert.Zero(t, fx.folderWrites.Load())
		})
	}
}

func TestAsyncDeleteCLI(t *testing.T) {
	binary := os.Getenv("RCLONE_ASYNC_DELETE_TEST_BINARY")
	if binary == "" {
		t.Skip("requires the freshly built rclone binary")
	}
	for _, test := range []struct {
		name    string
		status  int
		noCache bool
		sync    bool
		limit   int
	}{
		{"delete", 0, false, false, 0}, {"sync", 0, false, true, 0},
		{"delete with server limit", 0, false, false, 1}, {"sync with server limit", 0, false, true, 1},
		{"forbidden", http.StatusForbidden, false, false, 0},
		{"late failure without fs cache", http.StatusForbidden, true, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fx := newFixture(t)
			for i := range 3 {
				id := api.ID(fmt.Sprint(i + 1))
				fx.media = append(fx.media, api.Media{ID: id, Name: fmt.Sprintf("%d.txt", i), Type: "file", Size: 1})
				fx.content[id] = "x"
			}
			var mu sync.Mutex
			var sizes []int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") == "delete" {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					r.Body = io.NopCloser(bytes.NewReader(body))
					ids := deleteTestIDs(t, r)
					mu.Lock()
					sizes = append(sizes, len(ids))
					mu.Unlock()
					if test.status != 0 {
						w.WriteHeader(test.status)
						return
					}
					if test.limit > 0 && len(ids) > test.limit {
						jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1025", Message: "over the limit"}})
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(body))
				}
				fx.serve(t, w, r)
			}))
			defer srv.Close()
			args := []string{"delete", ":onemediahub:"}
			if test.sync {
				args = []string{"sync", t.TempDir(), ":onemediahub:", "--delete-after"}
			}
			args = append(args, "--config", "/notfound", "--cache-dir", t.TempDir(), "--onemediahub-url", srv.URL,
				"--onemediahub-auth-type", authPassword, "--onemediahub-user", "test", "--onemediahub-password", fx.config(t)["password"],
				"--onemediahub-async-delete", "--checkers", "1", "--retries", "1")
			if test.noCache {
				args = append(args, "--fs-cache-expire-duration", "0")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, binary, args...)
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "RCLONE_") {
					child.Env = append(child.Env, entry)
				}
			}
			output, err := child.CombinedOutput()
			if test.status == 0 {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err, "%s", output)
				assert.Contains(t, string(output), "asynchronous deletion")
			}
			mu.Lock()
			if test.limit > 0 {
				assert.Equal(t, []int{3, 1, 1, 1}, sizes)
			} else {
				assert.Equal(t, []int{3}, sizes)
			}
			mu.Unlock()
			fx.mu.Lock()
			if test.status == 0 {
				assert.Empty(t, fx.media)
			} else {
				assert.Len(t, fx.media, 3)
			}
			fx.mu.Unlock()
		})
	}
}

func TestAsyncDeleteMaximum(t *testing.T) {
	ctx := asyncDeleteContext(t)
	var mu sync.Mutex
	var count, largest int
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		ids := deleteTestIDs(t, r)
		assert.LessOrEqual(t, len(ids), 1000)
		mu.Lock()
		count += len(ids)
		largest = max(largest, len(ids))
		mu.Unlock()
	}, configmap.Simple{"async_delete": "true"})
	for i := range 1100 {
		require.NoError(t, (&Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}}).Remove(ctx))
	}
	require.NoError(t, f.Shutdown(ctx))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1100, count)
	assert.Greater(t, largest, 100)
}

func TestAsyncDeleteCancelledBarrier(t *testing.T) {
	ctx := asyncDeleteContext(t)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		deleteTestIDs(t, r)
		close(started)
		<-release
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "1"})
	require.NoError(t, (&Object{fs: f, info: api.Media{ID: "1", Type: "file"}}).Remove(ctx))
	<-started
	limited, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := f.List(limited, "")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	unblock()
	done := make(chan error, 1)
	go func() { done <- f.Shutdown(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cancelled barrier blocked queue draining")
	}
}

func TestAsyncDeleteCancelledFullQueue(t *testing.T) {
	ctx := asyncDeleteContext(t)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	start := sync.OnceFunc(func() { close(started) })
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		deleteTestIDs(t, r)
		start()
		<-release
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "1"})
	remove := func(ctx context.Context, id api.ID) error {
		return (&Object{fs: f, info: api.Media{ID: id, Type: "file"}}).Remove(ctx)
	}
	require.NoError(t, remove(ctx, "1"))
	<-started
	require.NoError(t, remove(ctx, "2"))
	limited, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, remove(limited, "3"), context.DeadlineExceeded)
	f.deleteMu.Lock()
	assert.Equal(t, 2, f.deletePending)
	f.deleteMu.Unlock()
	unblock()
	require.NoError(t, f.Shutdown(ctx))
	f.deleteMu.Lock()
	assert.Zero(t, f.deletePending)
	f.deleteMu.Unlock()
}

func TestAsyncDeleteBarrierDuringShutdown(t *testing.T) {
	ctx := asyncDeleteContext(t)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		deleteTestIDs(t, r)
		close(started)
		<-release
	}, configmap.Simple{"async_delete": "true", "delete_batch_size": "1"})
	require.NoError(t, (&Object{fs: f, info: api.Media{ID: "1", Type: "file"}}).Remove(ctx))
	<-started
	barrier, shutdown := make(chan error, 1), make(chan error, 1)
	go func() { barrier <- f.flushDeletions(ctx) }()
	go func() { shutdown <- f.Shutdown(ctx) }()
	unblock()
	select {
	case <-barrier:
	case <-time.After(time.Second):
		t.Fatal("barrier blocked during Shutdown")
	}
	select {
	case err := <-shutdown:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Shutdown blocked with a concurrent barrier")
	}
}
