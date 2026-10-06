package onemediahub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRmdirAmbiguousDeletion(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	for _, cached := range []bool{false, true} {
		for _, root := range []string{"", "parent"} {
			for _, test := range []struct {
				name       string
				changes    map[string]api.Changes
				apply      bool
				wantOK     bool
				status     int
				code       string
				feedError  bool
				incomplete bool
			}{
				{name: "deleted", apply: true, wantOK: true, changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
				{name: "trashed", apply: true, wantOK: true, changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
				{name: "transport", apply: true, wantOK: true, status: http.StatusServiceUnavailable, changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
				{name: "still exists", changes: map[string]api.Changes{}},
				{name: "conflicting changes", changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}, Updated: []api.ID{"10"}}}},
				{name: "other source", changes: map[string]api.Changes{"file": {Deleted: []api.ID{"10"}}}},
				{name: "denied", code: "FOL-1023", changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
				{name: "failed feed", apply: true, feedError: true, changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
				{name: "incomplete feed", apply: true, incomplete: true, changes: map[string]api.Changes{"folder": {Deleted: []api.ID{"10"}}}},
			} {
				t.Run(fmt.Sprintf("cache=%v/root=%q/%s", cached, root, test.name), func(t *testing.T) {
					ctx := context.Background()
					fx := newFixture(t)
					fx.folders = []api.Folder{{ID: "10", Name: "parent", Status: "U"}}
					fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"10"}}}
					fx.requestTime = 1700000000000
					var deletes, confirmations atomic.Int32
					code := test.code
					if code == "" {
						code = "FOL-1000"
					}
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/sapi/media/folder" && r.URL.Query().Get("action") == "delete" {
							body, err := io.ReadAll(r.Body)
							assert.NoError(t, err)
							var request struct {
								Data struct {
									Folders []api.ID `json:"folders"`
								} `json:"data"`
							}
							assert.NoError(t, json.Unmarshal(body, &request))
							assert.Equal(t, []api.ID{"10"}, request.Data.Folders)
							if test.apply {
								r.Body = io.NopCloser(bytes.NewReader(body))
								fx.serve(t, httptest.NewRecorder(), r)
							}
							fx.mu.Lock()
							fx.changes, fx.requestTime = test.changes, 1700000001000
							fx.mu.Unlock()
							deletes.Add(1)
							if test.status != 0 {
								w.WriteHeader(test.status)
							} else {
								jsonReply(t, w, map[string]any{"error": &api.Error{Code: code, Message: "folder error"}})
							}
							return
						}
						if r.URL.Path == "/sapi/profile/changes" && deletes.Load() != 0 {
							confirmations.Add(1)
							if test.feedError {
								jsonReply(t, w, map[string]any{"error": &api.Error{Code: "COM-1011", Message: "failed changes"}})
								return
							}
							if test.incomplete || test.name == "trashed" {
								jsonReply(t, w, map[string]any{"data": map[string]any{"folder": map[string]any{"S": []api.ID{"10"}}}, "more": test.incomplete, "requesttime": "1700000001000"})
								return
							}
						}
						fx.serve(t, w, r)
					}))
					defer srv.Close()
					m := fx.config(t)
					m["url"], m["metadata_cache"] = srv.URL, fmt.Sprint(cached)
					r, err := NewFs(ctx, "rmdir-test", root, m)
					require.NoError(t, err)
					f := r.(*Fs)
					t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
					dir := "parent"
					if root != "" {
						dir = ""
					}
					err = f.Rmdir(ctx, dir)
					if test.wantOK {
						require.NoError(t, err)
					} else if test.status != 0 {
						require.ErrorContains(t, err, http.StatusText(test.status))
					} else {
						var apiErr *api.Error
						require.ErrorAs(t, err, &apiErr)
						assert.Equal(t, code, apiErr.Code)
					}
					assert.EqualValues(t, 1, deletes.Load())
					wantConfirmations := 1
					if test.code != "" {
						wantConfirmations = 0
					}
					assert.EqualValues(t, wantConfirmations, confirmations.Load())
					if cached {
						assert.EqualValues(t, 1700000000000, f.metadata.state.Anchor)
					}
					if test.wantOK {
						assert.ErrorIs(t, f.Rmdir(ctx, dir), fs.ErrorDirNotFound)
						assert.EqualValues(t, 1, deletes.Load())
					}
				})
			}
		}
	}
}

func TestRmdirAccountRoot(t *testing.T) {
	for _, id := range []string{"", "0"} {
		t.Run(id, func(t *testing.T) {
			fx := newFixture(t)
			if id != "" {
				fx.folders = []api.Folder{{ID: api.ID(id), Name: "root", Status: "U"}}
			}
			m := fx.config(t)
			m["root_folder_id"] = id
			r, err := NewFs(context.Background(), "root-test", "", m)
			require.NoError(t, err)
			f := r.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
			require.NoError(t, f.Rmdir(context.Background(), ""))
			fx.mu.Lock()
			defer fx.mu.Unlock()
			if id != "" {
				assert.Len(t, fx.folders, 1)
			}
		})
	}
}

func TestRmdirAmbiguousDeletionCLI(t *testing.T) {
	binary := os.Getenv("RCLONE_ASYNC_DELETE_TEST_BINARY")
	if binary == "" {
		t.Skip("set RCLONE_ASYNC_DELETE_TEST_BINARY to exercise command completion")
	}
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			fx := newFixture(t)
			fx.folders = []api.Folder{{ID: "10", Name: "parent", Status: "U"}, {ID: "11", ParentID: "10", Name: "db", Status: "U"}}
			fx.requestTime = 1700000000000
			fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"10", "11"}}}
			var mu sync.Mutex
			var deleted []api.ID
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sapi/media/folder" || r.URL.Query().Get("action") != "delete" {
					fx.serve(t, w, r)
					return
				}
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				var request struct {
					Data struct {
						Folders []api.ID `json:"folders"`
					} `json:"data"`
				}
				assert.NoError(t, json.Unmarshal(body, &request))
				r.Body = io.NopCloser(bytes.NewReader(body))
				fx.serve(t, httptest.NewRecorder(), r)
				mu.Lock()
				deleted = append(deleted, request.Data.Folders...)
				mu.Unlock()
				fx.mu.Lock()
				change := fx.changes["folder"]
				change.New = slices.DeleteFunc(change.New, func(id api.ID) bool { return slices.Contains(request.Data.Folders, id) })
				change.Deleted = append(change.Deleted, request.Data.Folders...)
				fx.changes["folder"] = change
				fx.requestTime += 1000
				fx.mu.Unlock()
				jsonReply(t, w, map[string]any{"error": &api.Error{Code: "FOL-1000", Message: "Unknown exception in folder handling"}})
			}))
			defer srv.Close()
			args := []string{"purge", ":onemediahub:parent", "--config", "/notfound", "--cache-dir", t.TempDir(),
				"--onemediahub-url", srv.URL, "--onemediahub-auth-type", authPassword, "--onemediahub-user", "test",
				"--onemediahub-password", fx.config(t)["password"], "--onemediahub-async-delete", "--checkers", "1", "--retries", "3"}
			if cached {
				args = append(args, "--onemediahub-metadata-cache")
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
			require.NoError(t, err, "%s", output)
			mu.Lock()
			assert.Equal(t, []api.ID{"11", "10"}, deleted)
			mu.Unlock()
			fx.mu.Lock()
			assert.Empty(t, fx.folders)
			fx.mu.Unlock()
		})
	}
}

func deleteTestFs(t *testing.T, checkers int, handler http.HandlerFunc, overrides ...configmap.Simple) *Fs {
	f, _ := newDeleteTestFs(t, checkers, handler, overrides...)
	return f
}

func newDeleteTestFs(t *testing.T, checkers int, handler http.HandlerFunc, overrides ...configmap.Simple) (*Fs, *fixture) {
	t.Helper()
	fx := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") == "delete" {
			handler(w, r)
			return
		}
		fx.serve(t, w, r)
	}))
	t.Cleanup(srv.Close)
	ctx, ci := fs.AddConfig(context.Background())
	ci.Checkers = checkers
	m := fx.config(t)
	m["url"] = srv.URL
	for _, values := range overrides {
		for key, value := range values {
			m[key] = value
		}
	}
	if m["async_delete"] == "true" {
		ctx = accounting.WithStatsGroup(ctx, t.Name())
	}
	r, err := NewFs(ctx, "delete-test", "", m)
	require.NoError(t, err)
	f := r.(*Fs)
	t.Cleanup(func() {
		err := f.Shutdown(ctx)
		if m["async_delete"] != "true" {
			require.NoError(t, err)
		}
	})
	return f, fx
}

func deleteTestIDs(t *testing.T, r *http.Request) []api.ID {
	t.Helper()
	assert.Equal(t, http.MethodPost, r.Method)
	assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
	var body struct {
		Data map[string][]api.ID `json:"data"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
	typ := r.URL.Path[len("/sapi/media/"):]
	require.Len(t, body.Data, 1)
	return body.Data[typ+"s"]
}

func removeTogether(objects []*Object) <-chan []error {
	done := make(chan []error, 1)
	go func() {
		errors := make([]error, len(objects))
		var wg sync.WaitGroup
		for i, obj := range objects {
			wg.Go(func() { errors[i] = obj.Remove(context.Background()) })
		}
		wg.Wait()
		done <- errors
	}()
	return done
}

func TestDeleteBatchDefault(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var mu sync.Mutex
	batches := map[string][][]api.ID{}
	f := deleteTestFs(t, 8, func(w http.ResponseWriter, r *http.Request) {
		ids := deleteTestIDs(t, r)
		mu.Lock()
		batches[r.URL.Path] = append(batches[r.URL.Path], ids)
		mu.Unlock()
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	})
	var objects []*Object
	for i := range 8 {
		typ := "file"
		if i >= 4 {
			typ = "picture"
		}
		objects = append(objects, &Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: typ}})
	}
	done := removeTogether(objects)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("delete request did not start")
	}
	select {
	case <-done:
		t.Fatal("Remove returned before the server replied")
	default:
	}
	unblock()
	select {
	case results := <-done:
		for _, err := range results {
			require.NoError(t, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batched removes did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 2)
	for _, typ := range []string{"file", "picture"} {
		require.Len(t, batches["/sapi/media/"+typ], 1)
		assert.Len(t, batches["/sapi/media/"+typ][0], 4)
	}
}

func TestDeleteBatchPartialFailures(t *testing.T) {
	var mu sync.Mutex
	deleted := map[api.ID]bool{"1": true}
	var batches [][]api.ID
	f := deleteTestFs(t, 4, func(w http.ResponseWriter, r *http.Request) {
		ids := deleteTestIDs(t, r)
		mu.Lock()
		defer mu.Unlock()
		batches = append(batches, ids)
		if len(ids) > 1 && slices.Contains(ids, api.ID("1")) {
			// Simulate a batch that deleted an item before hitting the trash entry.
			deleted["3"] = true
			jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1022", Message: "already soft deleted"}})
			return
		}
		for _, id := range ids {
			if id == "2" {
				jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1034", Message: "access denied"}})
				return
			}
			if deleted[id] {
				jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1022", Message: "already soft deleted"}})
				return
			}
			deleted[id] = true
		}
	})
	f.metadata = &metadataCache{state: &metadataSnapshot{Media: map[api.ID]api.Media{}}, byName: map[mediaKey]api.ID{}, checked: time.Now()}
	var objects []*Object
	for i := range 4 {
		info := api.Media{ID: api.ID(fmt.Sprint(i + 1)), Name: fmt.Sprint(i + 1), Type: "file"}
		f.cacheMedia(info, false)
		objects = append(objects, &Object{fs: f, info: info})
	}
	results := <-removeTogether(objects)
	for i, err := range results {
		if i == 1 {
			var apiErr *api.Error
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "MED-1034", apiErr.Code)
		} else {
			assert.NoError(t, err)
		}
	}
	mu.Lock()
	assert.Len(t, batches[0], 4)
	assert.Equal(t, map[api.ID]bool{"1": true, "3": true, "4": true}, deleted)
	mu.Unlock()
	f.metadata.mu.Lock()
	defer f.metadata.mu.Unlock()
	assert.Len(t, f.metadata.state.Media, 1)
	assert.Contains(t, f.metadata.state.Media, api.ID("2"))
	assert.True(t, f.metadata.checked.IsZero())
}

func TestDeleteBatchDoesNotSplitGlobalErrors(t *testing.T) {
	for _, code := range []string{"SEC-1001", "MED-1000", "MED-1007", "MED-1034", "MED-9999"} {
		t.Run(code, func(t *testing.T) {
			var requests atomic.Int32
			f := deleteTestFs(t, 4, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				assert.Len(t, deleteTestIDs(t, r), 4)
				jsonReply(t, w, map[string]any{"error": &api.Error{Code: code, Message: "batch rejected"}})
			})
			var objects []*Object
			for i := range 4 {
				objects = append(objects, &Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}})
			}
			for _, err := range <-removeTogether(objects) {
				var apiErr *api.Error
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, code, apiErr.Code)
			}
			assert.EqualValues(t, 1, requests.Load())
		})
	}
}

func TestDeleteBatchCancelledPartialResultExpiresCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests atomic.Int32
	f := deleteTestFs(t, 2, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		ids := deleteTestIDs(t, r)
		if len(ids) == 2 {
			cancel()
		} else {
			assert.Equal(t, []api.ID{"2"}, ids)
		}
		jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1022", Message: "already soft deleted"}})
	})
	f.metadata = &metadataCache{state: &metadataSnapshot{Media: map[api.ID]api.Media{}}, byName: map[mediaKey]api.ID{}, checked: time.Now()}
	objects := []*Object{{fs: f, info: api.Media{ID: "1", Name: "cancelled", Type: "file"}}, {fs: f, info: api.Media{ID: "2", Name: "confirmed", Type: "file"}}}
	for _, obj := range objects {
		f.cacheMedia(obj.info, false)
	}
	first := make(chan error, 1)
	go func() { first <- objects[0].Remove(ctx) }()
	require.NoError(t, objects[1].Remove(context.Background()))
	assert.ErrorIs(t, <-first, context.Canceled)
	assert.EqualValues(t, 2, requests.Load())
	f.metadata.mu.Lock()
	defer f.metadata.mu.Unlock()
	assert.Contains(t, f.metadata.state.Media, api.ID("1"))
	assert.NotContains(t, f.metadata.state.Media, api.ID("2"))
	assert.True(t, f.metadata.checked.IsZero())
}

func TestDeleteBatchDoesNotReplayTransportFailures(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			f := deleteTestFs(t, 4, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				deleteTestIDs(t, r)
				w.WriteHeader(status)
			})
			var objects []*Object
			for i := range 4 {
				objects = append(objects, &Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}})
			}
			for _, err := range <-removeTogether(objects) {
				require.Error(t, err)
			}
			assert.EqualValues(t, 1, requests.Load())
		})
	}
}

func TestDeleteBatchDuplicateAndCancelled(t *testing.T) {
	var requests atomic.Int32
	f := deleteTestFs(t, 4, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, []api.ID{"1"}, deleteTestIDs(t, r))
	})
	obj := &Object{fs: f, info: api.Media{ID: "1", Type: "file"}}
	for _, err := range <-removeTogether([]*Object{obj, obj, obj, obj}) {
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, requests.Load())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, obj.Remove(ctx), context.Canceled)
	assert.EqualValues(t, 1, requests.Load())
}

func TestDeleteBatchCancellationAndShutdown(t *testing.T) {
	started, finished := make(chan struct{}), make(chan struct{})
	f := deleteTestFs(t, 1, func(w http.ResponseWriter, r *http.Request) {
		deleteTestIDs(t, r)
		close(started)
		<-r.Context().Done()
		close(finished)
	})
	ctx, cancel := context.WithCancel(context.Background())
	obj := &Object{fs: f, info: api.Media{ID: "1", Type: "file"}}
	done := make(chan error, 1)
	go func() { done <- obj.Remove(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not start")
	}
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled delete did not finish")
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled request remained active")
	}
	require.NoError(t, f.Shutdown(context.Background()))
	assert.Error(t, obj.Remove(context.Background()))
}

func TestDeleteBatchSizeOne(t *testing.T) {
	var requests atomic.Int32
	f := deleteTestFs(t, 8, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Len(t, deleteTestIDs(t, r), 1)
	}, configmap.Simple{"delete_batch_size": "1"})
	objects := []*Object{{fs: f, info: api.Media{ID: "1", Type: "audio"}}, {fs: f, info: api.Media{ID: "2", Type: "video"}}}
	for _, err := range <-removeTogether(objects) {
		require.NoError(t, err)
	}
	assert.EqualValues(t, 2, requests.Load())
}

func TestDeleteBatchMaximum(t *testing.T) {
	var mu sync.Mutex
	var batches [][]api.ID
	f := deleteTestFs(t, 2000, func(w http.ResponseWriter, r *http.Request) {
		ids := deleteTestIDs(t, r)
		assert.LessOrEqual(t, len(ids), 1000)
		mu.Lock()
		batches = append(batches, ids)
		mu.Unlock()
	})
	var objects []*Object
	for i := range 1100 {
		objects = append(objects, &Object{fs: f, info: api.Media{ID: api.ID(fmt.Sprint(i + 1)), Type: "file"}})
	}
	for _, err := range <-removeTogether(objects) {
		require.NoError(t, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var count, largest int
	for _, ids := range batches {
		count += len(ids)
		largest = max(largest, len(ids))
	}
	assert.Equal(t, 1100, count)
	assert.Greater(t, largest, 100)
	_, err := readOptions(testConfig(t, configmap.Simple{"delete_batch_size": "1001"}))
	assert.ErrorContains(t, err, "1000")
	_, err = readOptions(testConfig(t, configmap.Simple{"delete_batch_size": "0"}))
	assert.Error(t, err)
}

func TestDeleteBatchMixedGroupFailures(t *testing.T) {
	f := deleteTestFs(t, 2, func(w http.ResponseWriter, r *http.Request) {
		assert.Len(t, deleteTestIDs(t, r), 1)
		if r.URL.Path == "/sapi/media/file" {
			jsonReply(t, w, map[string]any{"error": &api.Error{Code: "MED-1034", Message: "access denied"}})
		}
	})
	objects := []*Object{{fs: f, info: api.Media{ID: "1", Type: "file"}}, {fs: f, info: api.Media{ID: "2", Type: "picture"}}}
	results := <-removeTogether(objects)
	assert.Error(t, results[0])
	assert.NoError(t, results[1])
}

func TestDeleteBatchQueuedCancellationAndDrain(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var requests atomic.Int32
	f := deleteTestFs(t, 2, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, "/sapi/media/file", r.URL.Path)
		assert.Equal(t, []api.ID{"1"}, deleteTestIDs(t, r))
		close(started)
		<-release
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	file := &Object{fs: f, info: api.Media{ID: "1", Type: "file"}}
	picture := &Object{fs: f, info: api.Media{ID: "2", Type: "picture"}}
	fileDone, pictureDone := make(chan error, 1), make(chan error, 1)
	go func() { fileDone <- file.Remove(context.Background()) }()
	go func() { pictureDone <- picture.Remove(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("file delete did not start")
	}
	cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- f.Shutdown(context.Background()) }()
	select {
	case <-shutdown:
		t.Fatal("Shutdown returned before the server replied")
	default:
	}
	unblock()
	select {
	case err := <-fileDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("live file delete did not finish")
	}
	select {
	case err := <-pictureDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("queued cancellation did not finish")
	}
	select {
	case err := <-shutdown:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not drain deletion batch")
	}
	assert.EqualValues(t, 1, requests.Load())
	assert.Error(t, file.Remove(context.Background()))
}
