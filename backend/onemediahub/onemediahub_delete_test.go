package onemediahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deleteTestFs(t *testing.T, checkers int, handler http.HandlerFunc, overrides ...configmap.Simple) *Fs {
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
	r, err := NewFs(ctx, "delete-test", "", m)
	require.NoError(t, err)
	f := r.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	return f
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
