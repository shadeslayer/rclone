package onemediahub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationSharedSchedule(t *testing.T) {
	fx := newFlatFixture(t)
	firstPoll := make(chan struct{})
	var batches [][]string
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("action") != "get-validation-status" {
				next.ServeHTTP(w, r)
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
			var ids []string
			var statuses []map[string]string
			status := "V"
			if len(batches) == 0 {
				status = "U"
			}
			for _, item := range request.Data.IDs {
				ids = append(ids, item.ID)
				statuses = append(statuses, map[string]string{"id": item.ID, "status": status})
			}
			batches = append(batches, ids)
			jsonReply(t, w, map[string]any{"data": map[string]any{"ids": statuses}})
			if len(batches) == 1 {
				close(firstPoll)
			}
		})
	})
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
	done := make(chan error, 2)
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(ctx, "1") }()
	<-firstPoll
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "2"}}).waitUpload(ctx, "1") }()
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("shared validation did not finish")
		}
	}
	require.Len(t, batches, 2)
	assert.ElementsMatch(t, []string{"1", "2"}, batches[1])
}

func TestValidationIndependentBatches(t *testing.T) {
	for _, mode := range []string{"completed batch", "canceled batch"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFlatFixture(t)
			gate := make(chan []string, 1)
			release := make(chan struct{})
			var released sync.Once
			unblock := func() { released.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls atomic.Int32
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("action") != "get-validation-status" {
						next.ServeHTTP(w, r)
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
					_, _ = io.Copy(io.Discard, r.Body)
					number := calls.Add(1)
					require.LessOrEqual(t, len(request.Data.IDs), pageSize)
					var ids []string
					var statuses []map[string]string
					for _, item := range request.Data.IDs {
						ids = append(ids, item.ID)
						statuses = append(statuses, map[string]string{"id": item.ID, "status": "V"})
					}
					if mode == "completed batch" && number == 2 || mode == "canceled batch" && number == 1 {
						gate <- ids
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": statuses}})
				})
			})
			f, _ := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
			waiters := make(map[string]*validationWaiter)
			cancels := make(map[string]context.CancelFunc)
			for id := range 101 {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				name := strconv.Itoa(id + 1)
				waiter := &validationWaiter{ctx: ctx, id: api.ID(name), folderID: "1", result: make(chan error, 1)}
				waiters[name], cancels[name] = waiter, cancel
				f.validationRequests <- waiter
			}
			var gated []string
			select {
			case gated = <-gate:
			case <-time.After(time.Second):
				t.Fatal("validation batch did not start")
			}
			blocked := make(map[string]bool)
			for _, id := range gated {
				blocked[id] = true
				if mode == "canceled batch" {
					cancels[id]()
				}
			}
			for id, waiter := range waiters {
				if blocked[id] {
					continue
				}
				select {
				case err := <-waiter.result:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("independent batch blocked other uploads")
				}
			}
			unblock()
			assert.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestValidationDuplicateWaiters(t *testing.T) {
	fx := newFlatFixture(t)
	var count int
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("action") != "get-validation-status" {
				next.ServeHTTP(w, r)
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
			count += len(request.Data.IDs)
			jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
		})
	})
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 2 {
		wg.Go(func() { <-start; assert.NoError(t, (&Object{fs: f, info: api.Media{ID: "42"}}).waitUpload(ctx, "1")) })
	}
	close(start)
	wg.Wait()
	assert.Equal(t, 1, count)
}

func TestValidationLateWaiterAfterCancellation(t *testing.T) {
	fx := newFlatFixture(t)
	started := make(chan struct{})
	var calls atomic.Int32
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("action") != "get-validation-status" {
				next.ServeHTTP(w, r)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				close(started)
				<-r.Context().Done()
				return
			}
			jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "2", "status": "V"}}}})
		})
	})
	f, _ := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(ctx, "1") }()
	<-started
	late := &validationWaiter{ctx: context.Background(), id: "2", folderID: "1", result: second}
	f.validationRequests <- late
	cancel()
	select {
	case err := <-first:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("original upload did not cancel")
	}
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("late upload did not resume after canceled cycle")
	}
	assert.EqualValues(t, 2, calls.Load())
}
