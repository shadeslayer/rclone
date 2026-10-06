package onemediahub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadReplyMetadata(t *testing.T) {
	for _, kind := range []string{"complete", "zero size", "missing size", "null size", "missing parent", "missing modified", "wrong ID", "wrong name", "wrong parent", "wrong size", "duplicate", "deleted", "accepted", "missing status", "conflicting status", "wrong type", "extra source", "conflicting parent", "conflicting etag", "malformed metadata", "malformed status", "malformed etag"} {
		t.Run(kind, func(t *testing.T) {
			fx := newFlatFixture(t)
			modified := time.Unix(1700000000, 0)
			content := "payload"
			if kind == "zero size" {
				content = ""
			}
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("action") != "save" {
						next.ServeHTTP(w, r)
						return
					}
					recorded := httptest.NewRecorder()
					next.ServeHTTP(recorded, r)
					var reply map[string]any
					require.NoError(t, json.Unmarshal(recorded.Body.Bytes(), &reply))
					fx.mu.Lock()
					media := fx.media[len(fx.media)-1]
					fx.mu.Unlock()
					data, err := json.Marshal(media)
					require.NoError(t, err)
					var fields map[string]any
					require.NoError(t, json.Unmarshal(data, &fields))
					fields["status"] = "V"
					reply["status"] = "V"
					switch kind {
					case "missing size":
						delete(fields, "size")
					case "null size":
						fields["size"] = nil
					case "missing parent":
						delete(fields, "folderid")
					case "missing modified":
						delete(fields, "modificationdate")
					case "wrong ID":
						fields["id"] = "999"
					case "wrong name":
						fields["name"] = "other"
					case "wrong parent":
						fields["folderid"] = "3"
					case "wrong size":
						fields["size"] = 999
					case "deleted":
						fields["softdeleted"] = true
					case "accepted":
						fields["status"], reply["status"] = "A", "A"
					case "missing status":
						delete(fields, "status")
						delete(reply, "status")
					case "conflicting status":
						fields["status"] = "U"
					case "wrong type":
						fields["mediatype"] = "picture"
					case "conflicting parent":
						fields["folder"] = "3"
					case "conflicting etag":
						fields["etag"], reply["etag"] = "one", "two"
					case "malformed status":
						reply["status"] = map[string]int{"code": 200}
					case "malformed etag":
						reply["etag"] = 123
					}
					items := []any{fields}
					if kind == "duplicate" {
						items = append(items, fields)
					}
					reply["metadata"] = map[string]any{"files": items}
					if kind == "extra source" {
						reply["metadata"].(map[string]any)["pictures"] = items
					}
					if kind == "malformed metadata" {
						reply["metadata"] = []any{}
					}
					jsonReply(t, w, reply)
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false"})
			item, err := f.Put(ctx, strings.NewReader(content), object.NewStaticObjectInfo("file.txt", modified, int64(len(content)), true, nil, nil))
			require.NoError(t, err)
			assert.EqualValues(t, len(content), item.Size())
			assert.Equal(t, modified, item.ModTime(ctx))
			fx.mu.Lock()
			defer fx.mu.Unlock()
			want := []int{0, 1}
			if kind == "complete" || kind == "zero size" {
				want = []int{0}
			}
			assert.Equal(t, want, fx.mediaBatches, "complete validated upload replies skip the final metadata lookup")
		})
	}
}

func TestUploadMetadataLookupInvalidIDs(t *testing.T) {
	for _, kind := range []string{"unexpected", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/sapi/media" {
						next.ServeHTTP(w, r)
						return
					}
					items := []api.Media{{ID: "42"}, {ID: "42"}}
					if kind == "unexpected" {
						items = []api.Media{{ID: "999"}}
					}
					jsonReply(t, w, map[string]any{"data": map[string]any{"media": items}})
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false"})
			o := &Object{fs: f, info: api.Media{ID: "42"}}
			require.ErrorContains(t, o.waitMetadata(ctx), kind+" ID")
			assert.Equal(t, api.Media{ID: "42"}, o.info)
		})
	}
}

func TestUploadReplyDoesNotCompleteAcceptedContent(t *testing.T) {
	fx := newFlatFixture(t)
	modified := time.Unix(1700000000, 0)
	var polls int
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Query().Get("action") {
			case "save-metadata":
				jsonReply(t, w, map[string]any{"id": "42"})
			case "save":
				fx.mu.Lock()
				fx.media = []api.Media{{ID: "42", FolderID: "1", Name: "file", Size: 1, Modified: modified.UnixMilli(), URL: fx.server.URL + "/content/42", Type: "file", Status: "U"}}
				item := fx.media[0]
				fx.mu.Unlock()
				item.Status = "V"
				jsonReply(t, w, map[string]any{"id": "42", "status": "A", "metadata": map[string]any{"files": []api.Media{item}}})
			case "get-validation-status":
				polls++
				jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]any{{"id": "42", "status": "F"}}}})
			default:
				next.ServeHTTP(w, r)
			}
		})
	})
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
	_, err := f.Put(ctx, strings.NewReader("x"), object.NewStaticObjectInfo("file", modified, 1, true, nil, nil))
	require.ErrorContains(t, err, `returned status "F"`)
	assert.Equal(t, 1, polls)
}

func TestUploadMetadataLookupsBatch(t *testing.T) {
	fx := newFlatFixture(t)
	for id := range 30 {
		fx.media = append(fx.media, api.Media{ID: api.ID(strconv.Itoa(id + 10)), Name: strconv.Itoa(id), FolderID: "1", Size: 1})
	}
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false"})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, info := range fx.media {
		wg.Go(func() {
			<-start
			o := &Object{fs: f, info: info}
			assert.NoError(t, o.waitMetadata(ctx))
			assert.Equal(t, info, o.info)
		})
	}
	close(start)
	wg.Wait()
	fx.mu.Lock()
	defer fx.mu.Unlock()
	assert.Equal(t, []int{30}, fx.mediaBatches)
}

func TestUploadMetadataLookupCancellation(t *testing.T) {
	fx := newFlatFixture(t)
	started := make(chan struct{})
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/sapi/media" {
				next.ServeHTTP(w, r)
				return
			}
			close(started)
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		})
	})
	f, _ := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "42"}}).waitMetadata(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled metadata lookup did not stop")
	}
}
