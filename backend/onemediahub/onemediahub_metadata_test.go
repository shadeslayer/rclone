package onemediahub

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func metadataEditsFixture(t *testing.T, fx *flatFixture) *atomic.Int32 {
	t.Helper()
	var calls atomic.Int32
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/sapi/upload/file" || r.URL.Query().Get("action") != "save-metadata" {
				next.ServeHTTP(w, r)
				return
			}
			calls.Add(1)
			var envelope struct {
				Data map[string]json.RawMessage `json:"data"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
			assert.NotContains(t, envelope.Data, "size")
			assert.NotContains(t, envelope.Data, "creationdate")
			var id, name, modified string
			var parent api.ID
			require.NoError(t, json.Unmarshal(envelope.Data["id"], &id))
			require.NoError(t, json.Unmarshal(envelope.Data["name"], &name))
			require.NoError(t, json.Unmarshal(envelope.Data["folderid"], &parent))
			require.NoError(t, json.Unmarshal(envelope.Data["modificationdate"], &modified))
			stamp, err := time.Parse(dateFormat, modified)
			require.NoError(t, err)
			fx.mu.Lock()
			defer fx.mu.Unlock()
			for i := range fx.media {
				if string(fx.media[i].ID) == id {
					fx.media[i].Name, fx.media[i].FolderID, fx.media[i].Modified = name, parent, stamp.UnixMilli()
					jsonReply(t, w, map[string]any{"id": id})
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		})
	})
	return &calls
}

func TestMetadataOnlyFileOperations(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, flat := range []bool{false, true} {
			t.Run("cache="+strconv.FormatBool(cached)+"/flat="+strconv.FormatBool(flat), func(t *testing.T) {
				fx := newFlatFixture(t)
				fx.nextID = 11
				name := "original.txt"
				if flat {
					name = flatFixtureName("old/original.txt", false)
				}
				fx.media = []api.Media{{ID: "10", Name: name, FolderID: "1", Size: 7, Modified: 1700000000000, Type: "file", Status: "V", URL: fx.server.URL + "/content/10", ETag: "unchanged"}}
				fx.content["10"] = "payload"
				calls := metadataEditsFixture(t, fx)
				f, ctx := flatTestFs(t, fx, "", cached, configmap.Simple{"flat_namespace": strconv.FormatBool(flat)})
				original := "original.txt"
				destination := "renamed.txt"
				if flat {
					original, destination = "old/original.txt", "new/"+strings.Repeat("long", 90)+"/renamed.txt"
				}
				source, err := f.NewObject(ctx, original)
				require.NoError(t, err)
				modified := time.Unix(1700004000, 0)
				require.NoError(t, source.SetModTime(ctx, modified))
				assert.Equal(t, modified, source.ModTime(ctx))
				move := f.Features().Move
				require.NotNil(t, move)
				moved, err := move(ctx, source, destination)
				require.NoError(t, err)
				assert.Equal(t, "10", moved.(*Object).ID())
				assert.Equal(t, modified, moved.ModTime(ctx))
				_, err = f.NewObject(ctx, original)
				assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
				found, err := f.NewObject(ctx, destination)
				require.NoError(t, err)
				body, err := found.Open(ctx)
				require.NoError(t, err)
				content, err := io.ReadAll(body)
				require.NoError(t, err)
				require.NoError(t, body.Close())
				assert.Equal(t, "payload", string(content))
				assert.EqualValues(t, 2, calls.Load())
				assert.Equal(t, "unchanged", moved.(*Object).info.ETag)
				assert.Zero(t, fx.folderWrites.Load())
			})
		}
	}
}

func TestMetadataMoveAcrossRoots(t *testing.T) {
	fx := newFlatFixture(t)
	fx.media = []api.Media{{ID: "10", Name: "original", FolderID: "1", Size: 7, Modified: 1700000000000}}
	calls := metadataEditsFixture(t, fx)
	source, ctx := flatTestFs(t, fx, "", true, configmap.Simple{"flat_namespace": "false"})
	destination, _ := flatTestFs(t, fx, "", true, configmap.Simple{"flat_namespace": "false", "root_folder_id": "0"})
	item, err := source.NewObject(ctx, "original")
	require.NoError(t, err)
	moved, err := destination.Features().Move(ctx, item, "renamed")
	require.NoError(t, err)
	assert.Equal(t, "10", moved.(*Object).ID())
	_, err = source.NewObject(ctx, "original")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	_, err = destination.NewObject(ctx, "renamed")
	require.NoError(t, err)
	assert.EqualValues(t, 1, calls.Load())
}

func TestMetadataMoveBetweenFlatCaches(t *testing.T) {
	fx := newFlatFixture(t)
	fx.nextID = 11
	fx.media = []api.Media{{ID: "10", Name: flatFixtureName("original", false), FolderID: "1", Size: 7, Modified: 1700000000000}}
	metadataEditsFixture(t, fx)
	source, ctx := flatTestFs(t, fx, "", true)
	destination, _ := flatTestFs(t, fx, "", true)
	item, err := source.NewObject(ctx, "original")
	require.NoError(t, err)
	name := strings.Repeat("long", 90)
	_, err = destination.Features().Move(ctx, item, name)
	require.NoError(t, err)
	for _, f := range []*Fs{source, destination} {
		_, err = f.NewObject(ctx, name)
		require.NoError(t, err)
		_, err = f.List(ctx, "")
		require.NoError(t, err)
	}
}

func TestMetadataUpdateGuards(t *testing.T) {
	for _, kind := range []string{"destination exists", "stale source", "unfinished upload", "unfinished destination", "different account"} {
		t.Run(kind, func(t *testing.T) {
			fx := newFlatFixture(t)
			fx.media = []api.Media{{ID: "10", Name: "original", FolderID: "1", Size: 7, Modified: 1700000000000}, {ID: "11", Name: "occupied", FolderID: "1", Size: 9}}
			calls := metadataEditsFixture(t, fx)
			extra := configmap.Simple{"flat_namespace": "false"}
			if strings.HasPrefix(kind, "unfinished") {
				extra["resume_uploads"], extra["async_upload"] = "true", "true"
			}
			f, ctx := flatTestFs(t, fx, "", false, extra)
			item, err := f.NewObject(ctx, "original")
			require.NoError(t, err)
			switch kind {
			case "destination exists":
				_, err = f.Features().Move(ctx, item, "occupied")
				require.ErrorContains(t, err, "already exists")
			case "stale source":
				fx.mu.Lock()
				fx.media[0].Name = "changed"
				fx.mu.Unlock()
				require.ErrorContains(t, item.SetModTime(ctx, time.Now()), "since it was read")
				require.ErrorContains(t, item.SetModTime(ctx, time.Now()), "since it was read")
				_, err = f.Features().Move(ctx, item, "renamed")
				require.ErrorContains(t, err, "since it was read")
			case "unfinished upload":
				record := &uploadRecord{Version: uploadJournalVersion, ID: "10", Name: "original", FolderID: "1"}
				require.NoError(t, f.journalDo(true, &uploadJournalOp{key: "test", record: record, write: true}))
				require.ErrorContains(t, item.SetModTime(ctx, time.Now()), "unfinished")
			case "unfinished destination":
				record := &uploadRecord{Version: uploadJournalVersion, ID: "99", Name: "renamed", FolderID: "1"}
				require.NoError(t, f.journalDo(true, &uploadJournalOp{key: "test", record: record, write: true}))
				_, err = f.Features().Move(ctx, item, "renamed")
				require.ErrorContains(t, err, "unfinished")
			case "different account":
				destination, _ := flatTestFs(t, fx, "", false, extra)
				fx.wrapHandler(func(next http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/sapi/profile" {
							jsonReply(t, w, map[string]any{"data": map[string]any{"user": map[string]any{"generic": map[string]string{"userid": r.Header.Get(deviceHeader)}}}})
							return
						}
						next.ServeHTTP(w, r)
					})
				})
				_, err = destination.Features().Move(ctx, item, "renamed")
				require.ErrorIs(t, err, fs.ErrorCantMove)
			}
			assert.Zero(t, calls.Load())
		})
	}
}
