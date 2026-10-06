package onemediahub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/crypt"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	fscache "github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uploadSourceFixture(t *testing.T, fx *flatFixture) func() []string {
	t.Helper()
	var paths []string
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			action := r.URL.Query().Get("action")
			if action == "get-validation-status" {
				var request struct {
					Data struct {
						IDs []struct {
							ID string `json:"id"`
						} `json:"ids"`
					} `json:"data"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				var statuses []map[string]string
				for _, item := range request.Data.IDs {
					statuses = append(statuses, map[string]string{"id": item.ID, "status": "V"})
				}
				jsonReply(t, w, map[string]any{"data": map[string]any{"ids": statuses}})
				return
			}
			if !strings.HasPrefix(r.URL.Path, "/sapi/upload") {
				next.ServeHTTP(w, r)
				return
			}
			kind := strings.TrimPrefix(r.URL.Path, "/sapi/upload/")
			if kind == "/sapi/upload" {
				kind = "file"
			}
			fx.mu.Lock()
			paths = append(paths, r.URL.Path+"?"+action)
			fx.mu.Unlock()
			var data api.Upload
			var content string
			if action == "save-metadata" {
				var request struct {
					Data api.Upload `json:"data"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				data = request.Data
			} else if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
				require.NoError(t, r.ParseMultipartForm(1<<20))
				defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
				var request struct {
					Data api.Upload `json:"data"`
				}
				require.NoError(t, json.Unmarshal([]byte(r.FormValue("data")), &request))
				data = request.Data
				reader, _, err := r.FormFile("file")
				require.NoError(t, err)
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				content = string(body)
			} else {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				content = string(body)
				data.ID = r.Header.Get("X-funambol-id")
			}
			fx.mu.Lock()
			defer fx.mu.Unlock()
			index := -1
			for i, item := range fx.media {
				if string(item.ID) == data.ID {
					index = i
					break
				}
			}
			if index < 0 {
				require.NotEqual(t, "", data.Name)
				data.ID = strconv.Itoa(fx.nextID)
				fx.nextID++
				index = len(fx.media)
				fx.media = append(fx.media, api.Media{ID: api.ID(data.ID), Type: kind, URL: fx.server.URL + "/content/" + data.ID})
			}
			item := fx.media[index]
			assert.Equal(t, kind, item.Type, "an existing ID must keep its original source route")
			if data.Name != "" {
				item.Name, item.FolderID = data.Name, data.FolderID
				modified, err := time.Parse(dateFormat, data.Modified)
				require.NoError(t, err)
				item.Modified = modified.UnixMilli()
				if data.Created != "" {
					item.Size = data.Size
				}
			}
			if action == "save" {
				fx.content[item.ID] = content
			}
			item.Status = "U"
			fx.media[index] = item
			jsonReply(t, w, map[string]any{"id": data.ID, "status": "U", "metadata": map[string]any{kind + "s": []api.Media{item}}})
		})
	})
	return func() []string { fx.mu.Lock(); defer fx.mu.Unlock(); return append([]string(nil), paths...) }
}

func TestUploadSourceRoutes(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, tc := range []struct{ name, setting, kind string }{{"photo.png", "auto", "picture"}, {"song.wav", "auto", "audio"}, {"clip.mp4", "auto", "video"}, {"other.bin", "auto", "file"}, {"photo.png", "file", "file"}} {
			t.Run(tc.name+"/"+tc.setting+"/async="+strconv.FormatBool(async), func(t *testing.T) {
				fx := newFlatFixture(t)
				paths := uploadSourceFixture(t, fx)
				f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": tc.setting, "async_upload": strconv.FormatBool(async)})
				modified := time.Unix(1700000000, 0)
				src := object.NewStaticObjectInfo(tc.name, modified, 7, true, nil, nil)
				item, err := f.Put(ctx, strings.NewReader("payload"), src)
				require.NoError(t, err)
				assert.Equal(t, tc.kind, item.(*Object).info.Type)
				if tc.setting == "auto" {
					_, err = f.Features().Move(ctx, item, "renamed.bin")
					require.NoError(t, err)
					item, err = f.NewObject(ctx, "renamed.bin")
					require.NoError(t, err)
					require.NoError(t, item.SetModTime(ctx, modified.Add(time.Hour)))
					src = object.NewStaticObjectInfo("renamed.bin", modified, 7, true, nil, nil)
					require.NoError(t, item.Update(ctx, strings.NewReader("changed"), src))
					assert.Equal(t, tc.kind, item.(*Object).info.Type)
				}
				for _, request := range paths() {
					if tc.kind == "file" && request == "/sapi/upload?save" {
						continue
					}
					assert.True(t, strings.HasPrefix(request, "/sapi/upload/"+tc.kind+"?"), request)
				}
			})
		}
	}
}

type transformedSourceInfo struct{ fs.ObjectInfo }

func (o transformedSourceInfo) Fs() fs.Info { return transformedSourceFs{o.ObjectInfo.Fs()} }

type transformedSourceFs struct{ fs.Info }

func (f transformedSourceFs) Features() *fs.Features { return &fs.Features{Overlay: true} }

func TestUploadTransformedSourceUsesFile(t *testing.T) {
	fx := newFlatFixture(t)
	paths := uploadSourceFixture(t, fx)
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto"})
	src := transformedSourceInfo{object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), 7, true, nil, f)}
	item, err := f.Put(ctx, strings.NewReader("encoded"), src)
	require.NoError(t, err)
	assert.Equal(t, "file", item.(*Object).info.Type)
	for _, request := range paths() {
		assert.NotContains(t, request, "/picture")
	}
}

func TestUploadCryptSourceUsesFile(t *testing.T) {
	for _, mode := range []string{"off", "standard"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFlatFixture(t)
			paths := uploadSourceFixture(t, fx)
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto"})
			m := flatCryptConfig(t, mode)
			m["suffix"] = "none"
			m["remote"] = fs.ConfigString(f)
			fscache.Put(m["remote"], f)
			t.Cleanup(func() { fscache.ClearConfig(f.Name()) })
			wrapped, err := crypt.NewFs(ctx, "crypt-source-test", "", m)
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), 7, true, nil, wrapped)
			item, err := wrapped.Put(ctx, strings.NewReader("payload"), src)
			require.NoError(t, err)
			reader, err := item.Open(ctx)
			require.NoError(t, err)
			content, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			assert.Equal(t, "payload", string(content))
			for _, request := range paths() {
				assert.NotContains(t, request, "/picture")
			}
		})
	}
}

func TestUploadSourceRecoveryKeepsRoute(t *testing.T) {
	fx := newFlatFixture(t)
	fx.nextID = 42
	paths := uploadSourceFixture(t, fx)
	failed := false
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("action") != "save" || !strings.HasPrefix(r.URL.Path, "/sapi/upload") {
				next.ServeHTTP(w, r)
				return
			}
			assert.Equal(t, "/sapi/upload/picture", r.URL.Path)
			if strings.HasPrefix(r.Header.Get("Content-Range"), "bytes */") {
				w.Header().Set("Range", "bytes=0-2")
				w.WriteHeader(http.StatusPermanentRedirect)
				return
			}
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			if !failed {
				assert.Equal(t, "abcdef", string(body))
				failed = true
				r.Body = io.NopCloser(strings.NewReader("abc"))
				next.ServeHTTP(httptest.NewRecorder(), r)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			assert.Equal(t, "def", string(body))
			assert.Equal(t, "bytes 3-5/6", r.Header.Get("Content-Range"))
			r.Body = io.NopCloser(strings.NewReader("abcdef"))
			next.ServeHTTP(w, r)
		})
	})
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": "true", "resume_uploads": "true"})
	source, _ := recoverySource(t)
	src := fs.NewOverrideRemote(source, "photo.png")
	reader, err := source.Open(ctx)
	require.NoError(t, err)
	_, err = f.Put(ctx, reader, src)
	require.Error(t, err)
	require.NoError(t, reader.Close())
	records, err := f.pendingUploads()
	require.NoError(t, err)
	require.Len(t, records, 1)
	for _, record := range records {
		assert.Equal(t, "picture", record.MediaType)
	}
	f.opt.UploadSource = "file"
	reader, err = source.Open(ctx)
	require.NoError(t, err)
	item, err := f.Put(ctx, reader, src)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "picture", item.(*Object).info.Type)
	records, err = f.pendingUploads()
	require.NoError(t, err)
	assert.Empty(t, records)
	for _, request := range paths() {
		assert.Contains(t, request, "/picture")
	}
}

func TestUploadCryptRejectsSpecializedReplacement(t *testing.T) {
	fx := newFlatFixture(t)
	paths := uploadSourceFixture(t, fx)
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto"})
	modified := time.Unix(1700000000, 0)
	src := object.NewStaticObjectInfo("photo.png", modified, 7, true, nil, f)
	_, err := f.Put(ctx, strings.NewReader("picture"), src)
	require.NoError(t, err)
	before := paths()
	m := flatCryptConfig(t, "off")
	m["suffix"] = "none"
	m["remote"] = fs.ConfigString(f)
	fscache.Put(m["remote"], f)
	t.Cleanup(func() { fscache.ClearConfig(f.Name()) })
	wrapped, err := crypt.NewFs(ctx, "crypt-source-replace-test", "", m)
	require.NoError(t, err)
	src = object.NewStaticObjectInfo("photo.png", modified, 7, true, nil, wrapped)
	_, err = wrapped.Put(ctx, strings.NewReader("encoded"), src)
	require.ErrorContains(t, err, "transformed content")
	assert.Equal(t, before, paths(), "rejection must precede upload mutation")
}

func TestUploadSourceRecoveryTypeMismatch(t *testing.T) {
	fx := newFlatFixture(t)
	paths := uploadSourceFixture(t, fx)
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": "true", "resume_uploads": "true"})
	source, _ := recoverySource(t)
	src := fs.NewOverrideRemote(source, "photo.png")
	o := &Object{fs: f, remote: "photo.png", uploadSource: "picture"}
	r, err := o.prepareUploadRecovery(ctx, api.Upload{Name: "photo.png", FolderID: "1", Size: 6, Modified: src.ModTime(ctx).UTC().Format(dateFormat)}, src, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	require.NoError(t, r.lock.Unlock())
	fx.mu.Lock()
	fx.media = append(fx.media, api.Media{ID: "42", FolderID: "1", Name: "photo.png", Size: 6, Type: "audio", Status: "U"})
	fx.mu.Unlock()
	_, err = f.Put(ctx, strings.NewReader("abcdef"), src)
	require.ErrorContains(t, err, "source type")
	assert.Empty(t, paths(), "mismatch must precede probe and registration")
}

func TestUploadSourceJournalValidation(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		version    int
		valid      bool
	}{
		{"legacy", "", 1, true}, {"generic", "file", 1, true}, {"native", "picture", 2, true},
		{"unknown", "unknown", 1, false}, {"unsafe-legacy-native", "picture", 1, false}, {"future", "file", 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			f, _ := recoveryFs(t, fx, "source-journal-"+tc.name)
			record := &uploadRecord{Version: tc.version, ID: "42", MediaType: tc.kind}
			require.NoError(t, f.journalDo(true, &uploadJournalOp{key: "test", record: record, write: true}))
			_, scanErr := f.pendingUploads()
			readErr := f.journalDo(false, &uploadJournalOp{key: "test"})
			if tc.valid {
				assert.NoError(t, scanErr)
				assert.NoError(t, readErr)
			} else {
				assert.ErrorContains(t, scanErr, "invalid upload recovery record")
				assert.ErrorContains(t, readErr, "invalid upload recovery record")
			}
		})
	}
}

func TestUploadNativeMetadataCompletes(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(strconv.FormatBool(async), func(t *testing.T) {
			fx := newFlatFixture(t)
			uploadSourceFixture(t, fx)
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("action") == "get-validation-status" {
						t.Error("a completed native U metadata response must not wait for V")
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "unexpected validation polling"}})
						return
					}
					next.ServeHTTP(w, r)
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": strconv.FormatBool(async)})
			item, err := f.Put(ctx, strings.NewReader("picture"), object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), 7, true, nil, nil))
			require.NoError(t, err)
			assert.Equal(t, "picture", item.(*Object).info.Type)
		})
	}
}

func TestUploadNativeEarlyReply(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(strconv.FormatBool(async), func(t *testing.T) {
			fx := newFlatFixture(t)
			uploadSourceFixture(t, fx)
			modified := time.Unix(1700000000, 0)
			const size = 8 << 20
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/sapi/upload/picture" && r.URL.Query().Get("action") == "save" {
						item := api.Media{ID: "1", Name: "photo.png", FolderID: "1", Size: size, Modified: modified.UnixMilli(), Type: "picture", Status: "U", URL: fx.server.URL + "/content/1"}
						jsonReply(t, w, map[string]any{"id": "1", "status": "U", "metadata": map[string]any{"pictures": []api.Media{item}}})
						return
					}
					next.ServeHTTP(w, r)
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": strconv.FormatBool(async), "resume_uploads": strconv.FormatBool(async)})
			var src fs.ObjectInfo = object.NewStaticObjectInfo("photo.png", modified, size, true, nil, nil)
			if async {
				source, path := recoverySource(t)
				require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", size)), 0600))
				require.NoError(t, os.Chtimes(path, modified, modified))
				source, err := source.Fs().(fs.Fs).NewObject(ctx, source.Remote())
				require.NoError(t, err)
				src = fs.NewOverrideRemote(source, "photo.png")
			}
			reader := &slowUploadReader{input: strings.NewReader(strings.Repeat("x", size)), started: make(chan struct{})}
			_, err := f.Put(ctx, reader, src)
			require.ErrorContains(t, err, "all content")
			if async {
				records, err := f.pendingUploads()
				require.NoError(t, err)
				require.Len(t, records, 1)
				for _, record := range records {
					assert.False(t, record.Processing)
				}
			}
		})
	}
}

func TestUploadNativeMetadataThroughDefaultAlias(t *testing.T) {
	fx := newFlatFixture(t)
	paths := uploadSourceFixture(t, fx)
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto"})
	modified := time.Unix(1700000000, 0)
	item, err := f.Put(ctx, strings.NewReader("picture"), object.NewStaticObjectInfo("photo.png", modified, 7, true, nil, nil))
	require.NoError(t, err)
	alias, _ := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "file"})
	moved, err := alias.Move(ctx, item, "renamed.png")
	require.NoError(t, err)
	require.NoError(t, moved.SetModTime(ctx, modified.Add(time.Hour)))
	for _, request := range paths() {
		assert.True(t, strings.HasPrefix(request, "/sapi/upload/picture?"), request)
	}
}

func TestUploadNativeSessionRenewal(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(strconv.FormatBool(async), func(t *testing.T) {
			fx := newFlatFixture(t)
			uploadSourceFixture(t, fx)
			calls := 0
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/sapi/upload/picture" && r.URL.Query().Get("action") == "save" {
						calls++
						if calls == 1 {
							w.WriteHeader(http.StatusUnauthorized)
							return
						}
					}
					next.ServeHTTP(w, r)
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": strconv.FormatBool(async)})
			payload := strings.Repeat("x", 1<<18)
			reader := &slowUploadReader{input: strings.NewReader(payload), started: make(chan struct{})}
			item, err := f.Put(ctx, reader, object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), int64(len(payload)), true, nil, nil))
			require.NoError(t, err)
			assert.Equal(t, 2, calls)
			assert.Equal(t, "picture", item.(*Object).info.Type)
			fx.mu.Lock()
			defer fx.mu.Unlock()
			assert.Equal(t, payload, fx.content[item.(*Object).info.ID])
		})
	}
}

func TestUploadNativeMetadataFallback(t *testing.T) {
	for _, kind := range []string{"absent", "malformed", "wrong-source", "wrong-type", "processing"} {
		t.Run(kind, func(t *testing.T) {
			fx := newFlatFixture(t)
			uploadSourceFixture(t, fx)
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/sapi/upload/picture" || r.URL.Query().Get("action") != "save" {
						next.ServeHTTP(w, r)
						return
					}
					captured := httptest.NewRecorder()
					next.ServeHTTP(captured, r)
					var reply map[string]any
					require.NoError(t, json.Unmarshal(captured.Body.Bytes(), &reply))
					switch kind {
					case "malformed":
						reply["metadata"] = []any{}
					case "wrong-source":
						reply["metadata"] = map[string]any{"files": []map[string]string{{"id": "1"}}}
					default:
						delete(reply, "metadata")
					}
					fx.mu.Lock()
					if kind == "wrong-type" {
						fx.media[len(fx.media)-1].Type = "file"
					}
					if kind == "processing" {
						fx.media[len(fx.media)-1].Status = "V"
					}
					fx.mu.Unlock()
					jsonReply(t, w, reply)
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "upload_source": "auto", "async_upload": "true"})
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			item, err := f.Put(ctx, strings.NewReader("picture"), object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), 7, true, nil, nil))
			if kind == "wrong-type" || kind == "processing" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "picture", item.(*Object).info.Type)
			fx.mu.Lock()
			defer fx.mu.Unlock()
			assert.Contains(t, fx.mediaBatches, 1)
		})
	}
}

func TestUploadFlatNamespaceUsesFile(t *testing.T) {
	fx := newFlatFixture(t)
	paths := uploadSourceFixture(t, fx)
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"upload_source": "auto"})
	item, err := f.Put(ctx, strings.NewReader("picture"), object.NewStaticObjectInfo("photo.png", time.Unix(1700000000, 0), 7, true, nil, nil))
	require.NoError(t, err)
	assert.Equal(t, "file", item.(*Object).info.Type)
	for _, request := range paths() {
		assert.NotContains(t, request, "/picture")
	}
}
