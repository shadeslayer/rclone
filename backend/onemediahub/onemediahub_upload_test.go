package onemediahub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadBodySessionRenewal(t *testing.T) {
	for _, kind := range []string{"seekable", "known length", "accounting", "multipart", "factory", "json", "invalid key", "nonseekable", "second rejection", "forbidden", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			fx := newFixture(t)
			var calls, logins int
			var bodies []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sapi/login" {
					logins++
				}
				if r.URL.Path != "/sapi/upload/file" {
					fx.serve(t, w, r)
					return
				}
				calls++
				if kind == "multipart" {
					require.NoError(t, r.ParseMultipartForm(1<<20))
					defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
					file, _, err := r.FormFile("file")
					require.NoError(t, err)
					body, err := io.ReadAll(file)
					require.NoError(t, err)
					require.NoError(t, file.Close())
					bodies = append(bodies, string(body))
					assert.Equal(t, `{"data":{"id":"42"}}`, r.FormValue("data"))
				} else {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					bodies = append(bodies, string(body))
					if kind == "known length" {
						assert.EqualValues(t, len(body), r.ContentLength)
						assert.Empty(t, r.TransferEncoding)
					}
				}
				if calls == 1 || kind == "second rejection" {
					switch kind {
					case "invalid key":
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": invalidKeyCode, "message": "expired"}})
					case "forbidden":
						w.WriteHeader(http.StatusForbidden)
					case "unavailable":
						w.WriteHeader(http.StatusServiceUnavailable)
					default:
						w.WriteHeader(http.StatusUnauthorized)
					}
					return
				}
				assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
				jsonReply(t, w, map[string]any{"id": 42})
			}))
			defer server.Close()
			m := fx.config(t)
			m["url"] = server.URL
			remote, err := NewFs(ctx, "auth-body", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			content := "exact upload bytes"
			reader := strings.NewReader("prefix" + content)
			_, err = reader.Seek(6, io.SeekStart)
			require.NoError(t, err)
			size := int64(len(content))
			opts := rest.Opts{Method: http.MethodPost, Path: "/upload/file", Parameters: url.Values{"action": {"save"}}, Body: reader, ContentLength: &size, ExtraHeaders: map[string]string{"X-funambol-id": "42"}}
			var request any
			var counted *accounting.StatsInfo
			switch kind {
			case "known length":
				opts.ContentLength = nil
			case "accounting":
				counted = accounting.NewStats(ctx)
				transfer := counted.NewTransferRemoteSize("auth-body", size, nil, nil)
				acc := transfer.Account(ctx, io.NopCloser(reader))
				t.Cleanup(func() { transfer.Done(ctx, nil); require.NoError(t, acc.Close()) })
				opts.Body = acc.WrapStream(reader)
			case "multipart":
				opts.MultipartMetadataName, opts.MultipartContentName, opts.MultipartFileName = "data", "file", "name"
				request = map[string]any{"data": map[string]string{"id": "42"}}
			case "factory":
				opts.Body = bytes.NewBufferString(content)
				opts.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(content)), nil }
			case "json":
				opts.Body, opts.ContentLength = nil, nil
				request = map[string]string{"name": content}
			case "nonseekable":
				opts.Body = bytes.NewBufferString(content)
			}
			_, err = f.call(ctx, opts, request)
			switch kind {
			case "nonseekable", "forbidden", "unavailable":
				require.Error(t, err)
				assert.Equal(t, 1, calls)
				assert.Equal(t, 1, logins)
			case "second rejection":
				require.Error(t, err)
				assert.Equal(t, 2, calls)
				assert.Equal(t, 2, logins)
			default:
				require.NoError(t, err)
				assert.Equal(t, 2, calls)
				assert.Equal(t, 2, logins)
			}
			for _, body := range bodies {
				if kind == "json" {
					assert.JSONEq(t, `{"name":"exact upload bytes"}`, body)
				} else {
					assert.Equal(t, content, body)
				}
			}
			if counted != nil {
				assert.EqualValues(t, 2*size, counted.GetBytes())
			}
		})
	}
}

type slowUploadReader struct {
	input   *strings.Reader
	started chan struct{}
	reads   atomic.Int32
	reading atomic.Bool
	closed  atomic.Bool
}

func (r *slowUploadReader) Read(p []byte) (int, error) {
	r.reading.Store(true)
	defer r.reading.Store(false)
	if r.reads.Add(1) == 1 {
		close(r.started)
	}
	time.Sleep(time.Millisecond)
	return r.input.Read(p)
}

func (r *slowUploadReader) Seek(offset int64, whence int) (int64, error) {
	if r.reading.Load() {
		return 0, errors.New("seek overlapped transport read")
	}
	return r.input.Seek(offset, whence)
}

func (r *slowUploadReader) Close() error {
	r.closed.Store(true)
	return nil
}

func TestUploadEarlySessionRejection(t *testing.T) {
	for _, mode := range []string{"raw", "multipart", "async"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fx := newFixture(t)
			calls := 0
			content := strings.Repeat("same bytes", 1<<18)
			modified := time.Now().Truncate(time.Second)
			reader := &slowUploadReader{input: strings.NewReader(content), started: make(chan struct{})}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "async" && r.URL.Query().Get("action") == "save-metadata" {
					jsonReply(t, w, map[string]any{"id": 42})
					return
				}
				if r.URL.Query().Get("action") == "get-validation-status" {
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]any{{"id": 42, "status": "V"}}}})
					return
				}
				if r.URL.Path != "/sapi/upload/file" {
					fx.serve(t, w, r)
					return
				}
				calls++
				if calls == 1 {
					<-reader.started
					w.Header().Set("Connection", "close")
					w.WriteHeader(http.StatusUnauthorized)
					w.(http.Flusher).Flush()
					return
				}
				var input io.Reader = r.Body
				if mode == "multipart" {
					require.NoError(t, r.ParseMultipartForm(4<<20))
					defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
					file, _, err := r.FormFile("file")
					require.NoError(t, err)
					defer func() { assert.NoError(t, file.Close()) }()
					input = file
				}
				body, err := io.ReadAll(input)
				require.NoError(t, err)
				assert.Equal(t, content, string(body))
				fx.mu.Lock()
				fx.media = []api.Media{{ID: "42", Name: "name", Size: int64(len(content)), Modified: modified.UnixMilli(), Type: "file", Status: "V"}}
				fx.mu.Unlock()
				jsonReply(t, w, map[string]any{"id": 42})
			}))
			defer server.Close()
			m := fx.config(t)
			m["url"] = server.URL
			if mode == "async" {
				m["async_upload"] = "true"
			}
			remote, err := NewFs(ctx, "early-auth", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			size := int64(len(content))
			opts := rest.Opts{Method: http.MethodPost, Path: "/upload/file", Parameters: url.Values{"action": {"save"}}, Body: reader, ContentLength: &size}
			if mode == "multipart" {
				opts.MultipartContentName, opts.MultipartFileName = "file", "name"
			}
			if mode == "async" {
				_, err = f.Put(ctx, reader, object.NewStaticObjectInfo("name", modified, size, true, nil, nil))
			} else {
				_, err = f.call(ctx, opts, nil)
			}
			require.NoError(t, err)
			assert.Equal(t, 2, calls)
			assert.False(t, reader.closed.Load(), "the caller owns the input")
			assert.Greater(t, reader.reads.Load(), int32(1))
		})
	}
}

func TestAsyncUploadSessionRenewal(t *testing.T) {
	for _, stage := range []string{"save-metadata", "save"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			fx := newFixture(t)
			calls := map[string]int{}
			modified := time.Now().Truncate(time.Second)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.URL.Query().Get("action")
				if r.URL.Path == "/sapi/upload/file" {
					calls[action]++
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					if action == "save-metadata" {
						var value struct{ Data api.Upload }
						require.NoError(t, json.Unmarshal(body, &value))
						assert.Equal(t, "name", value.Data.Name)
					} else {
						assert.Equal(t, "abcdef", string(body))
						assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
						assert.EqualValues(t, 6, r.ContentLength)
					}
					if action == stage && calls[action] == 1 {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					if action == "save" {
						fx.mu.Lock()
						fx.media = []api.Media{{ID: "42", Name: "name", Size: 6, Modified: modified.UnixMilli(), Type: "file", Status: "V"}}
						fx.mu.Unlock()
					}
					jsonReply(t, w, map[string]any{"id": 42})
					return
				}
				if action == "get-validation-status" {
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]any{{"id": 42, "status": "V"}}}})
					return
				}
				fx.serve(t, w, r)
			}))
			defer server.Close()
			m := fx.config(t)
			m["url"], m["async_upload"] = server.URL, "true"
			remote, err := NewFs(ctx, "async-auth", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			_, err = f.Put(ctx, strings.NewReader("abcdef"), object.NewStaticObjectInfo("name", modified, 6, true, nil, nil))
			require.NoError(t, err)
			assert.Equal(t, 2, calls[stage])
			other := "save"
			if stage == other {
				other = "save-metadata"
			}
			assert.Equal(t, 1, calls[other])
		})
	}
}
