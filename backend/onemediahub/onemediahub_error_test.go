package onemediahub

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUploadFailureClassification(t *testing.T) {
	for _, code := range []string{"MED-1007", "MED-1029", "MED-1050", "COM-1008", "COM-1011", "COM-1014", "COM-1005", "MED-1000"} {
		for _, mode := range []string{"multipart", "registration", "raw"} {
			t.Run(code+"/"+mode, func(t *testing.T) {
				fx := newFlatFixture(t)
				var failed, contentCalls int
				fx.wrapHandler(func(next http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						action := r.URL.Query().Get("action")
						if !strings.HasPrefix(r.URL.Path, "/sapi/upload") {
							next.ServeHTTP(w, r)
							return
						}
						if action == "save" {
							contentCalls++
						}
						if mode == "raw" && action == "save-metadata" {
							jsonReply(t, w, map[string]any{"id": "42"})
							return
						}
						failed++
						w.WriteHeader(http.StatusServiceUnavailable)
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": code, "message": "upload rejected"}})
					})
				})
				f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": fmt.Sprint(mode != "multipart")})
				fs.GetConfig(ctx).LowLevelRetries = 3
				_, err := f.Put(ctx, strings.NewReader("payload"), object.NewStaticObjectInfo("file", time.Now(), 7, true, nil, nil))
				require.Error(t, err)
				var serviceError *api.Error
				require.ErrorAs(t, err, &serviceError)
				assert.Equal(t, code, serviceError.Code)
				if code != "MED-1000" {
					assert.True(t, fserrors.IsNoRetryError(err))
					assert.Equal(t, 1, failed)
					assert.False(t, fserrors.IsRetryError(err))
				}
				assert.False(t, fserrors.IsFatalError(err))
				if mode == "registration" {
					assert.Zero(t, contentCalls)
				}
			})
		}
	}
}

func TestUploadStructuredOffsetRecovery(t *testing.T) {
	for _, code := range []string{"MED-1006", "MED-1017"} {
		t.Run(code, func(t *testing.T) {
			fx := newFlatFixture(t)
			modified := time.Unix(1700000000, 0)
			var saves, probes int
			var content string
			fx.wrapHandler(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					action := r.URL.Query().Get("action")
					if action == "save-metadata" {
						jsonReply(t, w, map[string]any{"id": "42"})
						return
					}
					if action == "get-validation-status" {
						jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
						return
					}
					if action != "save" {
						next.ServeHTTP(w, r)
						return
					}
					if strings.HasPrefix(r.Header.Get("Content-Range"), "bytes */") {
						probes++
						w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(content)-1))
						w.WriteHeader(http.StatusPermanentRedirect)
						return
					}
					saves++
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					if saves == 1 {
						assert.Equal(t, "abcdef", string(body))
						content = "abc"
						if code == "MED-1017" {
							content = "abcdef"
						}
						fx.mu.Lock()
						fx.media = []api.Media{{ID: "42", Name: "file", FolderID: "1", Size: 6, Modified: modified.UnixMilli(), Type: "file"}}
						fx.mu.Unlock()
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": code, "message": "recoverable upload state"}})
						return
					}
					assert.Equal(t, "def", string(body))
					assert.Equal(t, "bytes 3-5/6", r.Header.Get("Content-Range"))
					assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
					content += string(body)
					jsonReply(t, w, map[string]any{"id": "42"})
				})
			})
			f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false", "async_upload": "true"})
			fs.GetConfig(ctx).LowLevelRetries = 2
			item, err := f.Put(ctx, strings.NewReader("abcdef"), object.NewStaticObjectInfo("file", modified, 6, true, nil, nil))
			require.NoError(t, err)
			assert.Equal(t, "42", item.(*Object).ID())
			assert.Equal(t, "abcdef", content)
			assert.Equal(t, 1, probes)
			want := 2
			if code == "MED-1017" {
				want = 1
			}
			assert.Equal(t, want, saves)
		})
	}
}

func TestUploadPermanentMetadataFailure(t *testing.T) {
	fx := newFlatFixture(t)
	var uploaded atomic.Bool
	var contentCalls atomic.Int32
	fx.wrapHandler(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/sapi/media" && uploaded.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				jsonReply(t, w, map[string]any{"error": map[string]string{"code": "MED-1007", "message": "quota exceeded"}})
				return
			}
			if r.URL.Query().Get("action") == "save" {
				contentCalls.Add(1)
				next.ServeHTTP(w, r)
				uploaded.Store(true)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	f, ctx := flatTestFs(t, fx, "", false, configmap.Simple{"flat_namespace": "false"})
	_, err := f.Put(ctx, strings.NewReader("x"), object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, nil))
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err))
	assert.False(t, fserrors.IsRetryError(err))
	assert.EqualValues(t, 1, contentCalls.Load())
	var serviceError *api.Error
	require.ErrorAs(t, err, &serviceError)
	assert.Equal(t, "MED-1007", serviceError.Code)
}

func TestUploadPermanentErrorOverridesRetry(t *testing.T) {
	cause := &api.Error{Code: "MED-1007", Message: "quota exceeded"}
	before := fmt.Errorf("read uploaded metadata: %w", fserrors.RetryError(cause))
	after := uploadError(before)
	assert.True(t, fserrors.IsNoRetryError(after))
	assert.False(t, fserrors.IsRetryError(after))
	assert.Equal(t, before.Error(), after.Error())
	assert.True(t, errors.Is(after, cause))
}
