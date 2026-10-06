package onemediahub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResumeUploadOptions(t *testing.T) {
	_, err := readOptions(testConfig(t, configmap.Simple{"resume_uploads": "true"}))
	require.ErrorContains(t, err, "async_upload")
}

func TestResumeUploadDestinationProtectionOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*fs.ConfigInfo)
	}{
		{"--immutable", func(ci *fs.ConfigInfo) { ci.Immutable = true }},
		{"--backup-dir", func(ci *fs.ConfigInfo) { ci.BackupDir = "backup:" }},
		{"--suffix", func(ci *fs.ConfigInfo) { ci.Suffix = ".old" }},
		{"--ignore-existing", func(ci *fs.ConfigInfo) { ci.IgnoreExisting = true }},
		{"--update", func(ci *fs.ConfigInfo) { ci.UpdateOlder = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			ctx, ci := fs.AddConfig(context.Background())
			tc.set(ci)
			m := fx.config(t)
			m["async_upload"], m["resume_uploads"] = "true", "true"
			_, err := NewFs(ctx, "protected-destination", "", m)
			require.ErrorContains(t, err, tc.name)
			fx.mu.Lock()
			assert.Empty(t, fx.requests)
			fx.mu.Unlock()
			m["resume_uploads"] = "false"
			remote, err := NewFs(ctx, "regular-destination", "", m)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, remote.(*Fs).Shutdown(ctx)) })
		})
	}
}

type recoveryProcessInput struct {
	URL, CacheDir, SourceDir, Name string
	WantError                      bool
	Buffered                       bool
	CopyCommand                    bool
	SizeOnly                       bool
	WantRecord                     bool
}

// TestUploadRecoveryProcess is run in an executable without the .test suffix,
// so lib/kv preserves its database between independent processes.
func TestUploadRecoveryProcess(t *testing.T) {
	input := os.Getenv("RCLONE_O2_RECOVERY_TEST")
	if input == "" {
		t.Skip("subprocess helper")
	}
	var request recoveryProcessInput
	require.NoError(t, json.Unmarshal([]byte(input), &request))
	require.NoError(t, config.SetCacheDir(request.CacheDir))
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ci.KvLockTime = fs.Duration(50 * time.Millisecond)
	ci.SizeOnly = request.SizeOnly
	sourceFs, err := local.NewFs(ctx, "recovery-source", request.SourceDir, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	in, err := source.Open(ctx)
	require.NoError(t, err)
	if !request.Buffered {
		defer func() { require.NoError(t, in.Close()) }()
	}
	m := testConfig(t, configmap.Simple{
		"url": request.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test"),
		"async_upload": "true", "resume_uploads": "true", "upload_timeout": "1s",
	})
	remote, err := NewFs(ctx, request.Name, "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	defer func() { require.NoError(t, f.Shutdown(ctx)) }()
	if timeout := os.Getenv("RCLONE_O2_RECOVERY_TIMEOUT"); timeout != "" {
		duration, err := time.ParseDuration(timeout)
		require.NoError(t, err)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}
	var reader io.Reader = in
	if request.Buffered {
		transfer := accounting.Stats(ctx).NewTransfer(source, nil)
		defer transfer.Done(ctx, nil)
		acc := transfer.Account(ctx, in).WithBuffer()
		defer func() { require.NoError(t, acc.Close()) }()
		require.True(t, acc.HasBuffer())
		reader = acc
	}
	var obj fs.Object
	if request.CopyCommand {
		err = operations.CopyFile(ctx, f, sourceFs, "target.txt", "source.txt")
	} else {
		obj, err = f.Put(ctx, reader, fs.NewOverrideRemote(source, "target.txt"))
	}
	if request.WantError {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		if obj != nil {
			assert.EqualValues(t, source.Size(), obj.Size())
		}
	}
	if request.WantRecord {
		op := &recoveryRecords{}
		require.NoError(t, f.uploadJournal.Do(false, op))
		require.Len(t, op.items, 1)
	}
}

func recoveryHelper(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	name := "recovery-helper"
	if strings.HasSuffix(executable, ".exe") {
		name += ".exe"
	}
	copyPath := filepath.Join(t.TempDir(), name)
	if err := os.Link(executable, copyPath); err != nil {
		in, err := os.Open(executable)
		require.NoError(t, err)
		out, err := os.OpenFile(copyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0700)
		require.NoError(t, err)
		_, err = io.Copy(out, in)
		require.NoError(t, err)
		require.NoError(t, out.Close())
		require.NoError(t, in.Close())
	}
	return copyPath
}

func runRecoveryProcess(t *testing.T, helper string, input recoveryProcessInput) {
	t.Helper()
	b, err := json.Marshal(input)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, helper, "-test.run=^TestUploadRecoveryProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "RCLONE_O2_RECOVERY_TEST="+string(b))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestUploadRecoveryAcrossProcesses(t *testing.T) {
	helper := recoveryHelper(t)
	for _, mode := range []string{"partial", "none", "buffered", "copy command", "size-only copy command", "accepted", "processing", "changed content", "old replacement validation", "missing range", "trashed", "deleted", "soft deleted status"} {
		t.Run(mode, func(t *testing.T) {
			copyCommand := strings.HasSuffix(mode, "copy command")
			content := "abcdef"
			if mode == "buffered" || copyCommand {
				content = strings.Repeat(content, 200000)
			}
			size := len(content)
			fx := newFixture(t)
			if mode == "old replacement validation" {
				fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			}
			var stage atomic.Int32
			stage.Store(1)
			var metadataCalls, rawCalls, probes atomic.Int32
			var mu sync.Mutex
			var saved string
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sapi/upload/file" {
					if r.URL.Query().Get("action") == "save-metadata" {
						metadataCalls.Add(1)
						mu.Lock()
						saved = ""
						mu.Unlock()
						jsonReply(t, w, map[string]string{"id": "42"})
						return
					}
					if r.Header.Get("Content-Range") == "bytes */"+strconv.Itoa(size) {
						probes.Add(1)
						mu.Lock()
						if mode != "missing range" {
							w.Header().Set("Range", "bytes=0-"+strconv.Itoa(len(saved)-1))
						}
						mu.Unlock()
						w.WriteHeader(308)
						return
					}
					rawCalls.Add(1)
					b, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					mu.Lock()
					if stage.Load() == 1 {
						if mode == "accepted" || mode == "processing" {
							saved = string(b)
						} else if mode == "none" {
							saved = ""
						} else {
							saved = string(b[:3])
						}
					} else {
						if mode == "changed content" {
							assert.Empty(t, r.Header.Get("Content-Range"))
						} else {
							assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", len(saved), size-1, size), r.Header.Get("Content-Range"))
						}
						saved += string(b)
					}
					mu.Unlock()
					if stage.Load() == 1 && copyCommand {
						fx.mu.Lock()
						fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: int64(size), Modified: 1700000000123, Type: "file"}}
						fx.mu.Unlock()
					}
					if stage.Load() == 1 && mode != "processing" {
						w.WriteHeader(503)
					} else {
						w.WriteHeader(202)
					}
					return
				}
				if r.URL.Query().Get("action") == "get-validation-status" {
					if stage.Load() == 1 && mode == "processing" {
						w.WriteHeader(503)
						return
					}
					fx.mu.Lock()
					fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: int64(size), Type: "file"}}
					fx.mu.Unlock()
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			sourceDir := t.TempDir()
			sourcePath := filepath.Join(sourceDir, "source.txt")
			require.NoError(t, os.WriteFile(sourcePath, []byte(content), 0600))
			modified := time.Unix(1700000000, 123000000)
			require.NoError(t, os.Chtimes(sourcePath, modified, modified))
			input := recoveryProcessInput{URL: fx.server.URL, CacheDir: t.TempDir(), SourceDir: sourceDir, Name: "first-alias", WantError: true, Buffered: mode == "buffered"}
			runRecoveryProcess(t, helper, input)
			if mode == "changed content" {
				require.NoError(t, os.WriteFile(sourcePath, []byte("ABCDEF"), 0600))
				require.NoError(t, os.Chtimes(sourcePath, modified, modified))
			}
			deleted := mode == "trashed" || mode == "deleted" || mode == "soft deleted status"
			if deleted {
				item := api.Media{ID: "42", Name: "target.txt", Size: 6, Type: "file"}
				switch mode {
				case "trashed":
					item.SoftDeleted = true
				case "deleted":
					item.Status = "D"
				case "soft deleted status":
					item.Status = "S"
				}
				fx.mu.Lock()
				fx.media = []api.Media{item}
				fx.mu.Unlock()
			}
			stage.Store(2)
			input.Name, input.WantError = "second-alias", mode == "missing range" || deleted
			input.CopyCommand = copyCommand
			input.SizeOnly = mode == "size-only copy command"
			input.WantRecord = deleted
			runRecoveryProcess(t, helper, input)
			mu.Lock()
			got := saved
			mu.Unlock()
			if deleted {
				assert.Equal(t, "abc", got)
				assert.EqualValues(t, 1, rawCalls.Load())
				assert.EqualValues(t, 1, metadataCalls.Load())
				assert.Zero(t, probes.Load())
			} else if mode == "missing range" {
				assert.Equal(t, "abc", got)
				assert.EqualValues(t, 1, rawCalls.Load())
			} else if mode == "changed content" {
				assert.Equal(t, "ABCDEF", got)
				assert.EqualValues(t, 2, metadataCalls.Load())
				assert.Zero(t, probes.Load())
			} else {
				assert.Equal(t, content, got)
				assert.EqualValues(t, 1, metadataCalls.Load())
				if mode == "processing" {
					assert.Zero(t, probes.Load())
				} else {
					assert.EqualValues(t, 1, probes.Load())
				}
			}
		})
	}
}

func TestUploadRecoveryBindsSuppliedHandle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacement of an open source file")
	}
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "supplied-handle")
	source, sourcePath := recoverySource(t)
	modified := source.ModTime(ctx)
	in, err := source.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	replacement := sourcePath + ".replacement"
	require.NoError(t, os.WriteFile(replacement, []byte("uvwxyz"), 0600))
	require.NoError(t, os.Chtimes(replacement, modified, modified))
	require.NoError(t, os.Rename(replacement, sourcePath))
	var sent string
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/upload/file" {
			if r.URL.Query().Get("action") == "save-metadata" {
				jsonReply(t, w, map[string]string{"id": "42"})
			} else {
				b, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				sent = string(b)
				w.WriteHeader(503)
			}
			return
		}
		handler.ServeHTTP(w, r)
	})
	_, err = f.Put(ctx, in, fs.NewOverrideRemote(source, "target.txt"))
	require.Error(t, err)
	assert.Equal(t, "abcdef", sent)
	op := &recoveryRecords{}
	err = f.uploadJournal.Do(false, op)
	assert.ErrorIs(t, err, kv.ErrEmpty)
	assert.Empty(t, op.items)
}

func TestUploadRecoveryUnboundRenamedSourceRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacement of an open source file")
	}
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "nonseekable-handle")
	fs.GetConfig(ctx).LowLevelRetries = 2
	source, sourcePath := recoverySource(t)
	modified := source.ModTime(ctx)
	in, err := source.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	replacement := sourcePath + ".replacement"
	require.NoError(t, os.WriteFile(replacement, []byte("uvwxyz"), 0600))
	require.NoError(t, os.Chtimes(replacement, modified, modified))
	require.NoError(t, os.Rename(replacement, sourcePath))
	var sent []string
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/upload/file" {
			if r.URL.Query().Get("action") == "save-metadata" {
				jsonReply(t, w, map[string]string{"id": "42"})
			} else if r.Header.Get("Content-Range") == "bytes */6" {
				w.Header().Set("Range", "bytes=0-2")
				w.WriteHeader(308)
			} else {
				b, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				sent = append(sent, string(b))
				w.WriteHeader(503)
			}
			return
		}
		handler.ServeHTTP(w, r)
	})
	_, err = f.Put(ctx, io.NopCloser(in), fs.NewOverrideRemote(source, "target.txt"))
	require.ErrorContains(t, err, "cannot resume upload from a non-seekable source")
	assert.Equal(t, []string{"abcdef"}, sent)
}

func TestUploadRecoveryReOpenSourceMismatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit replacement of an open source file")
	}
	for _, buffered := range []bool{false, true} {
		t.Run(strconv.FormatBool(buffered), func(t *testing.T) {
			fx := newFixture(t)
			f, ctx := recoveryFs(t, fx, "reopen-mismatch")
			source, sourcePath := recoverySource(t)
			original, replacement := "abcdef", "uvwxyz"
			if buffered {
				original, replacement = strings.Repeat(original, 200000), strings.Repeat(replacement, 200000)
			}
			require.NoError(t, os.WriteFile(sourcePath, []byte(original), 0600))
			source, err := source.Fs().(fs.Fs).NewObject(ctx, "source.txt")
			require.NoError(t, err)
			modified := source.ModTime(ctx)
			in, err := operations.Open(ctx, source)
			require.NoError(t, err)
			transfer := accounting.Stats(ctx).NewTransfer(source, nil)
			defer transfer.Done(ctx, nil)
			acc := transfer.Account(ctx, in)
			if buffered {
				acc.WithBuffer()
				require.True(t, acc.HasBuffer())
			}
			defer func() { require.NoError(t, acc.Close()) }()
			replacementPath := sourcePath + ".replacement"
			require.NoError(t, os.WriteFile(replacementPath, []byte(replacement), 0600))
			require.NoError(t, os.Chtimes(replacementPath, modified, modified))
			require.NoError(t, os.Rename(replacementPath, sourcePath))
			_, err = f.Put(ctx, acc, fs.NewOverrideRemote(source, "target.txt"))
			require.ErrorContains(t, err, "upload input content differs")
			fx.mu.Lock()
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
			op := &recoveryRecords{}
			err = f.uploadJournal.Do(false, op)
			assert.ErrorIs(t, err, kv.ErrEmpty)
			assert.Empty(t, op.items)
		})
	}
}

func TestBindBufferedUploadReader(t *testing.T) {
	ctx := accounting.WithStatsGroup(context.Background(), t.Name())
	dir := t.TempDir()
	content := strings.Repeat("abcdef", 200000)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "source.txt"), []byte(content), 0600))
	sourceFs, err := local.NewFs(ctx, "buffered-source", dir, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	in, err := source.Open(ctx)
	require.NoError(t, err)
	transfer := accounting.Stats(ctx).NewTransfer(source, nil)
	defer transfer.Done(ctx, nil)
	acc := transfer.Account(ctx, in).WithBuffer()
	defer func() { require.NoError(t, acc.Close()) }()
	require.True(t, acc.HasBuffer())
	expected, err := source.Hash(ctx, hash.SHA256)
	require.NoError(t, err)
	reader, sum, err := bindUploadReader(ctx, acc, source.Size(), hash.SHA256, expected)
	require.NoError(t, err)
	assert.Zero(t, accounting.Stats(ctx).GetBytes())
	assert.Equal(t, expected, sum)
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(b))
	assert.EqualValues(t, len(content), accounting.Stats(ctx).GetBytes())
	_, err = reader.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF)
	require.NoError(t, acc.Close())
	_, err = in.Read(make([]byte, 1))
	assert.ErrorIs(t, err, os.ErrClosed)
}

func TestUploadRecoveryNonSeekableInput(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "unbound-input")
	source, _ := recoverySource(t)
	in, err := source.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	wrapped := io.NopCloser(in)
	owner, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, wrapped)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.lock.Unlock()) }()
	assert.Nil(t, owner.record)
	b, err := io.ReadAll(owner.reader)
	require.NoError(t, err)
	assert.Equal(t, "abcdef", string(b))
}

func recoverySource(t *testing.T) (fs.Object, string) {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "source.txt")
	require.NoError(t, os.WriteFile(file, []byte("abcdef"), 0600))
	sourceFs, err := local.NewFs(context.Background(), "recovery-source", dir, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(context.Background(), "source.txt")
	require.NoError(t, err)
	return source, file
}

func recoveryFs(t *testing.T, fx *fixture, name string, extra ...configmap.Simple) (*Fs, context.Context) {
	t.Helper()
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	ci.KvLockTime = fs.Duration(50 * time.Millisecond)
	m := fx.config(t)
	m["async_upload"], m["resume_uploads"] = "true", "true"
	for _, options := range extra {
		for key, value := range options {
			m[key] = value
		}
	}
	remote, err := NewFs(ctx, name, "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	return f, ctx
}

func TestUploadRecoveryPendingVisibility(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			fx := newFixture(t)
			fx.requestTime = 1700000000123
			fx.changes = map[string]api.Changes{"file": {New: []api.ID{"42"}}}
			fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			f, ctx := recoveryFs(t, fx, "pending-visibility", configmap.Simple{"metadata_cache": strconv.FormatBool(cached)})
			source, _ := recoverySource(t)
			data := api.Upload{Name: "target.txt", Size: 6}
			r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, data, source, strings.NewReader("abcdef"))
			require.NoError(t, err)
			r.record.ID = "42"
			require.NoError(t, r.save())
			require.NoError(t, r.lock.Unlock())
			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			assert.Empty(t, entries)
			err = f.ListR(ctx, "", func(entries fs.DirEntries) error {
				assert.Empty(t, entries)
				return nil
			})
			require.NoError(t, err)
			_, err = f.NewObject(ctx, "target.txt")
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			require.ErrorIs(t, f.Rmdir(ctx, ""), fs.ErrorDirectoryNotEmpty)
			if cached {
				state, err := f.notificationState(ctx)
				require.NoError(t, err)
				assert.Empty(t, state)
			}
			require.NoError(t, f.uploadJournal.Do(true, &uploadJournalOp{key: r.key, remove: true}))
			entries, err = f.List(ctx, "")
			require.NoError(t, err)
			require.Len(t, entries, 1)
			var recursive fs.DirEntries
			require.NoError(t, f.ListR(ctx, "", func(entries fs.DirEntries) error {
				recursive = append(recursive, entries...)
				return nil
			}))
			require.Len(t, recursive, 1)
			_, err = f.NewObject(ctx, "target.txt")
			require.NoError(t, err)
			if cached {
				state, err := f.notificationState(ctx)
				require.NoError(t, err)
				assert.Contains(t, state, notificationKey{"42", fs.EntryObject})
			}
		})
	}
}

func TestUploadRecoveryCorruptPendingRecord(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "invalid-record")
	require.NoError(t, f.uploadJournal.Do(true, &uploadJournalOp{key: "invalid", record: &uploadRecord{ID: "42"}, write: true}))
	_, err := f.List(ctx, "")
	require.ErrorContains(t, err, "invalid upload recovery record")
	require.ErrorContains(t, f.ListR(ctx, "", func(fs.DirEntries) error { return nil }), "invalid upload recovery record")
	_, err = f.NewObject(ctx, "target.txt")
	require.ErrorContains(t, err, "invalid upload recovery record")
}

func TestUploadRecoveryNullRecord(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "null-record")
	source, _ := recoverySource(t)
	r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	require.NoError(t, f.journalDo(true, &uploadJournalOp{key: r.key, write: true}))
	require.NoError(t, r.lock.Unlock())
	require.ErrorContains(t, f.journalDo(false, &uploadJournalOp{key: r.key}), "invalid upload recovery record")
	_, err = f.Put(ctx, strings.NewReader("abcdef"), fs.NewOverrideRemote(source, "target.txt"))
	require.ErrorContains(t, err, "invalid upload recovery record")
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestUploadRecoveryChangedSourceKeepsKnownID(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "known-previous-id")
	source, sourcePath := recoverySource(t)
	r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	require.NoError(t, r.lock.Unlock())
	modified := source.ModTime(ctx)
	require.NoError(t, os.WriteFile(sourcePath, []byte("ABCDEF"), 0600))
	require.NoError(t, os.Chtimes(sourcePath, modified, modified))
	var registered api.Upload
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/sapi/media" && req.Method == http.MethodPost && req.URL.Query().Get("action") == "get" {
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}}})
			return
		}
		if req.URL.Path == "/sapi/upload/file" {
			if req.URL.Query().Get("action") == "save-metadata" {
				var body struct {
					Data api.Upload `json:"data"`
				}
				require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
				registered = body.Data
				jsonReply(t, w, map[string]string{"id": "42"})
			} else {
				w.WriteHeader(503)
			}
			return
		}
		handler.ServeHTTP(w, req)
	})
	_, err = f.Put(ctx, strings.NewReader("ABCDEF"), fs.NewOverrideRemote(source, "target.txt"))
	require.Error(t, err)
	assert.Equal(t, "42", registered.ID)
	op := &recoveryRecords{}
	require.NoError(t, f.journalDo(false, op))
	require.Len(t, op.items, 1)
	assert.Equal(t, api.ID("42"), op.items[0].ID)
	assert.NotEqual(t, r.record.Hash, op.items[0].Hash)
}

func TestUploadRecoveryProtectionOnReusedFs(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "reused-options")
	ctx, ci := fs.AddConfig(ctx)
	ci.Immutable = true
	fx.mu.Lock()
	fx.requests = make(map[string]int)
	fx.mu.Unlock()
	source, _ := recoverySource(t)
	o := &Object{fs: f, remote: "target.txt", info: api.Media{ID: "42"}}
	err := o.Update(ctx, strings.NewReader("abcdef"), source)
	require.ErrorContains(t, err, "--immutable")
	_, err = f.Put(ctx, strings.NewReader("abcdef"), source)
	require.ErrorContains(t, err, "--immutable")
	fx.mu.Lock()
	assert.Empty(t, fx.requests)
	fx.mu.Unlock()
}

func TestUploadRecoveryPendingIDAtAnotherPath(t *testing.T) {
	fx := newFixture(t)
	fx.media = []api.Media{{ID: "42", Name: "b.txt", Size: 6, Type: "file"}}
	f, ctx := recoveryFs(t, fx, "renamed-pending")
	source, _ := recoverySource(t)
	r, err := (&Object{fs: f, remote: "a.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "a.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	defer func() { require.NoError(t, r.lock.Unlock()) }()
	_, err = f.Put(ctx, strings.NewReader("abcdef"), fs.NewOverrideRemote(source, "b.txt"))
	require.ErrorContains(t, err, "another path")
	assert.False(t, fserrors.IsRetryError(err))
	op := &recoveryRecords{}
	require.NoError(t, f.uploadJournal.Do(false, op))
	require.Len(t, op.items, 1)
	assert.Equal(t, "a.txt", op.items[0].Name)
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestUploadRecoveryFreshReplacementDestination(t *testing.T) {
	for _, status := range []string{"D", "S", "trash", "missing"} {
		t.Run(status, func(t *testing.T) {
			fx := newFixture(t)
			fx.requestTime = 1700000000123
			fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file", Status: "V"}}
			fx.changes = map[string]api.Changes{"file": {New: []api.ID{"42"}}}
			f, ctx := recoveryFs(t, fx, "stale-replacement", configmap.Simple{"metadata_cache": "true"})
			source, sourcePath := recoverySource(t)
			r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
			require.NoError(t, err)
			r.record.ID = "42"
			require.NoError(t, r.save())
			require.NoError(t, r.lock.Unlock())
			modified := source.ModTime(ctx)
			require.NoError(t, os.WriteFile(sourcePath, []byte("ABCDEF"), 0600))
			require.NoError(t, os.Chtimes(sourcePath, modified, modified))
			fx.mu.Lock()
			if status == "missing" {
				fx.media = nil
			} else {
				fx.media[0].Status = status
				fx.media[0].SoftDeleted = status == "trash"
			}
			fx.mu.Unlock()
			_, err = f.Put(ctx, strings.NewReader("ABCDEF"), fs.NewOverrideRemote(source, "target.txt"))
			require.Error(t, err)
			assert.False(t, fserrors.IsRetryError(err))
			op := &recoveryRecords{}
			require.NoError(t, f.uploadJournal.Do(false, op))
			require.Len(t, op.items, 1)
			assert.Equal(t, r.record.Hash, op.items[0].Hash)
			if status != "missing" {
				_, err = f.Put(ctx, strings.NewReader("ABCDEF"), fs.NewOverrideRemote(source, "target.txt"))
				require.Error(t, err)
				assert.False(t, fserrors.IsRetryError(err))
			}
			fx.mu.Lock()
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
		})
	}
}

func TestUploadRecoveryReplacementUnexpectedID(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "unexpected-destination")
	source, _ := recoverySource(t)
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/sapi/media" && r.URL.Query().Get("action") == "get" {
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []api.Media{{ID: "99", Name: "target.txt", Size: 6, Type: "file"}}}})
			return
		}
		handler.ServeHTTP(w, r)
	})
	o := &Object{fs: f, remote: "target.txt", info: api.Media{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
	err := o.Update(ctx, strings.NewReader("abcdef"), source)
	require.ErrorContains(t, err, "unexpected media ID")
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestUploadRecoveryNotificationShutdown(t *testing.T) {
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"42"}}}
	fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
	f, ctx := recoveryFs(t, fx, "notify-recovery", configmap.Simple{"metadata_cache": "true"})
	source, _ := recoverySource(t)
	r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	defer func() { require.NoError(t, r.lock.Unlock()) }()
	notifyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	intervals := make(chan time.Duration, 1)
	f.ChangeNotify(notifyCtx, func(string, fs.EntryType) {}, intervals)
	intervals <- 5 * time.Millisecond
	require.Eventually(t, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return fx.requests["/sapi/profile/changes"] >= 3
	}, time.Second, time.Millisecond)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, err := f.pendingUploads(); err != nil {
				return
			}
		}
	}()
	require.NoError(t, f.Shutdown(ctx))
	cancel()
	close(intervals)
	select {
	case <-readerDone:
	case <-time.After(time.Second):
		t.Fatal("journal read did not stop after shutdown")
	}
	require.ErrorIs(t, r.save(), kv.ErrInactive)
}

func TestUploadRecoveryRmdirInvisiblePending(t *testing.T) {
	fx := newFixture(t)
	fx.folders = []api.Folder{{ID: "1", Name: "pending"}}
	f, ctx := recoveryFs(t, fx, "invisible-pending")
	source, _ := recoverySource(t)
	r, err := (&Object{fs: f, remote: "pending/target.txt"}).prepareUploadRecovery(ctx, api.Upload{FolderID: "1", Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	require.NoError(t, r.lock.Unlock())
	require.ErrorIs(t, f.Rmdir(ctx, "pending"), fs.ErrorDirectoryNotEmpty)
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/media/folder/delete"])
	fx.mu.Unlock()
}

func TestUploadRecoveryPendingFileRoot(t *testing.T) {
	fx := newFixture(t)
	fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
	f, ctx := recoveryFs(t, fx, "pending-root")
	source, _ := recoverySource(t)
	r, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, api.Upload{Name: "target.txt", Size: 6}, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	r.record.ID = "42"
	require.NoError(t, r.save())
	require.NoError(t, r.lock.Unlock())
	m := fx.config(t)
	m["async_upload"], m["resume_uploads"] = "true", "true"
	remote, err := NewFs(ctx, "pending-file-root", "target.txt", m)
	require.ErrorIs(t, err, fs.ErrorIsFile)
	require.NotNil(t, remote)
	t.Cleanup(func() { require.NoError(t, remote.(*Fs).Shutdown(ctx)) })
}

type recoveryRecords struct {
	items []*uploadRecord
}

func (op *recoveryRecords) Do(_ context.Context, bucket kv.Bucket) error {
	return bucket.ForEach(func(_, data []byte) error {
		var item uploadRecord
		if err := json.Unmarshal(data, &item); err != nil {
			return err
		}
		op.items = append(op.items, &item)
		return nil
	})
}

func TestUploadRecoverySaveBeforeData(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "save-failure")
	source, _ := recoverySource(t)
	var rawCalls atomic.Int32
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/upload/file" {
			if r.URL.Query().Get("action") == "save-metadata" {
				require.NoError(t, f.stopUploadJournal())
				jsonReply(t, w, map[string]string{"id": "42"})
			} else {
				rawCalls.Add(1)
				w.WriteHeader(202)
			}
			return
		}
		handler.ServeHTTP(w, r)
	})
	in, err := source.Open(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	_, err = f.Put(ctx, in, fs.NewOverrideRemote(source, "target.txt"))
	require.ErrorContains(t, err, "save upload recovery")
	assert.Zero(t, rawCalls.Load())
}

func TestUploadRecoveryRequiresMatchingCompletion(t *testing.T) {
	for _, mismatch := range []string{"size", "name", "parent"} {
		t.Run(mismatch, func(t *testing.T) {
			fx := newFixture(t)
			f, ctx := recoveryFs(t, fx, "completion-"+mismatch)
			source, _ := recoverySource(t)
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sapi/upload/file" {
					if r.URL.Query().Get("action") == "save-metadata" {
						jsonReply(t, w, map[string]string{"id": "42"})
					} else {
						_, err := io.Copy(io.Discard, r.Body)
						require.NoError(t, err)
						w.WriteHeader(202)
					}
					return
				}
				if r.URL.Query().Get("action") == "get-validation-status" {
					item := api.Media{ID: "42", Name: "target.txt", Size: 6, Type: "file"}
					switch mismatch {
					case "size":
						item.Size = 5
					case "name":
						item.Name = "different.txt"
					case "parent":
						item.FolderID = "9"
					}
					fx.mu.Lock()
					fx.media = []api.Media{item}
					fx.mu.Unlock()
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			in, err := source.Open(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, in.Close()) }()
			_, err = f.Put(ctx, in, fs.NewOverrideRemote(source, "target.txt"))
			require.ErrorContains(t, err, "does not match")
			op := &recoveryRecords{}
			require.NoError(t, f.uploadJournal.Do(false, op))
			require.Len(t, op.items, 1)
			assert.True(t, op.items[0].Processing)
		})
	}
}

func TestUploadRecoveryLockCancellation(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "contention")
	source, _ := recoverySource(t)
	m := fx.config(t)
	m["async_upload"], m["resume_uploads"] = "true", "true"
	remote, err := NewFs(ctx, "another-alias", "", m)
	require.NoError(t, err)
	other := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, other.Shutdown(ctx)) })
	assert.Equal(t, f.uploadJournal.Path(), other.uploadJournal.Path())
	entered, release := make(chan struct{}), make(chan struct{})
	var metadataCalls atomic.Int32
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/upload/file" {
			if r.URL.Query().Get("action") == "save-metadata" {
				metadataCalls.Add(1)
				jsonReply(t, w, map[string]string{"id": "42"})
			} else {
				close(entered)
				<-release
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				w.WriteHeader(202)
			}
			return
		}
		if r.URL.Query().Get("action") == "get-validation-status" {
			fx.mu.Lock()
			fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			fx.mu.Unlock()
			jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
			return
		}
		handler.ServeHTTP(w, r)
	})
	done := make(chan error, 1)
	go func() {
		in, err := source.Open(ctx)
		if err == nil {
			defer func() { _ = in.Close() }()
			_, err = f.Put(ctx, in, fs.NewOverrideRemote(source, "target.txt"))
		}
		done <- err
	}()
	<-entered
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	in, err := source.Open(ctx)
	require.NoError(t, err)
	_, err = other.Put(waitCtx, in, fs.NewOverrideRemote(source, "target.txt"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, in.Close())
	assert.EqualValues(t, 1, metadataCalls.Load())
	close(release)
	require.NoError(t, <-done)
	op := &recoveryRecords{}
	require.NoError(t, f.uploadJournal.Do(false, op))
	assert.Empty(t, op.items)
}

func TestUploadRecoveryWaitsForAnotherProcess(t *testing.T) {
	if os.Getenv("RCLONE_O2_RECOVERY_TEST") != "" {
		t.Skip("parent process test")
	}
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "external-lock")
	source, _ := recoverySource(t)
	data := api.Upload{Name: "target.txt", Size: 6}
	owner, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, data, fs.NewOverrideRemote(source, data.Name), strings.NewReader("abcdef"))
	require.NoError(t, err)
	require.NotNil(t, owner)
	defer func() { require.NoError(t, owner.lock.Unlock()) }()

	// A distinct process must respect the same destination lock across aliases.
	helper := recoveryHelper(t)
	input := recoveryProcessInput{URL: fx.server.URL, CacheDir: config.GetCacheDir(), SourceDir: source.Fs().Root(), Name: "external-alias", WantError: true}
	b, err := json.Marshal(input)
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, helper, "-test.run=^TestUploadRecoveryProcess$")
	cmd.Env = append(os.Environ(), "RCLONE_O2_RECOVERY_TEST="+string(b), "RCLONE_O2_RECOVERY_TIMEOUT=100ms")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	fx.mu.Lock()
	assert.Zero(t, fx.requests["/sapi/upload/file"])
	fx.mu.Unlock()
}

func TestUploadRecoveryDestinationChangedWhileWaiting(t *testing.T) {
	for _, change := range []string{"created", "removed"} {
		t.Run(change, func(t *testing.T) {
			fx := newFixture(t)
			if change == "removed" {
				fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			}
			f, ctx := recoveryFs(t, fx, "wait-for-destination")
			source, _ := recoverySource(t)
			data := api.Upload{Name: "target.txt", Size: 6}
			if change == "removed" {
				data.ID = "42"
			}
			owner, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, data, source, strings.NewReader("abcdef"))
			require.NoError(t, err)
			defer func() { require.NoError(t, owner.lock.Unlock()) }()
			read := make(chan struct{})
			var once sync.Once
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r)
				if r.URL.Path == "/sapi/media" && r.URL.Query().Get("action") == "get" {
					once.Do(func() { close(read) })
				}
			})
			done := make(chan error, 1)
			go func() {
				in, err := source.Open(ctx)
				if err == nil {
					defer func() { _ = in.Close() }()
					_, err = f.Put(ctx, in, fs.NewOverrideRemote(source, "target.txt"))
				}
				done <- err
			}()
			<-read
			// Keep ownership while the competing copy finishes its destination lookup.
			select {
			case err := <-done:
				t.Fatalf("upload did not wait for ownership: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			fx.mu.Lock()
			if change == "removed" {
				fx.media = nil
			} else {
				fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			}
			fx.mu.Unlock()
			require.NoError(t, owner.lock.Unlock())
			err = <-done
			require.ErrorContains(t, err, "destination changed")
			assert.False(t, fserrors.IsRetryError(err))
			if change == "removed" {
				assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
			}
			fx.mu.Lock()
			assert.Zero(t, fx.requests["/sapi/upload/file"])
			fx.mu.Unlock()
		})
	}
}

func TestUploadRecoveryAccountIsolation(t *testing.T) {
	fx := newFixture(t)
	fx.accountID = "first-account"
	f, ctx := recoveryFs(t, fx, "same-alias")
	fx.mu.Lock()
	fx.accountID = "second-account"
	fx.mu.Unlock()
	m := fx.config(t)
	m["async_upload"], m["resume_uploads"] = "true", "true"
	remote, err := NewFs(ctx, "same-alias", "", m)
	require.NoError(t, err)
	other := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, other.Shutdown(ctx)) })
	assert.NotEqual(t, f.uploadJournal.Path(), other.uploadJournal.Path())
	assert.NoError(t, f.uploadJournal.Do(true, &uploadJournalOp{key: "test", record: &uploadRecord{Version: uploadJournalVersion, ID: "42"}, write: true}))
	op := &recoveryRecords{}
	err = other.uploadJournal.Do(false, op)
	assert.ErrorIs(t, err, kv.ErrEmpty)
	assert.Empty(t, op.items)
}

func TestUploadRecoveryEquivalentAPIPaths(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "first-path")
	m := fx.config(t)
	m["async_upload"], m["resume_uploads"], m["api_path"] = "true", "true", "sapi/"
	remote, err := NewFs(ctx, "second-path", "", m)
	require.NoError(t, err)
	other := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, other.Shutdown(ctx)) })
	assert.Equal(t, f.uploadJournal.Path(), other.uploadJournal.Path())
	source, _ := recoverySource(t)
	data := api.Upload{Name: "target.txt", Size: 6}
	owner, err := (&Object{fs: f, remote: "target.txt"}).prepareUploadRecovery(ctx, data, source, strings.NewReader("abcdef"))
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.lock.Unlock()) }()
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	competitor, err := (&Object{fs: other, remote: "target.txt"}).prepareUploadRecovery(waitCtx, data, source, strings.NewReader("abcdef"))
	if competitor != nil {
		defer func() { require.NoError(t, competitor.lock.Unlock()) }()
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestUploadRecoveryUnsupportedSource(t *testing.T) {
	fx := newFixture(t)
	f, ctx := recoveryFs(t, fx, "no-source-hash")
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/upload/file" {
			if r.URL.Query().Get("action") == "save-metadata" {
				jsonReply(t, w, map[string]string{"id": "42"})
			} else {
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				w.WriteHeader(202)
			}
			return
		}
		if r.URL.Query().Get("action") == "get-validation-status" {
			fx.mu.Lock()
			fx.media = []api.Media{{ID: "42", Name: "target.txt", Size: 6, Type: "file"}}
			fx.mu.Unlock()
			jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
			return
		}
		handler.ServeHTTP(w, r)
	})
	src := object.NewStaticObjectInfo("target.txt", time.Now(), 6, true, nil, f)
	_, err := f.Put(ctx, strings.NewReader("abcdef"), src)
	require.NoError(t, err)
	op := &recoveryRecords{}
	err = f.uploadJournal.Do(false, op)
	assert.ErrorIs(t, err, kv.ErrEmpty)
}
