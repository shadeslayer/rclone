package onemediahub

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/crypt"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	fscache "github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flatCryptConfig(t *testing.T, mode string) configmap.Simple {
	t.Helper()
	ri, err := fs.Find("crypt")
	require.NoError(t, err)
	m := configmap.Simple{}
	for _, opt := range ri.Options {
		m[opt.Name] = opt.String()
	}
	m["password"], m["filename_encryption"] = obscure.MustObscure("test"), mode
	return m
}

func TestFlatCryptLifecycle(t *testing.T) {
	for _, mode := range []string{"off", "standard"} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cached=%v", mode, cached), func(t *testing.T) {
				fx := newFlatFixture(t)
				fx.nextID = 100
				m := flatCryptConfig(t, mode)
				cipher, err := crypt.NewCipher(m)
				require.NoError(t, err)
				marker, err := flatName(cipher.EncryptDirName("upload/upload"), true)
				require.NoError(t, err)
				fx.media = []api.Media{{ID: "20", FolderID: "1", Name: marker + " (1)", Type: "file", Status: "U"}}
				f, ctx := flatTestFs(t, fx, "", cached)
				m["remote"] = fs.ConfigString(f)
				fscache.Put(m["remote"], f)
				t.Cleanup(func() { fscache.ClearConfig(f.Name()) })
				wrapped, err := crypt.NewFs(ctx, "crypt-flat-test", "", m)
				require.NoError(t, err)
				names := []string{
					"upload/upload/0_thumbnail.webp",
					"upload/upload/" + strings.Repeat("hash", 40) + "_thumbnail.webp",
					"upload/upload/11111111-2222-4333-8444-555555555555/ff/f7/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.HEIC",
					"upload/thumbs/11111111-2222-4333-8444-555555555555/fe/eb/01234567-89ab-4cde-8fab-0123456789ab_preview.jpeg",
				}
				modified := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				for _, name := range names {
					src := object.NewStaticObjectInfo(name, modified, 5, true, nil, wrapped)
					_, err := wrapped.Put(ctx, strings.NewReader("hello"), src)
					require.NoError(t, err)
				}
				require.NoError(t, wrapped.Mkdir(ctx, "upload/empty"))
				var listed []string
				require.NoError(t, wrapped.Features().ListR(ctx, "", func(entries fs.DirEntries) error {
					for _, entry := range entries {
						if _, ok := entry.(fs.Object); ok {
							listed = append(listed, entry.Remote())
						}
					}
					return nil
				}))
				assert.ElementsMatch(t, names, listed)
				for _, name := range names {
					entries, err := wrapped.List(ctx, path.Dir(name))
					require.NoError(t, err)
					assert.Contains(t, flatEntryNames(entries), name)
					obj, err := wrapped.NewObject(ctx, name)
					require.NoError(t, err)
					body, err := obj.Open(ctx)
					require.NoError(t, err)
					content, err := io.ReadAll(body)
					require.NoError(t, err)
					require.NoError(t, body.Close())
					assert.Equal(t, "hello", string(content))
					src := object.NewStaticObjectInfo(name, modified, 7, true, nil, wrapped)
					require.NoError(t, obj.Update(ctx, strings.NewReader("updated"), src))
					require.NoError(t, obj.Remove(ctx))
					_, err = wrapped.NewObject(ctx, name)
					require.ErrorIs(t, err, fs.ErrorObjectNotFound)
				}
				require.NoError(t, wrapped.Rmdir(ctx, "upload/empty"))
				require.NoError(t, wrapped.Rmdir(ctx, "upload/upload/11111111-2222-4333-8444-555555555555/ff/f7"))
				fx.mu.Lock()
				assert.True(t, slices.ContainsFunc(fx.media, func(item api.Media) bool { return isFlatMapping(item.Name) }), "long crypt paths must use immutable mappings")
				fx.mu.Unlock()
				assert.Zero(t, fx.folderWrites.Load())
			})
		}
	}
}

func TestFlatCryptCopySyncCLI(t *testing.T) {
	binary := os.Getenv("RCLONE_ONEMEDIAHUB_TEST_BINARY")
	if binary == "" {
		t.Skip("requires the freshly built rclone binary")
	}
	for _, mode := range []string{"off", "standard"} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cached=%v", mode, cached), func(t *testing.T) {
				fx := newFlatFixture(t)
				fx.nextID = 100
				m := flatCryptConfig(t, mode)
				cipher, err := crypt.NewCipher(m)
				require.NoError(t, err)
				marker, err := flatName(cipher.EncryptDirName("upload/upload"), true)
				require.NoError(t, err)
				fx.media = []api.Media{{ID: "20", FolderID: "1", Name: marker + " (1)", Type: "file", Status: "U"}}
				fx.changes = map[string]api.Changes{"file": {New: []api.ID{"20"}}}
				source, destination, cacheDir := t.TempDir(), t.TempDir(), t.TempDir()
				configPath := filepath.Join(t.TempDir(), "rclone.conf")
				text := fmt.Sprintf("[o2]\ntype = onemediahub\nurl = %s\nauth_type = password\nuser = test\npassword = %s\nflat_namespace = true\nroot_folder_id = 1\nmetadata_cache = %v\n\n[encrypted]\ntype = crypt\nremote = o2:\npassword = %s\nfilename_encryption = %s\n", fx.server.URL, fx.config(t)["password"], cached, m["password"], mode)
				require.NoError(t, os.WriteFile(configPath, []byte(text), 0600))
				names := []string{
					"upload/upload/0_thumbnail.webp",
					"upload/upload/" + strings.Repeat("hash", 40) + "_thumbnail.webp",
					"upload/upload/11111111-2222-4333-8444-555555555555/ff/f7/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.HEIC",
					"upload/thumbs/11111111-2222-4333-8444-555555555555/fe/eb/01234567-89ab-4cde-8fab-0123456789ab_preview.jpeg",
				}
				write := func(name, content string) {
					file := filepath.Join(source, filepath.FromSlash(name))
					require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
					require.NoError(t, os.WriteFile(file, []byte(content), 0600))
				}
				for _, name := range names {
					write(name, "hello "+name)
				}
				run := func(args ...string) {
					args = append(args, "--config", configPath, "--cache-dir", cacheDir, "--retries", "1", "--low-level-retries", "1", "--fast-list")
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					child := exec.CommandContext(ctx, binary, args...)
					for _, entry := range os.Environ() {
						if !strings.HasPrefix(entry, "RCLONE_") {
							child.Env = append(child.Env, entry)
						}
					}
					output, err := child.CombinedOutput()
					require.NoError(t, err, "%s", output)
				}
				run("copy", source, "encrypted:")
				write(names[0], "replacement thumbnail")
				require.NoError(t, os.Remove(filepath.Join(source, filepath.FromSlash(names[2]))))
				fx.mu.Lock()
				fx.changes = map[string]api.Changes{}
				fx.mu.Unlock()
				run("sync", source, "encrypted:", "--delete-after")
				run("copy", "encrypted:", destination)
				for _, name := range []string{names[0], names[1], names[3]} {
					expected, err := os.ReadFile(filepath.Join(source, filepath.FromSlash(name)))
					require.NoError(t, err)
					actual, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(name)))
					require.NoError(t, err)
					assert.Equal(t, expected, actual)
				}
				_, err = os.Stat(filepath.Join(destination, filepath.FromSlash(names[2])))
				assert.True(t, os.IsNotExist(err))
				assert.Zero(t, fx.folderWrites.Load())
			})
		}
	}
}
