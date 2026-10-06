package onemediahub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/onemediahub/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/lib/kv"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func testConfig(t *testing.T, values configmap.Simple) configmap.Simple {
	t.Helper()
	ri, err := fs.Find("onemediahub")
	require.NoError(t, err)
	m := configmap.Simple{}
	for _, opt := range ri.Options {
		m[opt.Name] = opt.String()
	}
	for key, value := range values {
		m[key] = value
	}
	return m
}

func jsonReply(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	assert.NoError(t, json.NewEncoder(w).Encode(value))
}

func TestMetadataCache(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "directory", Status: "U"}}
	fx.media = []api.Media{{ID: "2", FolderID: "1", Name: "old.txt", Size: 3, Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"1"}}, "file": {New: []api.ID{"2"}}}
	m := testConfig(t, configmap.Simple{"url": fx.server.URL, "auth_type": authPassword, "user": "alice", "password": obscure.MustObscure("password"), "metadata_cache": "true"})
	remote, err := NewFs(ctx, "metadata-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			obj, err := f.NewObject(ctx, "directory/old.txt")
			assert.NoError(t, err)
			if obj != nil {
				assert.EqualValues(t, 3, obj.Size())
			}
		})
	}
	wg.Wait()
	fx.mu.Lock()
	assert.Equal(t, 1, fx.requests["/sapi/profile/changes"])
	assert.Equal(t, 1, fx.requests["/sapi/media"])
	assert.Equal(t, 1, fx.requests["/sapi/media/folder"])
	fx.requestTime += 60000
	fx.folders[0].Name = "renamed"
	fx.media = []api.Media{{ID: "3", FolderID: "1", Name: "new.txt", Size: 7, Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {Updated: []api.ID{"1"}}, "file": {New: []api.ID{"3"}, Deleted: []api.ID{"2"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	for range 12 {
		wg.Go(func() {
			_, err := f.NewObject(ctx, "directory/old.txt")
			assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
			_, err = f.NewObject(ctx, "renamed/new.txt")
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	obj, err := f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 7, obj.Size())

	// A failed metadata fetch must leave the saved change cursor unchanged.
	fx.mu.Lock()
	fx.requestTime += 60000
	fx.media[0].Size = 9
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"3"}}}
	fx.failMedia = true
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	anchor := f.metadata.state.Anchor
	f.metadata.mu.Unlock()
	_, err = f.NewObject(ctx, "renamed/new.txt")
	require.ErrorContains(t, err, "failed metadata")
	assert.Equal(t, anchor, f.metadata.state.Anchor)
	fx.mu.Lock()
	fx.failMedia = false
	fx.mu.Unlock()
	obj, err = f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 9, obj.Size())

	// Download lookups must not rewrite comparison metadata outside a changes refresh.
	fx.mu.Lock()
	fx.media[0].Name = "download-name.txt"
	fx.media[0].URL = fx.server.URL + "/content/3"
	fx.content["3"] = "123456789"
	fx.mu.Unlock()
	body, err := obj.Open(ctx)
	require.NoError(t, err)
	contentBytes, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	assert.Equal(t, "123456789", string(contentBytes))
	_, err = f.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].Name = "new.txt"
	fx.mu.Unlock()

	// A second filesystem reads the persisted snapshot and applies only changes.
	fx.mu.Lock()
	fx.changes = map[string]api.Changes{}
	folderRequests := fx.requests["/sapi/media/folder"]
	mediaRequests := fx.requests["/sapi/media"]
	fx.mu.Unlock()
	restarted, err := NewFs(ctx, "metadata-test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.(*Fs).Shutdown(ctx)) })
	obj, err = restarted.NewObject(ctx, "renamed/new.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 9, obj.Size())
	fx.mu.Lock()
	assert.Equal(t, folderRequests, fx.requests["/sapi/media/folder"])
	assert.Equal(t, mediaRequests, fx.requests["/sapi/media"])
	fx.mu.Unlock()

	// Local writes must be visible before the next changes poll.
	fx.mu.Lock()
	fx.nextID = 10
	fx.mu.Unlock()
	content := "cached upload"
	src := object.NewStaticObjectInfo("created/file.txt", time.Now(), int64(len(content)), true, nil, f)
	uploaded, err := f.Put(ctx, strings.NewReader(content), src)
	require.NoError(t, err)
	listed, err := f.List(ctx, "created")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	found, err := f.NewObject(ctx, src.Remote())
	require.NoError(t, err)
	assert.Equal(t, uploaded.(*Object).ID(), found.(*Object).ID())
	require.NoError(t, uploaded.Remove(ctx))
	_, err = f.NewObject(ctx, src.Remote())
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	require.NoError(t, f.Rmdir(ctx, "created"))

	// Directory removal must check external changes even within the cache interval.
	require.NoError(t, f.Mkdir(ctx, "external"))
	parent, err := f.dirCache.FindDir(ctx, "external", false)
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media = append(fx.media, api.Media{ID: "99", FolderID: api.ID(parent), Name: "external.txt", Size: 1, Type: "file"})
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"99"}}}
	fx.mu.Unlock()
	assert.ErrorIs(t, f.Rmdir(ctx, "external"), fs.ErrorDirectoryNotEmpty)
}

func TestMetadataCacheSoftDeletedChanges(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000000
	fx.folders = []api.Folder{{ID: "1", Name: "parent"}, {ID: "2", ParentID: "1", Name: "child"}}
	fx.media = []api.Media{{ID: "3", FolderID: "2", Name: "trashed"}, {ID: "4", Name: "deleted"}, {ID: "5", Name: "kept"}}
	fx.changes = map[string]api.Changes{"folder": {Locked: []api.ID{"1", "2"}}, "file": {New: []api.ID{"3", "4", "5"}, Locked: []api.ID{"3", "4"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "soft-deleted", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	_, err = f.NewObject(ctx, "parent/child/trashed")
	require.NoError(t, err)
	assert.ElementsMatch(t, []api.ID{"3", "4"}, f.metadata.state.Pending)
	assert.ElementsMatch(t, []api.ID{"1", "2"}, f.metadata.state.PendingFolders)
	fx.mu.Lock()
	fx.requestTime += 60000
	fx.changeData = map[string]any{
		"folder": map[string]any{"S": []api.ID{"1", "2"}, "D": []api.ID{"1"}},
		"file":   map[string]any{"S": []api.ID{"3"}, "D": []api.ID{"3", "4"}},
	}
	fx.mu.Unlock()
	f.expireMetadata()
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "kept", entries[0].Remote())
	_, err = f.NewObject(ctx, "parent/child/trashed")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.Empty(t, f.metadata.state.Folders)
	assert.Empty(t, f.metadata.state.Pending)
	assert.Empty(t, f.metadata.state.PendingFolders)
	assert.Equal(t, fx.requestTime, f.metadata.state.Anchor)
}

func TestTrashedMetadataViews(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			cacheDir := config.GetCacheDir()
			require.NoError(t, config.SetCacheDir(t.TempDir()))
			t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
			fx := newFixture(t)
			fx.requestTime = 1700000000000
			fx.folders = []api.Folder{{ID: "1", Name: "trash", Status: "S"}, {ID: "2", Name: "active"}}
			fx.media = []api.Media{{ID: "3", FolderID: "1", Name: "hidden.txt"}, {ID: "4", Name: "keep.txt", Size: 5}}
			fx.changes = map[string]api.Changes{"file": {New: []api.ID{"3", "4"}}}
			m := fx.config(t)
			m["metadata_cache"] = strconv.FormatBool(cached)
			ctx := context.Background()
			remote, err := NewFs(ctx, "trash-views", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			if cached {
				// A cached trash record can survive a changes interval with no matching delta.
				item := api.Media{ID: "90", Name: "gone.txt", Status: "S"}
				f.metadata.state.Media[item.ID] = item
				f.metadata.byName[f.mediaKey(item)] = item.ID
			}
			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"active/", "keep.txt"}, flatEntryNames(entries))
			var recursive []string
			require.NoError(t, f.ListR(ctx, "", func(entries fs.DirEntries) error {
				recursive = append(recursive, flatEntryNames(entries)...)
				return nil
			}))
			assert.ElementsMatch(t, []string{"active/", "keep.txt"}, recursive)
			_, err = f.NewObject(ctx, "trash/hidden.txt")
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
			_, err = f.NewObject(ctx, "gone.txt")
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
		})
	}
}

func TestMetadataCacheTrashedWrites(t *testing.T) {
	fx := newFlatFixture(t)
	f, ctx := flatTestFs(t, fx, "", true)
	live := api.Media{ID: "3", FolderID: "1", Name: flatFixtureName("keep.txt", false), Size: 5}
	f.cacheMedia(live, false)
	for _, item := range []api.Media{
		{ID: "90", FolderID: "1", Name: live.Name, Status: "S"},
		{ID: "91", FolderID: "1", Name: live.Name, Status: "D"},
		{ID: "92", FolderID: "1", Name: live.Name, SoftDeleted: true},
	} {
		f.cacheMedia(item, false)
		obj, err := f.NewObject(ctx, "keep.txt")
		require.NoError(t, err)
		assert.EqualValues(t, 5, obj.Size())
		f.cacheFolder(api.Folder{ID: item.ID, Name: "trash", Status: item.Status, SoftDeleted: item.SoftDeleted}, false)
		folders, err := f.folders(ctx)
		require.NoError(t, err)
		assert.False(t, slices.ContainsFunc(folders, func(folder api.Folder) bool { return folder.ID == item.ID }))
	}
}

func TestListR(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, mode := range []string{"directory", "path-root", "id-root"} {
			t.Run(fmt.Sprintf("cached=%v/%s", cached, mode), func(t *testing.T) {
				cacheDir := config.GetCacheDir()
				require.NoError(t, config.SetCacheDir(t.TempDir()))
				t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
				ctx := context.Background()
				fx := newFixture(t)
				fx.requestTime = 1700000000000
				fx.folders = []api.Folder{{ID: "1", Name: "base"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "3", ParentID: "2", Name: "empty"}, {ID: "4", Name: "outside"}, {ID: "5", ParentID: "999", Name: "orphan"}, {ID: "6", ParentID: "1", Name: "trash", SoftDeleted: true}}
				fx.media = []api.Media{{ID: "10", FolderID: "0", Name: "unfiled"}, {ID: "11", FolderID: "1", Name: "file"}, {ID: "12", FolderID: "2", Name: "duplicate"}, {ID: "13", FolderID: "4", Name: "outside"}, {ID: "14", FolderID: "6", Name: "hidden"}, {ID: "15", FolderID: "2", Name: "duplicate"}}
				fx.changes = map[string]api.Changes{"file": {New: []api.ID{"10", "11", "12", "13", "14", "15"}}}
				m := fx.config(t)
				m["metadata_cache"] = strconv.FormatBool(cached)
				root, dir, prefix := "", "base", "base/"
				if mode == "path-root" {
					root, dir, prefix = "base", "", ""
				} else if mode == "id-root" {
					m["root_folder_id"] = "1"
					dir, prefix = "", ""
				}
				remote, err := NewFs(ctx, "list-r", root, m)
				require.NoError(t, err)
				f := remote.(*Fs)
				t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
				require.NotNil(t, f.Features().ListR)
				before := fx.requests["/sapi/media"]
				var got []string
				err = f.Features().ListR(ctx, dir, func(entries fs.DirEntries) error {
					for _, entry := range entries {
						got = append(got, entry.Remote()+"#"+entry.(fs.IDer).ID())
					}
					// A callback may call back into the backend.
					_, err := f.NewObject(ctx, prefix+"file")
					return err
				})
				require.NoError(t, err)
				assert.ElementsMatch(t, []string{prefix + "child#2", prefix + "child/empty#3", prefix + "file#11", prefix + "child/duplicate#12", prefix + "child/duplicate#15"}, got)
				if cached {
					assert.Equal(t, before, fx.requests["/sapi/media"])
				}
				assert.ErrorIs(t, f.Features().ListR(ctx, "missing", func(fs.DirEntries) error { t.Fatal("unexpected callback"); return nil }), fs.ErrorDirNotFound)
			})
		}
	}
}

func TestListRCallbackAndCancellation(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	for i := range 201 {
		fx.media = append(fx.media, api.Media{ID: api.ID(strconv.Itoa(i + 1)), Name: strconv.Itoa(i)})
	}
	remote, err := NewFs(ctx, "list-r-errors", "", fx.config(t))
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	require.NotNil(t, f.Features().ListR)
	wantErr := fmt.Errorf("stop listing")
	calls := 0
	err = f.Features().ListR(ctx, "", func(entries fs.DirEntries) error {
		calls++
		assert.Len(t, entries, 100)
		return wantErr
	})
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = f.Features().ListR(ctx, "", func(fs.DirEntries) error { cancel(); return nil })
	assert.ErrorIs(t, err, context.Canceled)
}

func TestListRCyclicFolders(t *testing.T) {
	fx := newFixture(t)
	fx.folders = []api.Folder{{ID: "1", ParentID: "1", Name: "cycle"}}
	m := fx.config(t)
	m["root_folder_id"] = "1"
	remote, err := NewFs(context.Background(), "list-r-cycle", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	err = f.ListR(context.Background(), "", func(fs.DirEntries) error { t.Fatal("unexpected callback"); return nil })
	assert.ErrorContains(t, err, "repeats ID")
}

type changeNotification struct {
	remote string
	kind   fs.EntryType
}

func TestChangeNotify(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newFixture(t)
	fx.requestTime = 1700000000000
	fx.folders = []api.Folder{{ID: "1", Name: "base"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "6", ParentID: "2", Name: "empty"}, {ID: "7", Name: "base-other"}}
	fx.media = []api.Media{{ID: "3", FolderID: "2", Name: "old"}, {ID: "4", FolderID: "7", Name: "outside"}}
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"3", "4"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "notify", "base", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	require.NotNil(t, f.Features().ChangeNotify)
	intervals := make(chan time.Duration)
	defer close(intervals)
	events := make(chan changeNotification, 64)
	f.Features().ChangeNotify(ctx, func(remote string, kind fs.EntryType) {
		_, err := f.List(ctx, "")
		assert.NoError(t, err)
		events <- changeNotification{remote, kind}
	}, intervals)
	intervals <- 25 * time.Millisecond
	fx.mu.Lock()
	fx.requestTime += 1000
	fx.folders[1].Name = "renamed"
	fx.media[0].Name = "new"
	fx.media[1].Name = "ignored"
	fx.changes = map[string]api.Changes{"folder": {Updated: []api.ID{"2"}}, "file": {Updated: []api.ID{"3", "4"}}}
	fx.mu.Unlock()
	receive := func(n int) []changeNotification {
		t.Helper()
		got := make([]changeNotification, 0, n)
		for range n {
			select {
			case event := <-events:
				got = append(got, event)
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for notifications")
			}
		}
		return got
	}
	assert.ElementsMatch(t, []changeNotification{{"child", fs.EntryDirectory}, {"renamed", fs.EntryDirectory}, {"child/empty", fs.EntryDirectory}, {"renamed/empty", fs.EntryDirectory}, {"child/old", fs.EntryObject}, {"renamed/new", fs.EntryObject}}, receive(6))
	intervals <- 0
	// Let an already running request finish before checking that polling is paused.
	time.Sleep(75 * time.Millisecond)
	fx.mu.Lock()
	paused := fx.requests["/sapi/profile/changes"]
	fx.mu.Unlock()
	time.Sleep(75 * time.Millisecond)
	fx.mu.Lock()
	assert.Equal(t, paused, fx.requests["/sapi/profile/changes"])
	fx.requestTime += 1000
	fx.media[0].Name = "third"
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"3"}}}
	fx.mu.Unlock()
	// A foreground refresh must not consume the notifier's changes.
	f.expireMetadata()
	_, err = f.NewObject(ctx, "renamed/third")
	require.NoError(t, err)
	intervals <- 25 * time.Millisecond
	assert.ElementsMatch(t, []changeNotification{{"renamed/new", fs.EntryObject}, {"renamed/third", fs.EntryObject}}, receive(2))
	fx.mu.Lock()
	fx.requestTime += 1000
	fx.changeData = map[string]any{"folder": map[string]any{"S": []api.ID{"2"}}, "file": map[string]any{"S": []api.ID{"3"}}}
	fx.mu.Unlock()
	assert.ElementsMatch(t, []changeNotification{{"renamed", fs.EntryDirectory}, {"renamed/empty", fs.EntryDirectory}, {"renamed/third", fs.EntryObject}}, receive(3))
}

func TestChangeNotifyIntervalDuringRequest(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	fx := newFixture(t)
	fx.requestTime = 1700000000000
	fx.changes = map[string]api.Changes{}
	var block atomic.Bool
	started, stopped := make(chan struct{}, 1), make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() && r.URL.Path == "/sapi/profile/changes" {
			started <- struct{}{}
			<-r.Context().Done()
			stopped <- struct{}{}
			return
		}
		fx.serve(t, w, r)
	}))
	defer server.Close()
	m := fx.config(t)
	m["url"], m["metadata_cache"] = server.URL, "true"
	remote, err := NewFs(context.Background(), "notify-blocked", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	require.NotNil(t, f.Features().ChangeNotify)
	intervals := make(chan time.Duration)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.Features().ChangeNotify(ctx, func(string, fs.EntryType) { t.Error("unexpected notification") }, intervals)
	block.Store(true)
	select {
	case intervals <- 10 * time.Millisecond:
	case <-time.After(time.Second):
		t.Fatal("initial interval was not accepted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	select {
	case intervals <- 0:
	case <-time.After(time.Second):
		t.Fatal("interval update blocked behind request")
	}
	close(intervals)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("channel closure did not cancel request")
	}
}

func TestChangeNotifyFailedRefresh(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newFixture(t)
	fx.requestTime = 1700000000000
	fx.media = []api.Media{{ID: "1", Name: "old"}}
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"1"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "notify-failure", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	intervals := make(chan time.Duration)
	defer close(intervals)
	events := make(chan changeNotification, 8)
	f.ChangeNotify(ctx, func(remote string, kind fs.EntryType) { events <- changeNotification{remote, kind} }, intervals)
	intervals <- 25 * time.Millisecond
	require.Eventually(t, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return fx.requests["/sapi/profile/changes"] >= 2
	}, time.Second, time.Millisecond)
	fx.mu.Lock()
	before := fx.requests["/sapi/media"]
	fx.media[0].Name = "new"
	fx.requestTime += 1000
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"1"}}}
	fx.failMedia = true
	fx.mu.Unlock()
	require.Eventually(t, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return fx.requests["/sapi/media"] > before
	}, time.Second, time.Millisecond)
	f.metadata.mu.Lock()
	assert.EqualValues(t, 1700000000000, f.metadata.state.Anchor)
	f.metadata.mu.Unlock()
	select {
	case event := <-events:
		t.Fatalf("failed refresh emitted notification: %+v", event)
	default:
	}
	fx.mu.Lock()
	fx.failMedia = false
	fx.mu.Unlock()
	var got []changeNotification
	for range 2 {
		select {
		case event := <-events:
			got = append(got, event)
		case <-time.After(time.Second):
			t.Fatal("successful retry lost change notifications")
		}
	}
	assert.ElementsMatch(t, []changeNotification{{"old", fs.EntryObject}, {"new", fs.EntryObject}}, got)
}

func TestChangeNotifyStartsBeforeLocalWrites(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx := newFixture(t)
	fx.requestTime = 1700000000000
	fx.media = []api.Media{{ID: "1", Name: "old"}}
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"1"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "notify-startup", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	intervals := make(chan time.Duration)
	defer close(intervals)
	events := make(chan changeNotification, 8)
	registered, accepted := make(chan struct{}), make(chan struct{})
	f.metadata.mu.Lock()
	go func() {
		f.ChangeNotify(ctx, func(remote string, kind fs.EntryType) { events <- changeNotification{remote, kind} }, intervals)
		close(registered)
	}()
	go func() { intervals <- 25 * time.Millisecond; close(accepted) }()
	select {
	case <-accepted:
		t.Error("initial interval acknowledged before the baseline could be captured")
	case <-time.After(50 * time.Millisecond):
	}
	f.metadata.mu.Unlock()
	<-registered
	<-accepted
	f.cacheMedia(api.Media{ID: "1", Name: "new"}, false)
	fx.mu.Lock()
	fx.media[0].Name = "new"
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	var got []changeNotification
	for range 2 {
		select {
		case event := <-events:
			got = append(got, event)
		case <-time.After(time.Second):
			t.Fatal("local write after registration was not reported")
		}
	}
	assert.ElementsMatch(t, []changeNotification{{"old", fs.EntryObject}, {"new", fs.EntryObject}}, got)
}

func TestChangeNotifyDisabled(t *testing.T) {
	fx := newFixture(t)
	remote, err := NewFs(context.Background(), "notify-disabled", "", fx.config(t))
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	assert.Nil(t, f.Features().ChangeNotify)
}

func TestDirMove(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			cacheDir := config.GetCacheDir()
			require.NoError(t, config.SetCacheDir(t.TempDir()))
			t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
			ctx := context.Background()
			fx := newFixture(t)
			fx.requestTime = 1700000000123
			fx.changes = map[string]api.Changes{"file": {New: []api.ID{"4"}}}
			fx.folders = []api.Folder{{ID: "1", Name: "old"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "3", Name: "destination"}}
			fx.media = []api.Media{{ID: "4", FolderID: "2", Name: "file", Size: 1}}
			m := fx.config(t)
			m["metadata_cache"] = strconv.FormatBool(cached)
			remote, err := NewFs(ctx, "move", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			move := f.Features().DirMove
			require.NotNil(t, move)
			_, err = f.NewObject(ctx, "old/child/file")
			require.NoError(t, err)
			assert.ErrorIs(t, move(ctx, f, "old", "destination"), fs.ErrorDirExists)
			before := len(fx.folders)
			require.Error(t, move(ctx, f, "old", "old/child/new/deeper"))
			assert.Len(t, fx.folders, before)
			require.Error(t, move(ctx, f, "", "root"))
			require.NoError(t, move(ctx, f, "old", "destination/renamed"))
			obj, err := f.NewObject(ctx, "destination/renamed/child/file")
			require.NoError(t, err)
			assert.Equal(t, "4", obj.(*Object).ID())
			_, err = f.NewObject(ctx, "old/child/file")
			assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
			assert.Equal(t, api.ID("1"), fx.folders[0].ID)
			assert.Equal(t, api.ID("3"), fx.folders[0].ParentID)
			assert.Zero(t, fx.requests["/sapi/upload"])
		})
	}
}

type snapshotBytes struct{ data []byte }

func (op *snapshotBytes) Do(_ context.Context, bucket kv.Bucket) error {
	op.data = slices.Clone(bucket.Get([]byte("metadata")))
	return nil
}

func TestETagMetadataCache(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.changes = map[string]api.Changes{"file": {New: []api.ID{"1"}}}
	var expiredCalls atomic.Int32
	expired := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { expiredCalls.Add(1); w.WriteHeader(http.StatusForbidden) }))
	defer expired.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 6, Date: 1, Modified: 1000, URL: expired.URL, ETag: "version-one"}}
	fx.content["1"] = "abcdef"
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "etag-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	o, err := f.NewObject(ctx, "file")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].URL = fx.server.URL + "/content/1"
	fx.mu.Unlock()
	read := func(o fs.Object) {
		body, err := o.Open(ctx)
		require.NoError(t, err)
		b, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		require.Equal(t, "abcdef", string(b))
	}
	read(o)
	// A rename and timestamp update leave the content version unchanged.
	fx.mu.Lock()
	fx.media[0].Name = "renamed"
	fx.media[0].URL = expired.URL
	fx.media[0].Date = 2
	fx.media[0].Modified = 2000
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"1"}}}
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	o, err = f.NewObject(ctx, "renamed")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].URL = fx.server.URL + "/content/1"
	fx.mu.Unlock()
	read(o)
	assert.EqualValues(t, 1, expiredCalls.Load())
	assert.Equal(t, time.UnixMilli(2000), o.ModTime(ctx))
	_, err = f.NewObject(ctx, "file")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	before := &snapshotBytes{}
	require.NoError(t, f.metadata.db.Do(false, before))
	// Overlapping unchanged deltas advance only the persisted cursor.
	fx.mu.Lock()
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	require.NoError(t, f.syncMetadata(ctx))
	after := &snapshotBytes{}
	require.NoError(t, f.metadata.db.Do(false, after))
	assert.True(t, bytes.Equal(before.data, after.data), "unchanged metadata snapshot was rewritten")
	loaded := &metadataOp{}
	require.NoError(t, f.metadata.db.Do(false, loaded))
	assert.Equal(t, f.metadata.state.Anchor, loaded.state.Anchor)
	// A changed token invalidates the URL even when size/time are identical.
	fx.mu.Lock()
	fx.media[0].ETag = "version-two"
	fx.media[0].URL = expired.URL
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	o, err = f.NewObject(ctx, "renamed")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].URL = fx.server.URL + "/content/1"
	fx.mu.Unlock()
	read(o)
	assert.EqualValues(t, 2, expiredCalls.Load())
	// Local writes must be persisted before advancing a cursor without changes.
	local := api.Media{ID: "2", Name: "local", Size: 1, ETag: "local-version"}
	f.cacheMedia(local, false)
	fx.mu.Lock()
	fx.changes = map[string]api.Changes{}
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	require.NoError(t, f.syncMetadata(ctx))
	loaded = &metadataOp{}
	require.NoError(t, f.metadata.db.Do(false, loaded))
	assert.Equal(t, local, loaded.state.Media["2"])
	assert.Equal(t, f.metadata.state.Anchor, loaded.state.Anchor)
	fx.mu.Lock()
	fx.media[0].SoftDeleted = true
	fx.changes = map[string]api.Changes{"file": {Updated: []api.ID{"1"}}}
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	_, err = f.NewObject(ctx, "renamed")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestMetadataCacheDeletedRoot(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "root"}}
	fx.media = []api.Media{{ID: "2", FolderID: "1", Name: "file", Type: "file"}}
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"1"}}, "file": {New: []api.ID{"2"}}}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	m["root_folder_id"] = "1"
	remote, err := NewFs(ctx, "deleted-root-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	_, err = f.NewObject(ctx, "file")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.folders = nil
	fx.changes = map[string]api.Changes{"folder": {Deleted: []api.ID{"1"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	_, err = f.NewObject(ctx, "file")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestMetadataCacheShutdown(t *testing.T) {
	for _, localWrite := range []bool{false, true} {
		t.Run(strconv.FormatBool(localWrite), func(t *testing.T) {
			cacheDir := config.GetCacheDir()
			require.NoError(t, config.SetCacheDir(t.TempDir()))
			t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
			ctx := context.Background()
			fx := newFixture(t)
			fx.requestTime = 1700000000123
			fx.changes = map[string]api.Changes{}
			m := fx.config(t)
			m["metadata_cache"] = "true"
			remote, err := NewFs(ctx, "shutdown-test", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			before := &snapshotBytes{}
			require.NoError(t, f.metadata.db.Do(false, before))
			fx.mu.Lock()
			fx.requestTime += 1000
			fx.mu.Unlock()
			f.expireMetadata()
			require.NoError(t, f.syncMetadata(ctx))
			// Keep the database open while the first filesystem shuts down.
			peer, err := NewFs(ctx, "shutdown-test", "", m)
			require.NoError(t, err)
			peerFs := peer.(*Fs)
			t.Cleanup(func() { require.NoError(t, peerFs.Shutdown(ctx)) })
			if localWrite {
				require.NoError(t, f.Mkdir(ctx, "created"))
			}
			require.NoError(t, f.Shutdown(ctx))
			loaded := &metadataOp{}
			require.NoError(t, peerFs.metadata.db.Do(false, loaded))
			assert.Equal(t, fx.requestTime, loaded.state.Anchor)
			if localWrite {
				require.Len(t, loaded.state.Folders, 1)
				assert.Equal(t, "created", loaded.state.Folders[0].Name)
			} else {
				after := &snapshotBytes{}
				require.NoError(t, peerFs.metadata.db.Do(false, after))
				assert.Equal(t, before.data, after.data, "shutdown rewrote unchanged metadata")
			}
		})
	}
}

func TestDirMoveAcrossAccounts(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	fx.folders = []api.Folder{{ID: "1", Name: "source"}, {ID: "2", Name: "destination"}}
	src, err := NewFs(ctx, "source-account", "", fx.config(t))
	require.NoError(t, err)
	dst, err := NewFs(ctx, "destination-account", "", fx.config(t))
	require.NoError(t, err)
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/profile" {
			id := r.Header.Get(deviceHeader)
			jsonReply(t, w, map[string]any{"data": map[string]any{"user": map[string]any{"generic": map[string]string{"userid": id}}}})
			return
		}
		handler.ServeHTTP(w, r)
	})
	assert.ErrorIs(t, dst.Features().DirMove(ctx, src, "source", "destination/moved"), fs.ErrorCantDirMove)
	assert.Equal(t, "source", fx.folders[0].Name)
}

func TestDirMoveAcrossRoots(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	fx.folders = []api.Folder{{ID: "1", Name: "source"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "3", Name: "destination"}}
	m := fx.config(t)
	src, err := NewFs(ctx, "move", "", m)
	require.NoError(t, err)
	m["root_folder_id"] = "2"
	dst, err := NewFs(ctx, "move", "", m)
	require.NoError(t, err)
	before := len(fx.folders)
	require.ErrorContains(t, dst.Features().DirMove(ctx, src, "source", "new/deeper"), "into itself")
	assert.Len(t, fx.folders, before)
	// Independent roots in the same account may move a whole tree.
	m["root_folder_id"] = "3"
	dst, err = NewFs(ctx, "move", "", m)
	require.NoError(t, err)
	require.NoError(t, dst.Features().DirMove(ctx, src, "source", "moved"))
	_, err = dst.List(ctx, "moved/child")
	require.NoError(t, err)
}

func TestMetadataCacheFolderDeltas(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	fx.folders = []api.Folder{{ID: "1", Name: "old"}, {ID: "2", ParentID: "1", Name: "child"}, {ID: "3", Name: "deleted"}}
	fx.changes = map[string]api.Changes{}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "folder-deltas", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	_, err = f.List(ctx, "old/child")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.folders[0].Name = "renamed"
	fx.folders = fx.folders[:2]
	fx.changes = map[string]api.Changes{"folder": {Updated: []api.ID{"1"}, New: []api.ID{"4"}, Deleted: []api.ID{"3"}}}
	fx.requestTime += 1000
	fx.mu.Unlock()
	f.expireMetadata()
	_, err = f.List(ctx, "renamed/child")
	require.NoError(t, err)
	assert.Equal(t, [][]api.ID{{"1", "4"}}, fx.folderBatches)
	_, err = f.List(ctx, "old/child")
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
	_, err = f.List(ctx, "deleted")
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
	fx.mu.Lock()
	fx.changes = map[string]api.Changes{}
	fx.folders = append(fx.folders, api.Folder{ID: "4", Name: "late"})
	fx.failFolders = true
	fx.mu.Unlock()
	f.expireMetadata()
	anchor := f.metadata.state.Anchor
	_, err = f.List(ctx, "late")
	require.ErrorContains(t, err, "failed folders")
	assert.Equal(t, anchor, f.metadata.state.Anchor)
	fx.mu.Lock()
	fx.failFolders = false
	fx.mu.Unlock()
	_, err = f.List(ctx, "late")
	require.NoError(t, err)
	assert.Equal(t, [][]api.ID{{"1", "4"}, {"4"}}, fx.folderBatches)

	// A child can arrive before its parent is visible in the changes feed.
	fx.mu.Lock()
	fx.folders = append(fx.folders, api.Folder{ID: "5", ParentID: "6", Name: "child"})
	fx.changes = map[string]api.Changes{"folder": {New: []api.ID{"5"}}}
	fx.mu.Unlock()
	f.expireMetadata()
	require.NoError(t, f.syncMetadata(ctx))
	assert.Equal(t, []api.ID{"6"}, f.metadata.state.PendingFolders)
	fx.mu.Lock()
	fx.folders = append(fx.folders, api.Folder{ID: "6", Name: "parent"})
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	f.expireMetadata()
	_, err = f.List(ctx, "parent/child")
	require.NoError(t, err)
	assert.Empty(t, f.metadata.state.PendingFolders)
}

func TestMetadataCachePendingAndBatching(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	ctx := context.Background()
	fx := newFixture(t)
	fx.requestTime = 1700000000123
	change := api.Changes{Locked: []api.ID{"999"}}
	fx.media = append(fx.media, api.Media{ID: "999", Name: "locked", Status: "L", Type: "file"})
	for i := 1; i <= pageSize+1; i++ {
		id := api.ID(strconv.Itoa(i))
		change.New = append(change.New, id)
		if i > 1 {
			fx.media = append(fx.media, api.Media{ID: id, Name: "file" + string(id), Size: 1, Type: "file"})
		}
	}
	fx.changes = map[string]api.Changes{"file": change}
	m := fx.config(t)
	m["metadata_cache"] = "true"
	remote, err := NewFs(ctx, "pending-test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
	assert.ElementsMatch(t, []int{pageSize, 2}, fx.mediaBatches)
	assert.ElementsMatch(t, []api.ID{"1", "999"}, f.metadata.state.Pending)
	assert.Equal(t, "19700101T000000Z", fx.changesFrom[0])
	anchor := f.metadata.state.Anchor
	fx.mu.Lock()
	fx.media = append(fx.media, api.Media{ID: "1", Name: "ready", Size: 5, Type: "file"})
	fx.requestTime += 60000
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	obj, err := f.NewObject(ctx, "ready")
	require.NoError(t, err)
	assert.EqualValues(t, 5, obj.Size())
	assert.Equal(t, []api.ID{"999"}, f.metadata.state.Pending)
	assert.Equal(t, time.UnixMilli(anchor).Add(-time.Second).UTC().Format(dateFormat), fx.changesFrom[1])
	assert.Equal(t, 2, fx.mediaBatches[2])
	fx.mu.Lock()
	fx.requestTime += 60000
	fx.changes = map[string]api.Changes{"file": {Deleted: []api.ID{"999"}}}
	fx.mu.Unlock()
	f.metadata.mu.Lock()
	f.metadata.checked = time.Time{}
	f.metadata.mu.Unlock()
	_, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, f.metadata.state.Pending)

	// Reconnecting the same remote to another account must start a new snapshot.
	fx.mu.Lock()
	fx.accountID = "another-account"
	fx.media = nil
	fx.changes = map[string]api.Changes{}
	fx.mu.Unlock()
	other, err := NewFs(ctx, "pending-test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.(*Fs).Shutdown(ctx)) })
	_, err = other.NewObject(ctx, "ready")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	assert.Equal(t, "19700101T000000Z", fx.changesFrom[3])
}

func TestMetadataCacheInvalidChanges(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	for _, tc := range []struct {
		name      string
		timestamp int64
		data      any
		want      string
	}{
		{"MissingTime", 0, map[string]any{}, "invalid requesttime"},
		{"UnknownSource", 1700000000123, map[string]any{"unexpected": map[string]any{}}, "unsupported source"},
		{"UnknownStatus", 1700000000123, map[string]any{"file": map[string]any{"X": []int{1}}}, "unsupported status"},
		{"InvalidID", 1700000000123, map[string]any{"file": map[string]any{"N": []string{"invalid"}}}, "invalid changes"},
		{"MissingData", 1700000000123, nil, "invalid changes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			fx.requestTime, fx.changeData = tc.timestamp, tc.data
			m := fx.config(t)
			m["metadata_cache"] = "true"
			_, err := NewFs(context.Background(), "invalid-test", "", m)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSessionRenewal(t *testing.T) {
	var logins atomic.Int32
	var expire atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		assert.Equal(t, "OneMediaHub", r.UserAgent())
		if r.URL.Path == "/tenant/api/system/information" {
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
			return
		}
		if r.URL.Path == "/tenant/api/login" {
			assert.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, r.ParseForm())
			assert.Equal(t, "alice", r.PostForm.Get("login"))
			assert.Equal(t, "password&=", r.PostForm.Get("password"))
			assert.Empty(t, r.URL.Query().Get("password"))
			assert.Empty(t, r.Header.Get("Cookie"))
			n := logins.Add(1)
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": fmt.Sprintf("session%d", n), "validationkey": fmt.Sprintf("key%d", n)}})
			return
		}
		if expire.Swap(false) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		assert.Equal(t, fmt.Sprintf("key%d", logins.Load()), r.URL.Query().Get("validationkey"))
		cookie, err := r.Cookie("JSESSIONID")
		assert.NoError(t, err)
		if cookie != nil {
			assert.Equal(t, fmt.Sprintf("session%d", logins.Load()), cookie.Value)
		}
		switch r.URL.Path {
		case "/tenant/api/media/folder":
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": []any{}}})
		case "/tenant/api/media":
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []any{}}, "more": false})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL + "/tenant", "api_path": "/api", "auth_type": authPassword, "user": "alice", "password": obscure.MustObscure("password&=")})
	ctx := context.Background()
	_, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	remote, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	require.EqualValues(t, 1, logins.Load())
	expire.Store(true)
	entries, err := remote.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.EqualValues(t, 2, logins.Load())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _, err := remote.List(ctx, ""); assert.NoError(t, err) })
	}
	wg.Wait()
	assert.EqualValues(t, 2, logins.Load())
}

func TestOAuthRefreshAndRestart(t *testing.T) {
	var exchanges, logins atomic.Int32
	var deviceID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
		case "/token":
			assert.Empty(t, r.Header.Get("X-deviceid"), "storage identity must not reach the OAuth provider")
			require.NoError(t, r.ParseForm())
			assert.Equal(t, "refresh_token", r.PostForm.Get("grant_type"))
			assert.Equal(t, "refresh-old", r.PostForm.Get("refresh_token"))
			id, secret, ok := r.BasicAuth()
			assert.True(t, ok)
			assert.Equal(t, "client", id)
			assert.Equal(t, "secret", secret)
			exchanges.Add(1)
			jsonReply(t, w, map[string]any{"access_token": "access-new", "refresh_token": "refresh-new", "expires_in": 3600, "token_type": "Bearer"})
		case "/sapi/login/oauth":
			assert.Equal(t, "true", r.URL.Query().Get("responsetime"))
			assert.NotEmpty(t, r.Header.Get("X-deviceid"))
			assert.True(t, strings.HasPrefix(r.Header.Get("X-deviceid"), "fol-"))
			if deviceID == "" {
				deviceID = r.Header.Get("X-deviceid")
			}
			assert.Equal(t, deviceID, r.Header.Get("X-deviceid"))
			assert.Equal(t, "application/x-www-form-urlencoded; charset=UTF-8", r.Header.Get("Content-Type"))
			require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), oauthPrefix))
			b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Authorization"), oauthPrefix))
			require.NoError(t, err)
			var envelope struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(b, &envelope))
			n := logins.Add(1)
			if n == 1 {
				assert.Equal(t, "access-new", envelope.Data["accesstoken"])
				assert.Equal(t, "refresh-new", envelope.Data["refreshtoken"])
			} else {
				assert.Equal(t, "access-rotated", envelope.Data["accesstoken"])
				assert.Equal(t, "refresh-rotated", envelope.Data["refreshtoken"])
				assert.Equal(t, "provider-value", envelope.Data["extension"])
			}
			assert.Equal(t, "windows", envelope.Data["platform"])
			rotated := `{"data":{"accesstoken":"access-rotated","refreshtoken":"refresh-rotated","expiresin":3600,"extension":"provider-value"}}`
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(rotated)))
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "token_url": srv.URL + "/token", "client_id": "client", "client_secret": obscure.MustObscure("secret")})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access-old", RefreshToken: "refresh-old", Expiry: time.Now().Add(-time.Hour)}, false))
	for range 2 {
		_, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, exchanges.Load())
	assert.EqualValues(t, 1, logins.Load())
	m[sessionKey] = ""
	_, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 2, logins.Load())
	assert.NotEmpty(t, m["device_id"])
	assert.Equal(t, deviceID, m["device_id"])
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "refresh-rotated", token.RefreshToken)
	assert.True(t, token.Valid())
}

func TestSessionScope(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/system/information"):
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		case strings.HasSuffix(r.URL.Path, "/login"):
			logins.Add(1)
			assert.Empty(t, r.Header.Get("Cookie"))
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	for range 2 {
		_, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, logins.Load())
	require.NotEmpty(t, m[sessionKey])
	for key, value := range map[string]string{
		"url": srv.URL + "/tenant", "api_path": "/other", "user": "other",
		"password": obscure.MustObscure("other"), "device_id": "other",
		sessionKey: "invalid JSON",
	} {
		t.Run(key, func(t *testing.T) {
			changed := maps.Clone(m)
			changed[key] = value
			before := logins.Load()
			_, err := NewFs(context.Background(), "test", "", changed)
			require.NoError(t, err)
			assert.Equal(t, before+1, logins.Load())
		})
	}
}

func TestConfigOAuth(t *testing.T) {
	var verifier string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "authorization_code", r.PostForm.Get("grant_type"))
		assert.Equal(t, "test-code", r.PostForm.Get("code"))
		assert.Equal(t, verifier, r.PostForm.Get("code_verifier"))
		jsonReply(t, w, map[string]any{"access_token": "access", "refresh_token": "refresh", "expires_in": 3600, "token_type": "Bearer"})
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_url": srv.URL + "/authorize", "token_url": srv.URL + "/token", "redirect_url": srv.URL + "/callback", "client_id": "client"})
	ctx := context.Background()
	out, err := configure(ctx, "test", m, fs.ConfigIn{State: "authorize"})
	require.NoError(t, err)
	loginURL, err := url.Parse(strings.Split(out.Option.Help, "\n\n")[1])
	require.NoError(t, err)
	verifier = m[authVerifierKey]
	assert.Equal(t, "S256", loginURL.Query().Get("code_challenge_method"))
	assert.Equal(t, "offline", loginURL.Query().Get("access_type"))
	assert.NotEmpty(t, loginURL.Query().Get("code_challenge"))
	callback := srv.URL + "/callback?code=test-code&state=" + url.QueryEscape(m[authStateKey])
	_, err = configure(ctx, "test", m, fs.ConfigIn{State: "exchange", Result: strings.Replace(callback, "state=", "state=wrong", 1)})
	require.ErrorContains(t, err, "state mismatch")
	_, err = configure(ctx, "test", m, fs.ConfigIn{State: "exchange", Result: callback})
	require.NoError(t, err)
	assert.Empty(t, m[authStateKey])
	assert.Empty(t, m[authVerifierKey])
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "refresh", token.RefreshToken)
}

func TestProviderOAuth(t *testing.T) {
	for _, tc := range []struct {
		name, server, authURL, tokenURL, scope, param, value string
	}{
		{"Spain", "https://cloud.o2online.es", "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/authorize", "https://apiseg.telefonica.es/openid/connect/auth/oauth/v2/o2/cus/token", "openid", "acr_values", "2"},
		{"Germany", "https://cloud.o2.de", "https://mondia-lcm.o2online.de/v2/web/auth/dialog/oauth", "https://police.mondiamedia.com/v2/api/auth/token", "", "client_type", "omh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := testConfig(t, configmap.Simple{"url": tc.server + "/", "client_id": "registered-client"})
			opt, err := readOptions(m)
			require.NoError(t, err)
			c, err := opt.oauthConfig()
			require.NoError(t, err)
			assert.Equal(t, tc.authURL, c.Endpoint.AuthURL)
			assert.Equal(t, tc.tokenURL, c.Endpoint.TokenURL)
			assert.Equal(t, tc.server+"/ui/html/clientoauth.html", c.RedirectURL)
			assert.Equal(t, tc.scope, strings.Join(c.Scopes, " "))
			out, err := configure(context.Background(), "test", m, fs.ConfigIn{State: "authorize"})
			require.NoError(t, err)
			u, err := url.Parse(strings.Split(out.Option.Help, "\n\n")[1])
			require.NoError(t, err)
			assert.Equal(t, tc.value, u.Query().Get(tc.param))
			assert.Equal(t, "registered-client", u.Query().Get("client_id"))
			assert.Equal(t, tc.scope, u.Query().Get("scope"))
			assert.Equal(t, "S256", u.Query().Get("code_challenge_method"))

			opt.AuthURL, opt.TokenURL, opt.RedirectURL = "https://custom/auth", "https://custom/token", "https://custom/callback"
			opt.Scope = "custom offline"
			c, err = opt.oauthConfig()
			require.NoError(t, err)
			assert.Equal(t, opt.AuthURL, c.Endpoint.AuthURL)
			assert.Equal(t, opt.TokenURL, c.Endpoint.TokenURL)
			assert.Equal(t, opt.RedirectURL, c.RedirectURL)
			assert.Equal(t, []string{"custom", "offline"}, c.Scopes)
		})
	}
}

func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{RemoteName: "TestOneMediaHub:", NilObject: (*Object)(nil)})
}

type fixtureMode int

const (
	specMode fixtureMode = iota
	o2Mode
)

// fixture models SAPI 14.5, with separately enabled O2 compatibility behavior.
type fixture struct {
	mu            sync.Mutex
	mode          fixtureMode
	pendingReads  int
	folders       []api.Folder
	media         []api.Media
	content       map[api.ID]string
	nextID        int
	server        *httptest.Server
	changes       map[string]api.Changes
	requestTime   int64
	requests      map[string]int
	failMedia     bool
	changesFrom   []string
	mediaBatches  []int
	folderBatches [][]api.ID
	failFolders   bool
	accountID     string
	changeData    any
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	fx := &fixture{folders: []api.Folder{}, media: []api.Media{}, content: map[api.ID]string{}, nextID: 1}
	fx.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fx.serve(t, w, r) }))
	t.Cleanup(fx.server.Close)
	return fx
}

func (fx *fixture) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	if fx.requests == nil {
		fx.requests = map[string]int{}
	}
	fx.requests[r.URL.Path]++
	if fx.mode == o2Mode && r.URL.Query().Get("offset") == "0" {
		jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1021", "message": "Invalid offset"}})
		return
	}

	if r.URL.Path == "/sapi/system/information" {
		assert.Equal(t, http.MethodGet, r.Method)
		jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/content/") {
		assert.Empty(t, r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("Cookie"))
		assert.Empty(t, r.URL.Query().Get("validationkey"))
		id := api.ID(strings.TrimPrefix(r.URL.Path, "/content/"))
		http.ServeContent(w, r, "content", time.Time{}, strings.NewReader(fx.content[id]))
		return
	}
	if r.URL.Path == "/sapi/login" {
		assert.Equal(t, http.MethodPost, r.Method)
		jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		return
	}
	assert.Equal(t, "key", r.URL.Query().Get("validationkey"))
	cookie, err := r.Cookie("JSESSIONID")
	require.NoError(t, err)
	assert.Equal(t, "session", cookie.Value)
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	action := r.URL.Query().Get("action")
	switch {
	case r.URL.Path == "/sapi/profile":
		id := fx.accountID
		if id == "" {
			id = "test-account"
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"user": map[string]any{"generic": map[string]string{"userid": id}}}})
	case r.URL.Path == "/sapi/profile/changes":
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "folder,file,picture,video,audio", r.URL.Query().Get("type"))
		assert.Equal(t, "true", r.URL.Query().Get("responsetime"))
		assert.Equal(t, "true", r.URL.Query().Get("locked"))
		assert.Equal(t, "creationdate", r.URL.Query().Get("sortby"))
		assert.Equal(t, "descending", r.URL.Query().Get("sortorder"))
		_, err := time.Parse(dateFormat, r.URL.Query().Get("from"))
		assert.NoError(t, err)
		fx.changesFrom = append(fx.changesFrom, r.URL.Query().Get("from"))
		data := fx.changeData
		if data == nil {
			data = fx.changes
		}
		jsonReply(t, w, map[string]any{"data": data, "requesttime": strconv.FormatInt(fx.requestTime, 10)})
	case r.URL.Path == "/sapi/media/folder" && action == "get":
		if fx.failFolders {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "failed folders"}})
			return
		}
		if r.Method == http.MethodPost {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
			var data struct {
				IDs []api.ID `json:"ids"`
			}
			require.NoError(t, json.Unmarshal(envelope.Data, &data))
			fx.folderBatches = append(fx.folderBatches, data.IDs)
			var folders []api.Folder
			for _, folder := range fx.folders {
				if slices.Contains(data.IDs, folder.ID) {
					folders = append(folders, folder)
				}
			}
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": folders}})
			return
		}
		assert.Equal(t, http.MethodGet, r.Method)
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		end := min(offset+limit, len(fx.folders))
		jsonReply(t, w, map[string]any{"data": map[string]any{"folders": fx.folders[min(offset, end):end]}})
	case r.URL.Path == "/sapi/media/folder" && action == "save":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		// §5.21 requires a name and numeric parent ID, without an ID on creation.
		var data struct {
			ID       json.RawMessage `json:"id"`
			Name     string          `json:"name"`
			ParentID *int64          `json:"parentid"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.NotEmpty(t, data.Name)
		folder := api.Folder{Name: data.Name, Status: "U", Date: time.Now().UnixMilli()}
		if data.ParentID != nil {
			folder.ParentID = api.ID(strconv.FormatInt(*data.ParentID, 10))
		}
		if len(data.ID) != 0 {
			require.NoError(t, json.Unmarshal(data.ID, &folder.ID))
			for i, old := range fx.folders {
				if old.ID == folder.ID {
					fx.folders[i] = folder
					break
				}
			}
		} else {
			folder.ID = api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
			fx.folders = append(fx.folders, folder)
		}
		jsonReply(t, w, map[string]any{"id": folder.ID, "success": "Folder saved successfully"})
	case r.URL.Path == "/sapi/media/folder" && action == "delete":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Folders []int64 `json:"folders"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.Len(t, data.Folders, 1)
		for i, folder := range fx.folders {
			if string(folder.ID) == strconv.FormatInt(data.Folders[0], 10) {
				fx.folders = append(fx.folders[:i], fx.folders[i+1:]...)
				break
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get":
		if fx.failMedia {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "failed metadata"}})
			return
		}
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			IDs    []int64  `json:"ids"`
			Fields []string `json:"fields"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Subset(t, data.Fields, []string{"name", "size", "modificationdate", "url", "folderid"})
		fx.mediaBatches = append(fx.mediaBatches, len(data.IDs))
		items := fx.media
		more := false
		if data.IDs != nil {
			assert.NotContains(t, r.URL.Query(), "limit", "§3.5.48 forbids pagination with IDs")
			assert.NotContains(t, r.URL.Query(), "offset")
			items = []api.Media{}
			if fx.pendingReads > 0 {
				fx.pendingReads--
				jsonReply(t, w, map[string]any{"data": map[string]any{"media": []api.Media{}}})
				return
			}
			for _, item := range fx.media {
				for _, id := range data.IDs {
					if strconv.FormatInt(id, 10) == string(item.ID) {
						items = append(items, item)
					}
				}
			}
		} else {
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			end := min(offset+limit, len(items))
			more = end < len(items)
			items = items[min(offset, end):end]
		}
		payload := map[string]any{"media": items}
		result := map[string]any{"data": payload}
		if data.IDs == nil {
			if fx.mode == o2Mode {
				payload["more"] = more
			} else {
				result["more"] = more
			}
		}
		jsonReply(t, w, result)
	case (r.URL.Path == "/sapi/upload" || r.URL.Path == "/sapi/upload/file") && action == "save":
		assert.Equal(t, http.MethodPost, r.Method)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, "invalid upload", http.StatusBadRequest)
			return
		}
		defer func() { assert.NoError(t, r.MultipartForm.RemoveAll()) }()
		require.NoError(t, json.Unmarshal([]byte(r.FormValue("data")), &envelope))
		// §5.59 uses a string media ID, numeric folder ID/size and RFC 2445 dates.
		var data struct {
			ID          string `json:"id"`
			FolderID    *int64 `json:"folderid"`
			Name        string `json:"name"`
			Size        int64  `json:"size"`
			ContentType string `json:"contenttype"`
			Created     string `json:"creationdate"`
			Modified    string `json:"modificationdate"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		require.NotEmpty(t, data.Name)
		require.NotEmpty(t, data.ContentType)
		_, err := time.Parse("20060102T150405Z", data.Created)
		require.NoError(t, err)
		if fx.mode == o2Mode && strings.HasPrefix(data.Name, ".") {
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "Missing name"}})
			return
		}
		file, header, err := r.FormFile("file")
		require.NoError(t, err)
		content, err := io.ReadAll(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		if int64(len(content)) != data.Size {
			http.Error(w, "incorrect upload size", http.StatusBadRequest)
			return
		}
		assert.Equal(t, data.Name, header.Filename)
		id := api.ID(data.ID)
		if id == "" {
			assert.Equal(t, "/sapi/upload", r.URL.Path)
			id = api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
		} else {
			assert.Equal(t, "/sapi/upload/file", r.URL.Path)
		}
		modified, err := time.Parse("20060102T150405Z", data.Modified)
		require.NoError(t, err)
		name := data.Name
		if fx.mode == o2Mode {
			name = strings.TrimRight(name, ".")
		}
		item := api.Media{ID: id, Name: name, Size: data.Size, Modified: modified.UnixMilli(), URL: fx.server.URL + "/content/" + string(id), Type: "file", Status: "U"}
		if data.FolderID != nil {
			item.FolderID = api.ID(strconv.FormatInt(*data.FolderID, 10))
		}
		index := len(fx.media)
		for i, existing := range fx.media {
			if existing.ID == id {
				index = i
				break
			}
		}
		if index == len(fx.media) {
			fx.media = append(fx.media, item)
		} else {
			fx.media[index] = item
		}
		fx.content[id] = string(content)
		jsonReply(t, w, map[string]any{"id": id, "success": "Media uploaded successfully"})
	case r.URL.Path == "/sapi/media/file" && action == "delete":
		assert.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		var data struct {
			Files []int64 `json:"files"`
		}
		require.NoError(t, json.Unmarshal(envelope.Data, &data))
		assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
		for _, id := range data.Files {
			for i, item := range fx.media {
				if string(item.ID) == strconv.FormatInt(id, 10) {
					fx.media = append(fx.media[:i], fx.media[i+1:]...)
					delete(fx.content, item.ID)
					break
				}
			}
		}
	case r.URL.Path == "/sapi/media" && action == "get-storage-space":
		assert.Equal(t, http.MethodGet, r.Method)
		jsonReply(t, w, map[string]any{"data": map[string]int64{"quota": 10000, "free": 9000}})
	default:
		t.Errorf("unexpected %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (fx *fixture) config(t *testing.T) configmap.Simple {
	return testConfig(t, configmap.Simple{"url": fx.server.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
}

func TestFileOperations(t *testing.T) {
	fx := newFixture(t)
	ctx := context.Background()
	m := fx.config(t)
	remote, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	require.NoError(t, f.Mkdir(ctx, "parent/empty"))
	modified := time.Date(2023, 4, 5, 6, 7, 8, 0, time.UTC)
	src := object.NewStaticObjectInfo("parent/file.txt", modified, 5, true, nil, f)
	obj, err := f.Put(ctx, strings.NewReader("hello"), src)
	require.NoError(t, err)
	assert.True(t, modified.Equal(obj.ModTime(ctx)))
	assert.EqualValues(t, 5, obj.Size())
	entries, err := f.List(ctx, "parent")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.ErrorIs(t, f.Rmdir(ctx, "parent"), fs.ErrorDirectoryNotEmpty)
	body, err := obj.Open(ctx, &fs.RangeOption{Start: 1, End: 3})
	require.NoError(t, err)
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	assert.Equal(t, "ell", string(content))
	oldID := obj.(*Object).ID()
	src = object.NewStaticObjectInfo("parent/file.txt", modified, 3, true, nil, f)
	obj, err = f.Put(ctx, strings.NewReader("new"), src)
	require.NoError(t, err)
	assert.Equal(t, oldID, obj.(*Object).ID())
	fileRoot, err := NewFs(ctx, "test", "parent/file.txt", m)
	require.ErrorIs(t, err, fs.ErrorIsFile)
	assert.Equal(t, "parent", fileRoot.Root())
	missing, err := NewFs(ctx, "test", "new/deep/root", m)
	require.NoError(t, err)
	require.NoError(t, missing.Mkdir(ctx, ""))
	usage, err := f.About(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1000, *usage.Used)
	require.NoError(t, obj.Remove(ctx))
	_, err = f.NewObject(ctx, "parent/file.txt")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	require.NoError(t, f.Rmdir(ctx, "parent/empty"))
	require.NoError(t, f.Rmdir(ctx, "parent"))
}

func TestUploadVisibility(t *testing.T) {
	fx := newFixture(t)
	fx.pendingReads = 1
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
	obj, err := f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	assert.EqualValues(t, 1, obj.Size())
}

func TestAsyncUpload(t *testing.T) {
	for _, size := range []int{0, 5} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			fx := newFixture(t)
			content := strings.Repeat("x", size)
			modified := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			var stages []string
			var metadata api.Upload
			var polls int
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.URL.Query().Get("action")
				if r.URL.Path == "/sapi/upload/file" {
					assert.Equal(t, http.MethodPost, r.Method)
					assert.Equal(t, "key", r.URL.Query().Get("validationkey"))
					stages = append(stages, action)
					if action == "save-metadata" {
						assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
						var request struct {
							Data api.Upload `json:"data"`
						}
						require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
						metadata = request.Data
						assert.Equal(t, "file.txt", metadata.Name)
						assert.EqualValues(t, size, metadata.Size)
						assert.Equal(t, modified.Format(dateFormat), metadata.Modified)
						jsonReply(t, w, map[string]any{"id": "42", "status": "U"})
						return
					}
					assert.Equal(t, "save", action)
					assert.Equal(t, "true", r.URL.Query().Get("acceptasynchronous"))
					assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
					assert.Equal(t, strconv.Itoa(size), r.Header.Get("X-funambol-file-size"))
					assert.EqualValues(t, size, r.ContentLength)
					assert.Equal(t, metadata.ContentType, r.Header.Get("Content-Type"))
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, content, string(body))
					fx.mu.Lock()
					fx.media = []api.Media{{ID: "42", Name: "file.txt", Size: int64(size), Modified: modified.UnixMilli(), Type: "file", Status: "U"}}
					fx.content["42"] = content
					fx.mu.Unlock()
					w.WriteHeader(http.StatusAccepted)
					jsonReply(t, w, map[string]any{"id": 42, "status": "A"})
					return
				}
				if action == "get-validation-status" {
					assert.Equal(t, "/sapi/media", r.URL.Path)
					assert.Equal(t, http.MethodPost, r.Method)
					var request struct {
						Data struct {
							IDs []struct {
								ID       string `json:"id"`
								FolderID string `json:"folder_id"`
							} `json:"ids"`
						} `json:"data"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					require.Len(t, request.Data.IDs, 1)
					assert.Equal(t, "42", request.Data.IDs[0].ID)
					polls++
					status := "V"
					if polls == 1 {
						status = "U"
					}
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]any{{"id": "42", "status": status}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			ctx := context.Background()
			f, err := NewFs(ctx, "test", "", m)
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("file.txt", modified, int64(size), true, nil, f)
			obj, err := f.Put(ctx, io.NopCloser(strings.NewReader(content)), src)
			require.NoError(t, err)
			assert.EqualValues(t, size, obj.Size())
			assert.True(t, modified.Equal(obj.ModTime(ctx)))
			assert.Equal(t, []string{"save-metadata", "save"}, stages)
			assert.Equal(t, 2, polls)
			stages = nil
			polls = 0
			require.NoError(t, obj.Update(ctx, strings.NewReader(content), src))
			assert.Equal(t, "42", metadata.ID)
			assert.Equal(t, []string{"save-metadata", "save"}, stages)
		})
	}
}

func TestAsyncUploadTimeoutOption(t *testing.T) {
	for _, timeout := range []string{"0s", "-1s"} {
		m := testConfig(t, configmap.Simple{"async_upload": "true", "upload_timeout": timeout})
		_, err := readOptions(m)
		require.ErrorContains(t, err, "upload_timeout must be positive")
	}
}

func TestAsyncUploadResume(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		accepted     int
	}{
		{"partial", "", 3}, {"complete", "", 6},
		{"none", "", 0}, {"none with unit", "bytes=", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var metadataCalls, rawCalls, probes atomic.Int32
			var saved string
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.URL.Query().Get("action")
				if r.URL.Path == "/sapi/upload/file" {
					if action == "save-metadata" {
						metadataCalls.Add(1)
						jsonReply(t, w, map[string]string{"id": "42"})
						return
					}
					assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
					if r.Header.Get("Content-Range") == "bytes */6" {
						probes.Add(1)
						assert.EqualValues(t, 0, r.ContentLength)
						w.Header().Set("Range", tc.prefix+"0-"+strconv.Itoa(len(saved)-1))
						w.WriteHeader(308)
						_, _ = io.WriteString(w, "<html>Resume</html>")
						return
					}
					call := rawCalls.Add(1)
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					if call == 1 {
						assert.Equal(t, "abcdef", string(body))
						saved = string(body[:tc.accepted])
						w.WriteHeader(503)
						return
					}
					assert.Equal(t, fmt.Sprintf("bytes %d-5/6", tc.accepted), r.Header.Get("Content-Range"))
					assert.EqualValues(t, 6-tc.accepted, r.ContentLength)
					assert.Equal(t, "abcdef"[tc.accepted:], string(body))
					saved += string(body)
					w.WriteHeader(202)
					return
				}
				if action == "get-validation-status" {
					fx.mu.Lock()
					fx.media = []api.Media{{ID: "42", Name: "file", Size: 6, URL: fx.server.URL + "/content/42"}}
					fx.content["42"] = saved
					fx.mu.Unlock()
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			ctx := context.Background()
			remote, err := NewFs(ctx, "resume", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			src := object.NewStaticObjectInfo("file", time.Now(), 6, true, nil, f)
			o, err := f.Put(ctx, strings.NewReader("abcdef"), src)
			require.NoError(t, err)
			assert.Equal(t, "abcdef", saved)
			assert.Equal(t, "42", o.(*Object).ID())
			assert.EqualValues(t, 1, metadataCalls.Load())
			assert.EqualValues(t, 1, probes.Load())
			if tc.accepted == 6 {
				assert.EqualValues(t, 1, rawCalls.Load())
			} else {
				assert.EqualValues(t, 2, rawCalls.Load())
			}
		})
	}
}

func TestAsyncUploadResumeRejectsUnsafeOffsets(t *testing.T) {
	for _, tc := range []struct {
		name, rangeValue, want string
		seekable               bool
	}{
		{"missing", "", "invalid Range", true}, {"nonzero start", "1-2", "invalid Range", true}, {"beyond size", "0-9", "invalid Range", true},
		{"negative end", "0--2", "invalid Range", true}, {"negative nonzero start", "1--1", "invalid Range", true}, {"noncanonical negative end", "0--01", "invalid Range", true},
		{"multiple ranges", "0-2,4-5", "invalid Range", true}, {"non-seekable", "0-2", "non-seekable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var rawCalls atomic.Int32
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sapi/upload/file" {
					handler.ServeHTTP(w, r)
					return
				}
				if r.URL.Query().Get("action") == "save-metadata" {
					jsonReply(t, w, map[string]string{"id": "42"})
					return
				}
				if r.Header.Get("Content-Range") == "bytes */6" {
					w.Header().Set("Range", tc.rangeValue)
					w.WriteHeader(308)
					return
				}
				rawCalls.Add(1)
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				w.WriteHeader(503)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			ctx := context.Background()
			remote, err := NewFs(ctx, "unsafe-resume", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			var reader io.Reader = strings.NewReader("abcdef")
			if !tc.seekable {
				reader = io.NopCloser(reader)
			}
			src := object.NewStaticObjectInfo("file", time.Now(), 6, true, nil, f)
			_, err = f.Put(ctx, reader, src)
			require.ErrorContains(t, err, tc.want)
			assert.EqualValues(t, 1, rawCalls.Load())
		})
	}
}

func TestUploadOffsetEmptySentinel(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		size     int64
		accepted bool
	}{
		{"resume incomplete", http.StatusPermanentRedirect, 6, true},
		{"HTTP success", http.StatusOK, 6, false},
		{"empty file", http.StatusPermanentRedirect, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sapi/upload/file" {
					handler.ServeHTTP(w, r)
					return
				}
				assert.Equal(t, "bytes */"+strconv.FormatInt(tc.size, 10), r.Header.Get("Content-Range"))
				assert.Equal(t, "42", r.Header.Get("X-funambol-id"))
				assert.Zero(t, r.ContentLength)
				w.Header().Set("Range", "0--1")
				w.WriteHeader(tc.status)
			})
			ctx := context.Background()
			remote, err := NewFs(ctx, "empty-offset", "", fx.config(t))
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			o := &Object{fs: f, info: api.Media{ID: "42"}}
			offset, err := o.uploadOffset(ctx, tc.size)
			if tc.accepted {
				require.NoError(t, err)
				assert.Zero(t, offset)
			} else {
				require.ErrorContains(t, err, "invalid Range")
				assert.Contains(t, err.Error(), fmt.Sprintf("HTTP %d", tc.status))
				assert.Contains(t, err.Error(), `Range="0--1"`)
			}
		})
	}
}

func TestAsyncUploadResumeSource(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(strconv.FormatBool(changed), func(t *testing.T) {
			ctx := context.Background()
			sourceDir := t.TempDir()
			sourcePath := filepath.Join(sourceDir, "source.txt")
			require.NoError(t, os.WriteFile(sourcePath, []byte("abcdef"), 0600))
			modified := time.Unix(1700000000, 100000000)
			require.NoError(t, os.Chtimes(sourcePath, modified, modified))
			sourceFs, err := local.NewFs(ctx, "source", sourceDir, configmap.Simple{})
			require.NoError(t, err)
			src, err := sourceFs.NewObject(ctx, "source.txt")
			require.NoError(t, err)
			fx := newFixture(t)
			var rawCalls atomic.Int32
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				action := r.URL.Query().Get("action")
				if r.URL.Path == "/sapi/upload/file" {
					if action == "save-metadata" {
						jsonReply(t, w, map[string]string{"id": "42"})
						return
					}
					if r.Header.Get("Content-Range") == "bytes */6" {
						w.Header().Set("Range", "bytes=0-2")
						w.WriteHeader(308)
						return
					}
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					if rawCalls.Add(1) == 1 {
						if changed {
							require.NoError(t, os.WriteFile(sourcePath, []byte("ABCDEF"), 0600))
							require.NoError(t, os.Chtimes(sourcePath, modified.Add(100*time.Millisecond), modified.Add(100*time.Millisecond)))
						}
						w.WriteHeader(503)
						return
					}
					assert.Equal(t, "def", string(body))
					w.WriteHeader(202)
					return
				}
				if action == "get-validation-status" {
					fx.mu.Lock()
					fx.media = []api.Media{{ID: "42", Name: "file", Size: 6}}
					fx.mu.Unlock()
					jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "42", "status": "V"}}}})
					return
				}
				handler.ServeHTTP(w, r)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			remote, err := NewFs(ctx, "source-resume", "", m)
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			o := &Object{fs: f, remote: "file"}
			err = o.Update(ctx, io.NopCloser(strings.NewReader("abcdef")), src)
			if changed {
				require.ErrorContains(t, err, "source changed")
				assert.EqualValues(t, 1, rawCalls.Load())
			} else {
				require.NoError(t, err)
				assert.EqualValues(t, 2, rawCalls.Load())
			}
		})
	}
}

func TestAsyncUploadsParallel(t *testing.T) {
	fx := newFixture(t)
	var arrived atomic.Int32
	var validationCalls atomic.Int32
	ready := make(chan struct{})
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/sapi/upload/file", r.URL.Path)
		switch r.URL.Query().Get("action") {
		case "save-metadata":
			var request struct {
				Data api.Upload `json:"data"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			fx.mu.Lock()
			id := api.ID(strconv.Itoa(fx.nextID))
			fx.nextID++
			fx.media = append(fx.media, api.Media{ID: id, Name: request.Data.Name, Size: request.Data.Size, Type: "file"})
			fx.mu.Unlock()
			jsonReply(t, w, map[string]any{"id": id})
		case "save":
			if arrived.Add(1) == 2 {
				close(ready)
			}
			select {
			case <-ready:
			case <-r.Context().Done():
				return
			}
			content, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			fx.mu.Lock()
			fx.content[api.ID(r.Header.Get("X-funambol-id"))] = string(content)
			fx.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected upload action %s", r.URL.Query().Get("action"))
		}
	}))
	defer upload.Close()
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
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
		validationCalls.Add(1)
		require.Len(t, request.Data.IDs, 2)
		var ids []map[string]string
		for _, item := range request.Data.IDs {
			ids = append(ids, map[string]string{"id": item.ID, "status": "V"})
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"ids": ids}})
	})
	m := fx.config(t)
	m["async_upload"], m["upload_url"] = "true", upload.URL
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.(*Fs).Shutdown(context.Background())) })
	errors := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		go func() {
			src := object.NewStaticObjectInfo(name+".txt", time.Now(), int64(len(name)), true, nil, f)
			_, err := f.Put(ctx, strings.NewReader(name), src)
			errors <- err
		}()
	}
	for range 2 {
		require.NoError(t, <-errors)
	}
	assert.EqualValues(t, 2, arrived.Load())
	assert.EqualValues(t, 1, validationCalls.Load())
	fx.mu.Lock()
	defer fx.mu.Unlock()
	for _, item := range fx.media {
		assert.Equal(t, strings.TrimSuffix(item.Name, ".txt"), fx.content[item.ID])
	}
}

func TestAsyncUploadErrors(t *testing.T) {
	for _, tc := range []struct {
		name, metadata, content, status, want string
	}{
		{"metadata failure", `{"error":{"code":"COM-1011","message":"invalid metadata"}}`, "", "", "register upload metadata: COM-1011"},
		{"missing ID", `{}`, "", "", "no media ID"},
		{"wrong metadata ID", `{"id":43}`, "", "", "expected 42"},
		{"content failure", `{"id":42}`, `{"error":{"code":"MED-1000","message":"upload failed"}}`, "", "upload media 42: MED-1000"},
		{"wrong content ID", `{"id":42}`, `{"id":43}`, "", "expected 42"},
		{"processing failure", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"F"}]}}`, `returned status "F"`},
		{"invalid status response", `{"id":42}`, `{}`, `{"data":[]}`, "decode upload processing status"},
		{"empty status", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":""}]}}`, `returned status ""`},
		{"duplicate status", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"V"},{"id":42,"status":"F"}]}}`, "duplicate upload processing status"},
		{"unexpected status ID", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":43,"status":"V"}]}}`, "unexpected upload processing ID"},
		{"accepted remains pending", `{"id":42}`, `{}`, `{"data":{"ids":[{"id":42,"status":"A"}]}}`, "context deadline exceeded"},
		{"missing status remains pending", `{"id":42}`, `{}`, `{"data":{"ids":[]}}`, "context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var metadataCalls, contentCalls, statusCalls int
			handler := fx.server.Config.Handler
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body string
				switch r.URL.Query().Get("action") {
				case "save-metadata":
					metadataCalls++
					body = tc.metadata
				case "save":
					contentCalls++
					body = tc.content
				case "get-validation-status":
					statusCalls++
					body = tc.status
				default:
					handler.ServeHTTP(w, r)
					return
				}
				_, err := io.WriteString(w, body)
				assert.NoError(t, err)
			})
			m := fx.config(t)
			m["async_upload"] = "true"
			m["upload_timeout"] = "50ms"
			ctx := context.Background()
			f, err := NewFs(ctx, "test", "", m)
			require.NoError(t, err)
			src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
			if tc.name == "wrong metadata ID" {
				obj := &Object{fs: f.(*Fs), remote: "file", info: api.Media{ID: "42"}}
				err = obj.Update(ctx, strings.NewReader("x"), src)
				assert.Equal(t, "42", obj.ID())
			} else {
				_, err = f.Put(ctx, strings.NewReader("x"), src)
			}
			require.ErrorContains(t, err, tc.want)
			assert.Equal(t, 1, metadataCalls)
			if tc.content != "" {
				assert.Equal(t, 1, contentCalls, "raw uploads must not be replayed")
			} else {
				assert.Zero(t, contentCalls)
			}
			if tc.status != "" {
				assert.Equal(t, 1, statusCalls)
			} else {
				assert.Zero(t, statusCalls)
			}
		})
	}
}

func TestValidationCancellation(t *testing.T) {
	fx := newFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
			return
		}
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		jsonReply(t, w, map[string]any{"data": map[string]any{"ids": []map[string]string{{"id": "1", "status": "V"}, {"id": "2", "status": "V"}}}})
	})
	m := fx.config(t)
	m["async_upload"] = "true"
	remote, err := NewFs(context.Background(), "cancellation", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	t.Cleanup(func() { require.NoError(t, f.Shutdown(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(ctx, "") }()
	go func() { second <- (&Object{fs: f, info: api.Media{ID: "2"}}).waitUpload(context.Background(), "") }()
	<-started
	cancel()
	select {
	case err := <-first:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled upload blocked on another upload")
	}
	close(release)
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("one cancellation stopped the shared request")
	}
}

func TestValidationShutdown(t *testing.T) {
	fx := newFixture(t)
	started := make(chan struct{})
	handler := fx.server.Config.Handler
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "get-validation-status" {
			handler.ServeHTTP(w, r)
			return
		}
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		close(started)
		<-r.Context().Done()
	})
	m := fx.config(t)
	m["async_upload"] = "true"
	remote, err := NewFs(context.Background(), "shutdown", "", m)
	require.NoError(t, err)
	f := remote.(*Fs)
	done := make(chan error, 1)
	go func() { done <- (&Object{fs: f, info: api.Media{ID: "1"}}).waitUpload(context.Background(), "") }()
	<-started
	require.NoError(t, f.Shutdown(context.Background()))
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("shutdown left an upload waiting")
	}
}

func TestO2RootLayout(t *testing.T) {
	fx := newFixture(t)
	fx.folders = []api.Folder{
		{ID: "1", Name: "/"},
		{ID: "2", ParentID: "1", Name: "Backups"},
		{ID: "4", Name: "Other"},
	}
	fx.media = []api.Media{{ID: "3", FolderID: "1", Name: "root.txt"}}
	ctx := context.Background()
	m := fx.config(t)
	nested, err := NewFs(ctx, "test", "／", m)
	require.NoError(t, err)
	entries, err := nested.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"／", "Other"}, names)
	entries, err = f.List(ctx, "／")
	require.NoError(t, err)
	names = nil
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"／/Backups", "／/root.txt"}, names)
	_, err = f.NewObject(ctx, "／/root.txt")
	require.NoError(t, err)
	require.NoError(t, f.Mkdir(ctx, "／/new"))
	assert.Equal(t, api.ID("1"), fx.folders[len(fx.folders)-1].ParentID)

	m["root_folder_id"] = "2"
	f, err = NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestPagination(t *testing.T) {
	for name, mode := range map[string]fixtureMode{"Spec": specMode, "O2": o2Mode} {
		t.Run(name, func(t *testing.T) { testPagination(t, mode) })
	}
}

func testPagination(t *testing.T, mode fixtureMode) {
	fx := newFixture(t)
	fx.mode = mode
	for i := range pageSize + 1 {
		id := api.ID(strconv.Itoa(i + 1))
		fx.folders = append(fx.folders, api.Folder{ID: id, Name: fmt.Sprintf("folder%d", i)})
		fx.media = append(fx.media, api.Media{ID: id, Name: fmt.Sprintf("file%d", i), Type: "file", Status: "U"})
	}
	if mode == o2Mode {
		// O2 repeats the root folder at the end of its last page.
		fx.folders = append(fx.folders, fx.folders[0])
	}
	f, err := NewFs(context.Background(), "test", "", fx.config(t))
	require.NoError(t, err)
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, entries, 2*(pageSize+1))
	_, err = f.NewObject(context.Background(), fmt.Sprintf("file%d", pageSize))
	require.NoError(t, err)
}

func TestO2Filenames(t *testing.T) {
	fx := newFixture(t)
	fx.mode = o2Mode
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	for _, name := range []string{".hidden", "trailing."} {
		t.Run(name, func(t *testing.T) {
			src := object.NewStaticObjectInfo(name, time.Now(), 1, true, nil, f)
			_, err = f.Put(ctx, strings.NewReader("x"), src)
			require.NoError(t, err)
			_, err = f.NewObject(ctx, name)
			require.NoError(t, err)
		})
	}
}

// SAPI 14.5 §§3.5.25, 3.5.48 and 5.21: literal wire examples, independent of api types.
func TestSpecListing(t *testing.T) {
	fx := newFixture(t)
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/media/folder":
			jsonReply(t, w, json.RawMessage(`{"data":{"folders":[
				{"id":2500,"name":"onemediahub","magic":true,"status":"U","date":1231323524524},
				{"id":"2501","name":"pictures","parentid":2500,"status":"U","date":1231323524524},
				{"id":2502,"name":"Other","status":"U","date":1231323524524}
			]}}`))
		case "/sapi/media":
			assert.Equal(t, http.MethodPost, r.Method)
			pages.Add(1)
			if r.URL.Query().Get("offset") == "" {
				jsonReply(t, w, json.RawMessage(`{"data":{"media":[
					{"id":"0","name":"unfiled.txt","mediatype":"file","status":"U","date":1399985134672,"modificationdate":1161931347000,"size":5}
				]},"more":true}`))
				return
			}
			assert.Equal(t, "1", r.URL.Query().Get("offset"))
			jsonReply(t, w, json.RawMessage(`{"data":{"media":[
				{"id":"7","name":"100_1924.jpg","folder":2501,"mediatype":"picture","status":"U","date":1399985134672,"modificationdate":1161931347000,"size":722942}
			]},"more":false}`))
		default:
			fx.serve(t, w, r)
		}
	}))
	defer srv.Close()
	m := fx.config(t)
	m["url"] = srv.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"onemediahub", "Other", "unfiled.txt"}, names)
	entries, err = f.List(ctx, "onemediahub/pictures")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "onemediahub/pictures/100_1924.jpg", entries[0].Remote())
	assert.EqualValues(t, 722942, entries[0].Size())
	assert.Equal(t, time.UnixMilli(1161931347000), entries[0].ModTime(ctx))
	assert.EqualValues(t, 4, pages.Load())
}

// SAPI 14.5 §§3.5.3, 3.5.9, 3.5.14 and 3.5.18 use type-specific numeric ID arrays.
func TestSpecDelete(t *testing.T) {
	for _, mediaType := range []string{"picture", "video", "audio", "file"} {
		t.Run(mediaType, func(t *testing.T) {
			fx := newFixture(t)
			var deletes atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") != "delete" {
					fx.serve(t, w, r)
					return
				}
				deletes.Add(1)
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/sapi/media/"+mediaType, r.URL.Path)
				assert.Equal(t, "true", r.URL.Query().Get("softdelete"))
				b, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, `{"data":{"`+mediaType+`s":[23508]}}`, string(b))
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			m := fx.config(t)
			m["url"] = srv.URL
			f, err := NewFs(context.Background(), "test", "", m)
			require.NoError(t, err)
			obj := &Object{fs: f.(*Fs), info: api.Media{ID: "23508", Type: mediaType}}
			require.NoError(t, obj.Remove(context.Background()))
			assert.EqualValues(t, 1, deletes.Load())
		})
	}
}

func TestProtocolIntegration(t *testing.T) {
	fx := newFixture(t)
	t.Setenv("RCLONE_ONEMEDIAHUB_URL", fx.server.URL)
	t.Setenv("RCLONE_ONEMEDIAHUB_AUTH_TYPE", authPassword)
	t.Setenv("RCLONE_ONEMEDIAHUB_USER", "test")
	t.Setenv("RCLONE_ONEMEDIAHUB_PASSWORD", obscure.MustObscure("test"))
	fstests.Run(t, &fstests.Opt{
		RemoteName: ":onemediahub:", NilObject: (*Object)(nil), QuickTestOK: true,
	})
}

// O2 clients send OAuth credentials on SAPI requests to receive refreshed tokens.
func TestOAuthRequestHeaders(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/system/information" {
			jsonReply(t, w, map[string]string{"sapiversion": "31.0"})
			return
		}
		header := r.Header.Get("Authorization")
		assert.Equal(t, "configured-device", r.Header.Get("X-deviceid"))
		assert.True(t, strings.HasPrefix(header, oauthPrefix), "desktop OAuth is required on each SAPI request")
		switch r.URL.Path {
		case "/sapi/login/oauth":
			logins.Add(1)
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/media/folder":
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"rotated","refreshtoken":"rotated-refresh","expiresin":3600}}`)))
			jsonReply(t, w, map[string]any{"data": map[string]any{"folders": []any{}}})
		case "/sapi/media":
			jsonReply(t, w, map[string]any{"data": map[string]any{"media": []any{}}})
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "device_id": "configured-device"})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}, false))
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.NoError(t, err)
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "rotated-refresh", token.RefreshToken)
	_, err = NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 1, logins.Load())
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "other", RefreshToken: "other-refresh"}, false))
	_, err = NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.EqualValues(t, 2, logins.Load())
}

// SAPI 14.5 §§4.1.3 and 6.3.2: validation keys depend on server configuration.
func TestSessionWithoutKey(t *testing.T) {
	var logins atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, json.RawMessage(`{"sapiversion":"14.5"}`))
		case "/sapi/login":
			logins.Add(1)
			jsonReply(t, w, json.RawMessage(`{"data":{"jsessionid":"session","roles":[]}}`))
		case "/sapi/media":
			assert.NotContains(t, r.URL.Query(), "validationkey")
			cookie, err := r.Cookie("JSESSIONID")
			if assert.NoError(t, err) {
				assert.Equal(t, "session", cookie.Value)
			}
			jsonReply(t, w, json.RawMessage(`{"data":{"quota":1024000,"free":534123}}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	for range 2 {
		f, err := NewFs(context.Background(), "test", "", m)
		require.NoError(t, err)
		_, err = f.(*Fs).About(context.Background())
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, logins.Load())
}

// The Zefiro APK uses the native fac- device prefix; fol- logins are rejected.
func TestPasswordDeviceID(t *testing.T) {
	fx := newFixture(t)
	var logins atomic.Int32
	var expired atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/login":
			if !strings.HasPrefix(r.Header.Get("X-deviceid"), "fac-") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			logins.Add(1)
		case "/sapi/media/folder":
			if expired.Swap(false) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	m := fx.config(t)
	m["url"] = srv.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	device := m["device_id"]
	require.NotEmpty(t, device)
	expired.Store(true)
	_, err = f.List(ctx, "")
	require.NoError(t, err)
	_, err = NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	assert.Equal(t, device, m["device_id"])
	assert.EqualValues(t, 2, logins.Load())
}

func TestEmptyDownload(t *testing.T) {
	fx := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/empty" {
			// Zefiro accepts zero-byte uploads but returns 403 from their content URL.
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "empty", Size: 0, Type: "file", URL: srv.URL + "/empty"}}
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", fx.config(t))
	require.NoError(t, err)
	obj, err := f.NewObject(ctx, "empty")
	require.NoError(t, err)
	body, err := obj.Open(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, body.Close()) }()
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Empty(t, content)
}

func TestUploadDiscovery(t *testing.T) {
	fx := newFixture(t)
	uploaded := false
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploaded = true
		assert.Equal(t, "/sapi/upload", r.URL.Path)
		fx.serve(t, w, r)
	}))
	defer upload.Close()
	main := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/system/information" {
			assert.Empty(t, r.Header.Get("Cookie"))
			assert.Empty(t, r.Header.Get("Authorization"))
			jsonReply(t, w, map[string]string{"sapi.upload.endpoint": upload.URL})
			return
		}
		assert.False(t, strings.HasPrefix(r.URL.Path, "/sapi/upload"))
		fx.serve(t, w, r)
	}))
	defer main.Close()
	m := fx.config(t)
	m["url"] = main.URL
	ctx := context.Background()
	f, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	src := object.NewStaticObjectInfo("file", time.Now(), 1, true, nil, f)
	_, err = f.Put(ctx, strings.NewReader("x"), src)
	require.NoError(t, err)
	assert.True(t, uploaded)
}

const cloudFrontBlockedBody = `<HTML><HEAD><TITLE>ERROR: The request could not be satisfied</TITLE></HEAD><BODY><H1>403 ERROR</H1>Request blocked.</BODY></HTML>`

func cloudFrontBlockedReply(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.Header().Set("Server", "CloudFront")
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("X-Cache", "Error from cloudfront")
	w.WriteHeader(http.StatusForbidden)
	_, err := io.WriteString(w, cloudFrontBlockedBody)
	assert.NoError(t, err)
}

func TestServerInfoRecovery(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		blocked      bool
		persistent   bool
		explicitURL  bool
		wantError    bool
		wantRequests int32
	}{
		{name: "CloudFront block", blocked: true, wantRequests: 2},
		{name: "server unavailable", status: http.StatusServiceUnavailable, wantRequests: 2},
		{name: "permission denied", status: http.StatusForbidden, wantError: true, wantRequests: 1},
		{name: "bounded block retries", blocked: true, persistent: true, wantError: true, wantRequests: 2},
		{name: "explicit upload server", blocked: true, persistent: true, explicitURL: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var requests atomic.Int32
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sapi/system/information" && (requests.Add(1) == 1 || tc.persistent) {
					if tc.blocked {
						cloudFrontBlockedReply(t, w)
					} else {
						http.Error(w, "denied", tc.status)
					}
					return
				}
				fx.serve(t, w, r)
			})
			ctx, ci := fs.AddConfig(context.Background())
			ci.LowLevelRetries = 2
			m := fx.config(t)
			if tc.explicitURL {
				m["upload_url"] = fx.server.URL
			}
			remote, err := NewFs(ctx, "test", "", m)
			if tc.wantError {
				require.ErrorContains(t, err, "read OneMediaHub server information")
			} else {
				require.NoError(t, err)
				require.NoError(t, remote.(*Fs).Shutdown(ctx))
			}
			assert.Equal(t, tc.wantRequests, requests.Load())
		})
	}
}

func TestCloudFrontRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name, server, contentType, body string
		status                          int
		wantRetry                       bool
	}{
		{name: "blocked", status: 403, server: "CloudFront", contentType: "text/html", body: cloudFrontBlockedBody, wantRetry: true},
		{name: "charset", status: 403, server: "CloudFront", contentType: "text/html; charset=UTF-8", body: cloudFrontBlockedBody, wantRetry: true},
		{name: "other server", status: 403, contentType: "text/html", body: cloudFrontBlockedBody},
		{name: "other content type", status: 403, server: "CloudFront", contentType: "text/plain", body: cloudFrontBlockedBody},
		{name: "other denial", status: 403, server: "CloudFront", contentType: "text/html", body: "<HTML>Access denied: secret</HTML>"},
		{name: "other CloudFront error", status: 403, server: "CloudFront", contentType: "text/html", body: strings.ReplaceAll(cloudFrontBlockedBody, "Request blocked.", "Bad request.")},
		{name: "API permission", status: 403, server: "CloudFront", contentType: "application/json", body: `{"error":{"code":"PRO-1115","message":"InvalidPassword"}}`},
		{name: "other status", status: 404, server: "CloudFront", contentType: "text/html", body: cloudFrontBlockedBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: tc.status, Status: fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status)), Header: http.Header{}, Body: io.NopCloser(strings.NewReader(tc.body))}
			resp.Header.Set("Server", tc.server)
			resp.Header.Set("Content-Type", tc.contentType)
			err := errorHandler(resp)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret")
			wantRetry, err := retry(context.Background(), resp, err)
			assert.Equal(t, tc.wantRetry, wantRetry)
		})
	}
}

func TestCloudFrontReadAndDelete(t *testing.T) {
	for _, action := range []string{"get", "delete"} {
		t.Run(action, func(t *testing.T) {
			fx := newFixture(t)
			ctx, ci := fs.AddConfig(context.Background())
			ci.LowLevelRetries = 2
			remote, err := NewFs(ctx, "test", "", fx.config(t))
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			var requests atomic.Int32
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					cloudFrontBlockedReply(t, w)
					return
				}
				jsonReply(t, w, map[string]any{"data": map[string]any{}})
			})
			_, err = f.request(ctx, http.MethodPost, "/media/file", action, nil, map[string]any{"files": []api.ID{"1"}})
			if action == "get" {
				require.NoError(t, err)
				assert.EqualValues(t, 2, requests.Load())
			} else {
				require.Error(t, err)
				assert.False(t, fserrors.IsRetryError(err))
				assert.EqualValues(t, 1, requests.Load())
			}
		})
	}
}

func TestCloudFrontCooldownCancellation(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests atomic.Int32
	started := make(chan struct{})
	fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := requests.Add(1) == 1
		cloudFrontBlockedReply(t, w)
		if first {
			close(started)
		}
	})
	done := make(chan error, 1)
	m := fx.config(t)
	go func() {
		_, err := NewFs(ctx, "test", "", m)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery request did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("cooldown ended before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assert.EqualValues(t, 1, requests.Load())
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cooldown did not stop after cancellation")
	}
}

func TestCloudFrontLoginRecovery(t *testing.T) {
	for _, blocked := range []bool{true, false} {
		t.Run(strconv.FormatBool(blocked), func(t *testing.T) {
			fx := newFixture(t)
			var requests atomic.Int32
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sapi/login" && requests.Add(1) == 1 {
					if blocked {
						cloudFrontBlockedReply(t, w)
					} else {
						w.WriteHeader(http.StatusForbidden)
						jsonReply(t, w, map[string]any{"error": map[string]string{"code": "PRO-1115", "message": "InvalidPassword"}})
					}
					return
				}
				fx.serve(t, w, r)
			})
			ctx, ci := fs.AddConfig(context.Background())
			ci.LowLevelRetries = 2
			m := fx.config(t)
			m["upload_url"] = fx.server.URL
			remote, err := NewFs(ctx, "test", "", m)
			if blocked {
				require.NoError(t, err)
				assert.EqualValues(t, 2, requests.Load())
				require.NoError(t, remote.(*Fs).Shutdown(ctx))
			} else {
				require.ErrorContains(t, err, "PRO-1115")
				assert.EqualValues(t, 1, requests.Load())
			}
		})
	}
}

func TestAPIErrors(t *testing.T) {
	var reads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sapi/system/information":
			jsonReply(t, w, map[string]string{"sapiversion": "14.5"})
		case "/sapi/login":
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
		case "/sapi/media/folder":
			if reads.Add(1) == 1 {
				jsonReply(t, w, map[string]any{"error": map[string]string{"code": invalidKeyCode, "message": "Invalid mandatory validation key"}})
				return
			}
			jsonReply(t, w, map[string]any{"error": map[string]string{"code": "COM-1011", "message": "Invalid request"}})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	m := testConfig(t, configmap.Simple{"url": srv.URL, "auth_type": authPassword, "user": "test", "password": obscure.MustObscure("test")})
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.ErrorContains(t, err, "COM-1011")
	assert.EqualValues(t, 2, reads.Load())
}

func TestAPIResponseBodies(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantError  bool
	}{
		{"empty", "", false},
		{"whitespace", " \r\n\t", false},
		{"object", "{}", false},
		{"API failure", `{"error":{"code":"COM-1011","message":"Invalid request"}}`, true},
		{"truncated", `{"data":`, true},
		{"trailing data", `{} invalid`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			fx.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") == "delete" {
					_, err := io.WriteString(w, tc.body)
					assert.NoError(t, err)
					return
				}
				fx.serve(t, w, r)
			})
			ctx := context.Background()
			remote, err := NewFs(ctx, "response-test", "", fx.config(t))
			require.NoError(t, err)
			f := remote.(*Fs)
			t.Cleanup(func() { require.NoError(t, f.Shutdown(ctx)) })
			_, err = f.request(ctx, http.MethodPost, "/media/file", "delete", nil, map[string]any{"files": []api.ID{"1"}})
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHTTPErrorContext(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://cloud.example.com/sapi/upload?action=save&validationkey=secret", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		request *http.Request
		body    string
		want    string
	}{
		{name: "Request", request: req, body: "<html>secret</html>", want: `OneMediaHub HTTP error: 403 Forbidden (POST cloud.example.com/sapi/upload action="save")`},
		{name: "NoRequest", body: "<html>secret</html>", want: "OneMediaHub HTTP error: 403 Forbidden"},
		{name: "APIError", request: req, body: `{"error":{"code":"COM-1011","message":"Invalid request"}}`, want: "COM-1011: Invalid request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Request: tc.request, Body: io.NopCloser(strings.NewReader(tc.body))}
			err := errorHandler(resp)
			assert.EqualError(t, err, tc.want)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestTokenResponse(t *testing.T) {
	m := configmap.Simple{}
	a := &auth{name: "test", m: m, token: &oauth2.Token{AccessToken: "old", RefreshToken: "keep"}}
	header := oauthPrefix + base64.RawStdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"new","expiresin":"3600","valid":"true"}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, "keep", a.token.RefreshToken)
	before := m["token"]
	for _, bad := range []string{"oauth invalid!", "Bearer access", oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{}}`))} {
		assert.Error(t, a.saveHeaderLocked(bad))
		assert.Equal(t, before, m["token"])
	}
	resp := &http.Response{Header: http.Header{"Authorization": {oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"stale","refreshtoken":"stale"}}`))}}}
	require.NoError(t, a.saveHeader(resp, "old"))
	assert.Equal(t, before, m["token"])
}

// Both O2 APKs preserve tokens when SAPI sends an empty replacement.
func TestTokenEmptyAccess(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	a := &auth{name: "test", m: configmap.Simple{}, token: &oauth2.Token{AccessToken: "keep", RefreshToken: "old", Expiry: expiry}}
	header := oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"","refreshtoken":"rotated","expiresin":"3600"}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, "keep", a.token.AccessToken)
	assert.Equal(t, "rotated", a.token.RefreshToken)
	assert.Equal(t, expiry, a.token.Expiry)

	a.token = nil
	require.ErrorContains(t, a.saveHeaderLocked(header), "no access token")
}

func TestMissingRootFolder(t *testing.T) {
	fx := newFixture(t)
	m := fx.config(t)
	m["root_folder_id"] = "99"
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	_, err = f.List(context.Background(), "")
	require.ErrorIs(t, err, fs.ErrorDirNotFound)
}

func TestDownloadRedirect(t *testing.T) {
	fx := newFixture(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-deviceid"))
		assert.Empty(t, r.Header.Get("Cookie"), "download redirects must not forward the SAPI session")
		assert.Empty(t, r.Header.Get("Referer"), "download redirects must not expose the validation key")
		assert.Empty(t, r.Header.Get("Authorization"))
		_, err := io.WriteString(w, "x")
		assert.NoError(t, err)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/download/file" {
			http.Redirect(w, r, target.URL, http.StatusFound)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: srv.URL + "/sapi/download/file"}}
	m := fx.config(t)
	m["url"] = srv.URL
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	body, err := obj.Open(context.Background())
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestTokenExpiryDoesNotSlide(t *testing.T) {
	expiry := time.Now().Add(time.Minute)
	a := &auth{name: "test", m: configmap.Simple{}, token: &oauth2.Token{AccessToken: "same", RefreshToken: "refresh", Expiry: expiry}}
	header := oauthPrefix + base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"same","expiresin":3600}}`))
	require.NoError(t, a.saveHeaderLocked(header))
	assert.Equal(t, expiry, a.token.Expiry, "echoed credentials must not extend the token's lifetime")
}

func TestConcurrentOpen(t *testing.T) {
	fx := newFixture(t)
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: fx.server.URL + "/content/1"}}
	fx.content["1"] = "x"
	f, err := NewFs(context.Background(), "test", "", fx.config(t))
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			_ = obj.Size()
		}
	}()
	defer func() { stop.Store(true); <-done }()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			body, err := obj.Open(context.Background())
			if !assert.NoError(t, err) {
				return
			}
			_, err = io.Copy(io.Discard, body)
			assert.NoError(t, err)
			assert.NoError(t, body.Close())
		})
	}
	wg.Wait()
	fx.mu.Lock()
	assert.Equal(t, 1, fx.requests["/sapi/media"], "range openings reuse the listed download URL")
	fx.mu.Unlock()
}

func TestDownloadURLExpiry(t *testing.T) {
	for _, apiForbidden := range []bool{false, true} {
		t.Run(strconv.FormatBool(apiForbidden), func(t *testing.T) {
			fx := newFixture(t)
			var oldCalls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/expired" || r.URL.Path == "/sapi/forbidden" {
					oldCalls.Add(1)
					w.WriteHeader(http.StatusForbidden)
					return
				}
				fx.serve(t, w, r)
			}))
			defer srv.Close()
			oldPath := "/expired"
			if apiForbidden {
				oldPath = "/sapi/forbidden"
			}
			fx.media = []api.Media{{ID: "1", Name: "file", Size: 6, URL: srv.URL + oldPath}}
			fx.content["1"] = "abcdef"
			m := fx.config(t)
			m["url"] = srv.URL
			remote, err := NewFs(context.Background(), "expiry", "", m)
			require.NoError(t, err)
			obj, err := remote.NewObject(context.Background(), "file")
			require.NoError(t, err)
			fx.mu.Lock()
			fx.media[0].URL = srv.URL + "/content/1"
			fx.mu.Unlock()
			for range 2 {
				body, err := obj.Open(context.Background(), &fs.RangeOption{Start: 1, End: 3})
				if apiForbidden {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				b, err := io.ReadAll(body)
				require.NoError(t, err)
				require.NoError(t, body.Close())
				assert.Equal(t, "bcd", string(b))
			}
			fx.mu.Lock()
			defer fx.mu.Unlock()
			if apiForbidden {
				assert.Equal(t, 1, fx.requests["/sapi/media"])
				assert.EqualValues(t, 2, oldCalls.Load())
			} else {
				assert.Equal(t, 2, fx.requests["/sapi/media"])
				assert.EqualValues(t, 1, oldCalls.Load())
			}
		})
	}
}

func TestDownloadExpiryContentChange(t *testing.T) {
	fx := newFixture(t)
	expired := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer expired.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, URL: expired.URL}}
	remote, err := NewFs(context.Background(), "content-change", "", fx.config(t))
	require.NoError(t, err)
	obj, err := remote.NewObject(context.Background(), "file")
	require.NoError(t, err)
	fx.mu.Lock()
	fx.media[0].Size = 2
	fx.media[0].URL = fx.server.URL + "/content/1"
	fx.mu.Unlock()
	_, err = obj.Open(context.Background())
	require.ErrorContains(t, err, "media changed")
}

func TestDownloadRenewal(t *testing.T) {
	fx := newFixture(t)
	var downloads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sapi/login/oauth" {
			jsonReply(t, w, map[string]any{"data": map[string]string{"jsessionid": "session", "validationkey": "key"}})
			return
		}
		if r.URL.Path == "/sapi/download/file" {
			if downloads.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Authorization", oauthPrefix+base64.StdEncoding.EncodeToString([]byte(`{"data":{"accesstoken":"download-token","refreshtoken":"download-refresh"}}`)))
			_, err := io.WriteString(w, "x")
			assert.NoError(t, err)
			return
		}
		fx.serve(t, w, r)
	}))
	defer srv.Close()
	fx.media = []api.Media{{ID: "1", Name: "file", Size: 1, Type: "file", URL: srv.URL + "/sapi/download/file"}}
	m := testConfig(t, configmap.Simple{"url": srv.URL})
	require.NoError(t, oauthutil.PutToken("test", m, &oauth2.Token{AccessToken: "access", RefreshToken: "refresh"}, false))
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	obj, err := f.NewObject(context.Background(), "file")
	require.NoError(t, err)
	body, err := obj.Open(context.Background())
	require.NoError(t, err)
	require.NoError(t, body.Close())
	token, err := oauthutil.GetToken("test", m)
	require.NoError(t, err)
	assert.Equal(t, "download-refresh", token.RefreshToken)
}

// SAPI 14.5 §4.1.1 returns JSON errors with HTTP 200, including on download APIs.
func TestDownloadJSON(t *testing.T) {
	const expired = `{"error":{"code":"SEC-1003","message":"Invalid mandatory validation key"}}`
	for _, tc := range []struct {
		name        string
		content     string
		disposition string
		err         string
	}{
		{name: "Renew", content: `{"value":"file content"}`},
		{name: "Error", content: `{"error":{"code":"COM-1005","message":"Unsupported operation"}}`, err: "COM-1005"},
		{name: "Attachment", content: expired, disposition: `attachment; filename="error.json"`},
		{name: "LargeJSON", content: `{"value":"` + strings.Repeat("x", 128*1024) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			var downloads atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sapi/download/file" {
					fx.serve(t, w, r)
					return
				}
				assert.Empty(t, r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				if downloads.Add(1) == 1 {
					_, err := io.WriteString(w, expired)
					assert.NoError(t, err)
					return
				}
				w.Header().Set("Content-Disposition", tc.disposition)
				_, err := io.WriteString(w, tc.content)
				assert.NoError(t, err)
			}))
			defer srv.Close()
			m := fx.config(t)
			m["url"] = srv.URL
			f, err := NewFs(context.Background(), "test", "", m)
			require.NoError(t, err)
			u, err := url.Parse(srv.URL + "/sapi/download/file")
			require.NoError(t, err)
			body, err := f.(*Fs).open(context.Background(), u, nil)
			if body != nil {
				defer func() { assert.NoError(t, body.Close()) }()
			}
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				content, err := io.ReadAll(body)
				require.NoError(t, err)
				assert.Equal(t, tc.content, string(content))
			}
			assert.EqualValues(t, 2, downloads.Load())
		})
	}
}
