package dircache

import (
	"context"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 5 * time.Second

type testDirs struct {
	mu      sync.Mutex
	dirs    map[string]string
	creates map[string]int
	find    func(context.Context, string, string) error
	create  func(context.Context, string, string) error
	fold    func(string) string
}

func (d *testDirs) FindLeaf(ctx context.Context, parent, leaf string) (string, bool, error) {
	if d.find != nil {
		if err := d.find(ctx, parent, leaf); err != nil {
			return "", false, err
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	key := path.Join(parent, leaf)
	if d.fold != nil {
		key = d.fold(key)
	}
	id, found := d.dirs[key]
	return id, found, nil
}

func (d *testDirs) CreateDir(ctx context.Context, parent, leaf string) (string, error) {
	if d.create != nil {
		if err := d.create(ctx, parent, leaf); err != nil {
			return "", err
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	id := path.Join(parent, leaf)
	if d.fold != nil {
		id = d.fold(id)
	}
	d.dirs[id] = id
	d.creates[id]++
	return id, nil
}

func newTestDirs() *testDirs {
	return &testDirs{dirs: make(map[string]string), creates: make(map[string]int)}
}

func waitDir(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("directory operation did not finish")
	}
}

func TestIndependentDirs(t *testing.T) {
	for _, kind := range []string{"cached", "create"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			d := newTestDirs()
			dc := New("", "root", d)
			require.NoError(t, dc.FindRoot(ctx, false))
			if kind == "cached" {
				dc.Put("fast", "root/fast")
			}

			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			d.create = func(ctx context.Context, _, leaf string) error {
				if leaf != "slow" {
					return nil
				}

				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			slow, fast := make(chan error, 1), make(chan error, 1)
			go func() { _, err := dc.FindDir(ctx, "slow", true); slow <- err }()
			select {
			case <-started:
			case <-time.After(testTimeout):
				t.Fatal("directory creation did not start")
			}
			go func() { _, err := dc.FindDir(ctx, "fast", true); fast <- err }()
			select {
			case err := <-fast:
				require.NoError(t, err)
			case <-time.After(testTimeout):
				t.Error("unrelated directory waited for creation")
				unblock()
				waitDir(t, fast)
			}
			unblock()
			waitDir(t, slow)
		})
	}
}

func TestSharedDirs(t *testing.T) {
	const callers = 16
	d := newTestDirs()
	dc := New("base", "root", d)
	results := make(chan error, callers)
	for range callers {
		go func() {
			id, err := dc.FindDir(context.Background(), "parent/child", true)
			assert.Equal(t, "root/base/parent/child", id)
			results <- err
		}()
	}
	for range callers {
		waitDir(t, results)
	}
	require.Equal(t, map[string]int{"root/base": 1, "root/base/parent": 1, "root/base/parent/child": 1}, d.creates)
	parent, err := dc.RootParentID(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, "root", parent)
}

func TestLookupBeforeCreate(t *testing.T) {
	d := newTestDirs()
	dc := New("", "root", d)
	require.NoError(t, dc.FindRoot(context.Background(), false))
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	d.find = func(context.Context, string, string) error {
		once.Do(func() { close(started); <-release })
		return nil
	}
	lookup, create := make(chan error, 1), make(chan error, 1)
	go func() { _, err := dc.FindDir(context.Background(), "child", false); lookup <- err }()
	<-started
	go func() { _, err := dc.FindDir(context.Background(), "child", true); create <- err }()
	close(release)
	require.ErrorIs(t, <-lookup, fs.ErrorDirNotFound)
	waitDir(t, create)
	assert.Equal(t, 1, d.creates["root/child"])
}

func TestDirAliases(t *testing.T) {
	for _, names := range [][2]string{{"Child", "child"}, {"a/child", "b/child"}, {"child", "child"}} {
		t.Run(strings.Join(names[:], ","), func(t *testing.T) {
			d := newTestDirs()
			d.fold = strings.ToLower
			dc := New("", "root", d)
			require.NoError(t, dc.FindRoot(context.Background(), false))
			dc.Put("a", "root")
			dc.Put("b", "root")
			if names[0] == names[1] {
				d.find = func(_ context.Context, parent, _ string) error {
					if parent == "root" {
						dc.SetRootIDAlias("alias")
					}
					return nil
				}
				d.fold = func(s string) string { return strings.ReplaceAll(s, "alias/", "root/") }
			}
			started, release := make(chan struct{}, 2), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			d.create = func(context.Context, string, string) error {
				started <- struct{}{}
				<-release
				return nil
			}
			result := make(chan error, 2)
			go func() { _, err := dc.FindDir(context.Background(), names[0], true); result <- err }()
			select {
			case <-started:
			case <-time.After(testTimeout):
				t.Fatal("creation did not start")
			}
			go func() { _, err := dc.FindDir(context.Background(), names[1], true); result <- err }()
			select {
			case <-started:
				t.Error("aliases created the same directory concurrently")
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			waitDir(t, result)
			waitDir(t, result)
			assert.Equal(t, 1, d.creates["root/child"])
		})
	}
}

func TestDirWaitCancellation(t *testing.T) {
	d := newTestDirs()
	dc := New("", "root", d)
	require.NoError(t, dc.FindRoot(context.Background(), false))
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	d.create = func(context.Context, string, string) error {
		close(started)
		<-release
		return nil
	}
	result := make(chan error, 1)
	go func() { _, err := dc.FindDir(context.Background(), "child", true); result <- err }()
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("creation did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := dc.FindDir(ctx, "child", true)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	unblock()
	waitDir(t, result)
	assert.Equal(t, 1, d.creates["root/child"])
}

func TestConcurrentReset(t *testing.T) {
	const callers = 16
	d := newTestDirs()
	dc := New("base", "root", d)
	result := make(chan error, callers)
	for range callers {
		go func() {
			for range callers {
				dc.ResetRoot()
				id, err := dc.FindDir(context.Background(), "child", true)
				if err != nil {
					result <- err
					return
				}
				assert.Equal(t, "root/base/child", id)
			}
			result <- nil
		}()
	}
	for range callers {
		waitDir(t, result)
	}
	assert.Equal(t, map[string]int{"root/base": 1, "root/base/child": 1}, d.creates)
	assert.True(t, dc.FoundRoot())
	id, err := dc.RootID(context.Background(), false)
	require.NoError(t, err)
	assert.Equal(t, "root/base", id)
}
