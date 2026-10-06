package batcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	Result string
	Item   string
)

// blockingStringer pauses Commit in its debug log after the shutdown check.
type blockingStringer struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingStringer) String() string {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return "batcher test"
}

func TestBatcherNew(t *testing.T) {
	ctx := context.Background()
	ci := fs.GetConfig(ctx)

	opt := Options{
		Mode:                  "async",
		Size:                  100,
		Timeout:               1 * time.Second,
		MaxBatchSize:          1000,
		DefaultTimeoutSync:    500 * time.Millisecond,
		DefaultTimeoutAsync:   10 * time.Second,
		DefaultBatchSizeAsync: 100,
	}
	commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) (err error) {
		return nil
	}

	b, err := New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	require.True(t, b.Batching())
	b.Shutdown()

	opt.Mode = "sync"
	b, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	require.True(t, b.Batching())
	b.Shutdown()

	opt.Mode = "off"
	b, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	require.False(t, b.Batching())
	b.Shutdown()

	opt.Mode = "bad"
	_, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.ErrorContains(t, err, "batch mode")

	opt.Mode = "async"
	opt.Size = opt.MaxBatchSize + 1
	_, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.ErrorContains(t, err, "batch size")

	opt.Mode = "sync"
	opt.Size = 0
	opt.Timeout = 0
	b, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	assert.Equal(t, ci.Transfers, b.opt.Size)
	assert.Equal(t, opt.DefaultTimeoutSync, b.opt.Timeout)
	b.Shutdown()

	opt.Mode = "async"
	opt.Size = 0
	opt.Timeout = 0
	b, err = New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	assert.Equal(t, opt.DefaultBatchSizeAsync, b.opt.Size)
	assert.Equal(t, opt.DefaultTimeoutAsync, b.opt.Timeout)
	b.Shutdown()

	// Check we get an error on commit
	_, err = b.Commit(ctx, "last", Item("last"))
	require.ErrorContains(t, err, "shutting down")

}

func TestBatcherCommit(t *testing.T) {
	ctx := context.Background()

	opt := Options{
		Mode:                  "sync",
		Size:                  3,
		Timeout:               1 * time.Second,
		MaxBatchSize:          1000,
		DefaultTimeoutSync:    500 * time.Millisecond,
		DefaultTimeoutAsync:   10 * time.Second,
		DefaultBatchSizeAsync: 100,
	}
	var wg sync.WaitGroup
	errFail := errors.New("fail")
	var commits int
	var totalSize int
	commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) (err error) {
		commits += 1
		totalSize += len(items)
		for i := range items {
			if items[i] == "5" {
				errors[i] = errFail
			} else {
				results[i] = Result(items[i]) + " result"
			}
		}
		return nil
	}
	b, err := New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	defer b.Shutdown()

	for i := range 10 {
		wg.Add(1)
		s := fmt.Sprintf("%d", i)
		go func() {
			defer wg.Done()
			result, err := b.Commit(ctx, s, Item(s))
			if s == "5" {
				assert.True(t, errors.Is(err, errFail))
			} else {
				require.NoError(t, err)
				assert.Equal(t, Result(s+" result"), result)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 4, commits)
	assert.Equal(t, 10, totalSize)
}

func TestBatcherCommitFail(t *testing.T) {
	ctx := context.Background()

	opt := Options{
		Mode:                  "sync",
		Size:                  3,
		Timeout:               1 * time.Second,
		MaxBatchSize:          1000,
		DefaultTimeoutSync:    500 * time.Millisecond,
		DefaultTimeoutAsync:   10 * time.Second,
		DefaultBatchSizeAsync: 100,
	}
	var wg sync.WaitGroup
	errFail := errors.New("fail")
	var commits int
	var totalSize int
	commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) (err error) {
		commits += 1
		totalSize += len(items)
		return errFail
	}
	b, err := New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	defer b.Shutdown()

	for i := range 10 {
		wg.Add(1)
		s := fmt.Sprintf("%d", i)
		go func() {
			defer wg.Done()
			_, err := b.Commit(ctx, s, Item(s))
			assert.True(t, errors.Is(err, errFail))
		}()
	}
	wg.Wait()
	assert.Equal(t, 4, commits)
	assert.Equal(t, 10, totalSize)
}

func TestBatcherCommitShutdown(t *testing.T) {
	ctx := context.Background()

	opt := Options{
		Mode:                  "sync",
		Size:                  3,
		Timeout:               1 * time.Second,
		MaxBatchSize:          1000,
		DefaultTimeoutSync:    500 * time.Millisecond,
		DefaultTimeoutAsync:   10 * time.Second,
		DefaultBatchSizeAsync: 100,
	}
	var wg sync.WaitGroup
	var commits int
	var totalSize int
	commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) (err error) {
		commits += 1
		totalSize += len(items)
		for i := range items {
			results[i] = Result(items[i])
		}
		return nil
	}
	b, err := New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)

	for i := range 10 {
		wg.Add(1)
		s := fmt.Sprintf("%d", i)
		go func() {
			defer wg.Done()
			result, err := b.Commit(ctx, s, Item(s))
			assert.NoError(t, err)
			assert.Equal(t, Result(s), result)
		}()
	}

	time.Sleep(100 * time.Millisecond)
	b.Shutdown() // shutdown with batches outstanding

	wg.Wait()
	assert.Equal(t, 4, commits)
	assert.Equal(t, 10, totalSize)
}

func TestBatcherCommitRacingShutdown(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			ci := fs.GetConfig(ctx)
			oldLogLevel := ci.LogLevel
			ci.LogLevel = fs.LogLevelDebug
			defer func() { ci.LogLevel = oldLogLevel }()

			committed := make(chan struct{})
			commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) error {
				close(committed)
				for i := range items {
					results[i] = Result(items[i])
				}
				return nil
			}
			blocker := &blockingStringer{
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			b, err := New[Item, Result](ctx, blocker, commitBatch, Options{
				Mode:         mode,
				Size:         1,
				Timeout:      time.Hour,
				MaxBatchSize: 1000,
			})
			require.NoError(t, err)

			commitDone := make(chan error, 1)
			go func() {
				_, err := b.Commit(ctx, "item", Item("item"))
				commitDone <- err
			}()
			select {
			case <-blocker.started:
			case <-time.After(time.Second):
				t.Fatal("commit did not reach the admission point")
			}

			ci.LogLevel = oldLogLevel
			shutdownDone := make(chan struct{})
			go func() {
				b.Shutdown()
				close(shutdownDone)
			}()

			// Give Shutdown a chance to contend with the blocked admission.
			select {
			case <-b.closed:
			case <-time.After(100 * time.Millisecond):
			}
			close(blocker.release)

			select {
			case err := <-commitDone:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("commit hung while racing shutdown")
			}
			select {
			case <-shutdownDone:
			case <-time.After(time.Second):
				t.Fatal("shutdown hung while racing commit")
			}
			select {
			case <-committed:
			case <-time.After(time.Second):
				t.Fatal("accepted commit was dropped during shutdown")
			}
		})
	}
}

func TestBatcherCommitAsync(t *testing.T) {
	ctx := context.Background()

	opt := Options{
		Mode:                  "async",
		Size:                  3,
		Timeout:               1 * time.Second,
		MaxBatchSize:          1000,
		DefaultTimeoutSync:    500 * time.Millisecond,
		DefaultTimeoutAsync:   10 * time.Second,
		DefaultBatchSizeAsync: 100,
	}
	var wg sync.WaitGroup
	errFail := errors.New("fail")
	var commits atomic.Int32
	var totalSize atomic.Int32
	commitBatch := func(ctx context.Context, items []Item, results []Result, errors []error) (err error) {
		wg.Add(1)
		defer wg.Done()
		// t.Logf("commit %d", len(items))
		commits.Add(1)
		totalSize.Add(int32(len(items)))
		for i := range items {
			if items[i] == "5" {
				errors[i] = errFail
			} else {
				results[i] = Result(items[i]) + " result"
			}
		}
		return nil
	}
	b, err := New[Item, Result](ctx, nil, commitBatch, opt)
	require.NoError(t, err)
	defer b.Shutdown()

	for i := range 10 {
		wg.Add(1)
		s := fmt.Sprintf("%d", i)
		go func() {
			defer wg.Done()
			result, err := b.Commit(ctx, s, Item(s))
			// Async just returns straight away
			require.NoError(t, err)
			assert.Equal(t, Result(""), result)
		}()
	}
	time.Sleep(2 * time.Second) // wait for batch timeout - needed with async
	wg.Wait()

	assert.Equal(t, int32(4), commits.Load())
	assert.Equal(t, int32(10), totalSize.Load())
}

func TestBatcherCancelledAdmission(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		t.Run(mode, func(t *testing.T) {
			var commits atomic.Int32
			b, err := New[Item, Result](context.Background(), nil, func(_ context.Context, _ []Item, _ []Result, _ []error) error {
				commits.Add(1)
				return nil
			}, Options{Mode: mode, Size: 1, MaxBatchSize: 1, Timeout: time.Millisecond})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = b.Commit(ctx, "cancelled", Item("cancelled"))
			assert.ErrorIs(t, err, context.Canceled)
			b.Shutdown()
			assert.Zero(t, commits.Load())
		})
	}
}

func TestBatcherCancelledFullQueue(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var mu sync.Mutex
	var committed []Item
	b, err := New[Item, Result](context.Background(), nil, func(_ context.Context, items []Item, _ []Result, _ []error) error {
		if items[0] == "first" {
			close(started)
			<-release
		}
		mu.Lock()
		committed = append(committed, items...)
		mu.Unlock()
		return nil
	}, Options{Mode: "async", Size: 1, MaxBatchSize: 1, Timeout: time.Hour})
	require.NoError(t, err)
	t.Cleanup(b.Shutdown)
	_, err = b.Commit(context.Background(), "first", Item("first"))
	require.NoError(t, err)
	<-started
	_, err = b.Commit(context.Background(), "second", Item("second"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := b.Commit(ctx, "cancelled", Item("cancelled")); done <- err }()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("cancelled admission remained blocked by the full queue")
	}
	unblock()
	b.Shutdown()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []Item{"first", "second"}, committed)
}

func TestBatcherCancelledWaitingForAdmission(t *testing.T) {
	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	oldLevel := ci.LogLevel
	ci.LogLevel = fs.LogLevelDebug
	defer func() { ci.LogLevel = oldLevel }()
	blocker := &blockingStringer{started: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(blocker.release) })
	defer unblock()
	b, err := New[Item, Result](ctx, blocker, func(_ context.Context, _ []Item, _ []Result, _ []error) error { return nil },
		Options{Mode: "async", Size: 1, MaxBatchSize: 1, Timeout: time.Hour})
	require.NoError(t, err)
	t.Cleanup(b.Shutdown)
	firstDone := make(chan error, 1)
	go func() { _, err := b.Commit(ctx, "first", Item("first")); firstDone <- err }()
	<-blocker.started
	limited, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := b.Commit(limited, "cancelled", Item("cancelled")); done <- err }()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("cancelled caller remained blocked waiting for admission")
	}
	unblock()
	require.NoError(t, <-firstDone)
	b.Shutdown()
}
