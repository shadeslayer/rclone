package cmd

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveExitCode(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
	}{
		{"success", exitcode.Success},
		{"late failure", exitcode.FatalError},
		{"existing failure", exitcode.FileNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			child := exec.Command(os.Args[0], "-test.run=^TestResolveExitCodeHelper$")
			child.Env = append(os.Environ(), "RCLONE_CMD_EXIT_TEST="+test.name)
			output, err := child.CombinedOutput()
			if test.code == 0 {
				require.NoError(t, err, "%s", output)
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", output)
			}
			assert.Equal(t, test.code, child.ProcessState.ExitCode(), "%s", output)
		})
	}
}

func TestResolveExitCodeHelper(t *testing.T) {
	variant := os.Getenv("RCLONE_CMD_EXIT_TEST")
	if variant == "" {
		return
	}
	ctx := context.Background()
	accounting.Start(ctx)
	var err error
	if variant != "success" {
		atexit.Register(func() { _ = fs.CountError(ctx, fserrors.FatalError(errors.New("background deletion failed"))) })
	}
	if variant == "existing failure" {
		err = fs.ErrorObjectNotFound
	}
	resolveExitCode(err)
}
