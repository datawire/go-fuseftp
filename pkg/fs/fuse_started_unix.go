//go:build !windows

package fs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func (fh *FuseHost) detectFuseStarted(ctx context.Context, started chan error) {
	st, err := statWithTimeout(ctx, fh.mountPoint, 10*time.Millisecond)
	if err != nil {
		select {
		case started <- fmt.Errorf("unable to stat mount point %q: %v", fh.mountPoint, err):
		default:
		}
		close(started)
		return
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer func() {
		close(started)
		ticker.Stop()
	}()
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				started <- fmt.Errorf("timeout trying to stat mount point %q", fh.mountPoint)
			} else {
				started <- ctx.Err()
			}
			return
		case <-ticker.C:
			if mountSt, err := statWithTimeout(ctx, fh.mountPoint, 20*time.Millisecond); err != nil {
				// we don't consider a failure to stat an error here, just a cause for a retry.
				logrus.Debugf("unable to stat mount point %q: %v", fh.mountPoint, err)
			} else {
				if st.Ino != mountSt.Ino || st.Dev != mountSt.Dev {
					return
				}
			}
		}
	}
}

// statWithTimeout performs a normal unix.Stat but will not allow that it hangs for
// more than the given timeout.
func statWithTimeout(ctx context.Context, path string, timeout time.Duration) (*unix.Stat_t, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	errCh := make(chan error, 1)
	mountSt := new(unix.Stat_t)
	go func() {
		errCh <- unix.Stat(path, mountSt)
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
		return mountSt, nil
	}
}
