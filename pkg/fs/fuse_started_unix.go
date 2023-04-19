//go:build !windows

package fs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"

	"github.com/datawire/dlib/dlog"
)

func (fh *FuseHost) detectFuseStarted(ctx context.Context, started chan error) {
	var st unix.Stat_t
	if err := unix.Stat(fh.mountPoint, &st); err != nil {
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
			var mountSt unix.Stat_t
			if err := unix.Stat(fh.mountPoint, &mountSt); err != nil {
				// we don't consider a failure to stat an error here, just a cause for a retry.
				dlog.Debugf(ctx, "unable to stat mount point %q: %v", fh.mountPoint, err)
			} else {
				if st.Ino != mountSt.Ino || st.Dev != mountSt.Dev {
					return
				}
			}
		}
	}
}
