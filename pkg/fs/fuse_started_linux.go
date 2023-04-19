package fs

import (
	"context"
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
			return
		case <-ticker.C:
			var mountSt unix.Stat_t
			if err := unix.Stat(fh.mountPoint, &mountSt); err != nil {
				dlog.Errorf(ctx, "unable to stat mount point %q: %v", fh.mountPoint, err)
			} else {
				if st.Ino != mountSt.Ino || st.Dev != mountSt.Dev {
					return
				}
			}
		}
	}
}
