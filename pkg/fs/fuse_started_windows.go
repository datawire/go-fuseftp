package fs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/windows"
)

func (fh *FuseHost) detectFuseStarted(ctx context.Context, started chan error) {
	devPath, err := windows.UTF16PtrFromString(`\\.\` + fh.mountPoint)
	if err != nil {
		panic(err)
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
				started <- fmt.Errorf("timeout trying to read mount point %q", fh.mountPoint)
			} else {
				started <- ctx.Err()
			}
			return
		case <-ticker.C:
			fh, err := windows.CreateFile(devPath,
				windows.GENERIC_READ,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
				nil,
				windows.OPEN_EXISTING,
				windows.FILE_ATTRIBUTE_NORMAL, 0)
			if err == nil {
				_ = windows.CloseHandle(fh)
				return
			}
		}
	}
}
