package fs

import (
	"context"
	"time"

	"golang.org/x/sys/windows"
)

func (fh *FuseHost) detectFuseStarted(ctx context.Context, started chan struct{}) {
	devPath, err := windows.UTF16PtrFromString(`\\.\` + fh.mountPoint)
	if err != nil {
		return err
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
