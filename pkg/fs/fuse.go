package fs

import (
	"context"
	"runtime"
	"sync"

	"github.com/winfsp/cgofuse/fuse"

	"github.com/datawire/dlib/dlog"
)

// FuseHost wraps a fuse.FileSystemHost and adds Start/Stop semantics
type FuseHost struct {
	host       *fuse.FileSystemHost
	mountPoint string
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewHost creates a FuseHost instance that will mount the given filesystem
// on the given mountPoint.
func NewHost(fsh fuse.FileSystemInterface, mountPoint string) *FuseHost {
	host := fuse.NewFileSystemHost(fsh)
	host.SetCapReaddirPlus(true)
	return &FuseHost{host: host, mountPoint: mountPoint}
}

// Start will mount the filesystem on the mountPoint passed to NewHost.
func (fh *FuseHost) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	fh.cancel = cancel

	opts := []string{
		"-o", "default_permissions",
		"-o", "auto_cache",
		"-o", "sync_read",
		"-o", "allow_root",
	}
	if runtime.GOOS == "windows" {
		// WinFsp requires this to create files with the same
		// user as the one that starts the FUSE mount
		opts = append(opts, "-o", "uid=-1", "-o", "gid=-1")
	}
	//	if dlog.MaxLogLevel(ctx) >= dlog.LogLevelDebug {
	//		opts = append(opts, "-o", "debug")
	//	}

	mCh := make(chan bool, 1)
	fh.wg.Add(1)
	go func() {
		defer fh.wg.Done()
		mCh <- fh.host.Mount(fh.mountPoint, opts)
	}()

	fh.wg.Add(1)
	go func() {
		defer fh.wg.Done()
		select {
		case <-ctx.Done():
			fh.host.Unmount()
		case mountResult := <-mCh:
			if !mountResult {
				dlog.Errorf(ctx, "fuse mount of %s failed", fh.mountPoint)
			}
		}
	}()
}

// Stop will unmount the file system and terminate the FTP client, wait for all clean-up to
// complete, and then return
func (fh *FuseHost) Stop() {
	// cancel will cause the host to unmount, which in turn will result in a call to
	// Destroy() which will terminate the FTP client.
	fh.cancel()
	fh.wg.Wait()
}
