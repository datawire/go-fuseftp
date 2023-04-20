//go:build !windows

package fs

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

var interruptableSysProcAttr *syscall.SysProcAttr = nil       //nolint:gochecknoglobals // OS-specific constant
var signalsToForward = []os.Signal{unix.SIGINT, unix.SIGTERM} //nolint:gochecknoglobals // OS-specific constant list
