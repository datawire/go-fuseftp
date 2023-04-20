//go:build !windows

package fs

import (
	"syscall"
)

var interruptableSysProcAttr *syscall.SysProcAttr = nil //nolint:gochecknoglobals // OS-specific constant
