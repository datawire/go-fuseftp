//go:build !windows

package fs

import (
	"syscall"
)

const manyLargeFilesCount = 12

var interruptableSysProcAttr *syscall.SysProcAttr = nil //nolint:gochecknoglobals // OS-specific constant
