package fs

import (
	"syscall"
)

const manyLargeFilesCount = 5 // Keep reasonably low or Windoze thinks it's under attach

var interruptableSysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP} //nolint:gochecknoglobals // OS-specific constant
