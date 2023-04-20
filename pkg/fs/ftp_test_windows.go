package fs

import (
	"syscall"
)

var interruptableSysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP} //nolint:gochecknoglobals // OS-specific constant
