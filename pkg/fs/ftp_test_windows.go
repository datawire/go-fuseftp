package fs

import (
	"os"
	"syscall"
)

var interruptableSysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP} //nolint:gochecknoglobals // OS-specific constant
var signalsToForward = []os.Signal{os.Interrupt}                                                     //nolint:gochecknoglobals // OS-specific constant list
