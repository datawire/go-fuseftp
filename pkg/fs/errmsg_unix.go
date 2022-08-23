//go:build !windows

package fs

const (
	errDirNotFound  = "no such file or directory"
	errFileNotFound = "no such file or directory"
	errFileExists   = "file exists"
	errDirNotEmpty  = "directory not empty"
	errNotDirectory = "not a directory"
)
