// Package fs contains the FTP implementation of the fuse.FileSystemInterface, and the FuseHost that can mount it.
package fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"net/netip"
	"net/textproto"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
	log "github.com/sirupsen/logrus"
	"github.com/winfsp/cgofuse/fuse"
)

// fuseImpl implements the fuse.FileSystemInterface. The official documentation for the API
// can be found at https://libfuse.github.io/doxygen/structfuse__operations.html
type fuseImpl struct {
	*fuse.FileSystemBase

	// connPool is the pool of control connections to the remote FTP server.
	pool connPool

	// cancel the GC loop
	cancel context.CancelFunc

	// Mutex protects nextHandle, current, and shuttingDown
	sync.RWMutex

	// Next file handle. File handles are opaque to FUSE, and much faster to use than
	// the full path
	nextHandle uint64

	// current maps file handles to info structs
	current map[uint64]*info

	// shuttingDown prevent that new handles are added
	shuttingDown bool
}

// info holds information about file or directory that has been obtained from
// using an FTP MLST call (or sometimes populated locally with known information to
// save extra calls). In addition to the FTP information, the info is also
// responsible for maintaining a ftp.Response during Read, and a pipe (reader/writer pair)
// during Write.
type info struct {
	*fuseImpl

	// path is used to find already existing file handles. Their ftp.Entry can be
	// reused which means that no MLST call is required for subsequent accesses to
	// the same path.
	path string

	// fh is the file handle. Every open, create, or mkdir receives a unique identifier
	fh uint64

	// conn is the dedicated connection that is used during read/write operations with
	// this handle
	conn *ftp.ServerConn

	// rr and of is used when reading data from a remote file
	rr *ftp.Response

	// Current offset for read operations.
	rof uint64

	// Current offset for write operations.
	wof uint64

	// The Entry from the ftp server with estimated size based on initial size and write/truncate operations
	entry ftp.Entry

	// The writer is the writer side of an io.Pipe() used when writing data to a remote file.
	writer io.WriteCloser

	// 1 is added to this wg when the reader/writer pipe is created. Wait for it when closing the writer.
	wg sync.WaitGroup
}

// close this handle and free up any resources that it holds.
func (i *info) close() {
	if i.rr != nil {
		_ = i.rr.Close()
	}
	if i.writer != nil {
		_ = i.writer.Close()
	}
	i.wg.Wait()
}

const stalePeriod = time.Second // Fuse default cache time

type FTPClient interface {
	fuse.FileSystemInterface

	// SetAddress will quit open connections, change the address, and reconnect
	// The method is intended to be used when a FUSE mount must survive a change of
	// FTP server address.
	SetAddress(addr netip.AddrPort) error
}

// NewFTPClient returns an implementation of the fuse.FileSystemInterface that is backed by
// an FTP server connection tp the address. The dir parameter is the directory that the
// FTP server changes to when connecting.
func NewFTPClient(ctx context.Context, addr netip.AddrPort, dir string, readTimeout time.Duration) (FTPClient, error) {
	ctx, cancel := context.WithCancel(ctx)
	f := &fuseImpl{
		cancel:  cancel,
		current: make(map[uint64]*info),
		pool: connPool{
			dir:     dir,
			timeout: readTimeout,
		},
	}
	go func() {
		ticker := time.NewTicker(stalePeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f.pool.tidy()
			}
		}
	}()

	if err := f.pool.setAddr(addr); err != nil {
		cancel()
		return nil, err
	}
	return f, nil
}

func (f *fuseImpl) SetAddress(addr netip.AddrPort) error {
	return f.pool.setAddr(addr)
}

// Create will create a file of size zero unless the file already exists
// The third argument, the mode bits, are currently ignored
func (f *fuseImpl) Create(path string, flags int, _ uint32) (int, uint64) {
	log.Debugf("Create(%s, %#x)", path, flags)
	fe, _, errCode := f.openHandle(path, true, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	return 0, fe.fh
}

// Destroy will drain all ongoing writes, and for each active connection, send the QUIT message to the FTP server and disconnect
func (f *fuseImpl) Destroy() {
	log.Debug("Destroy")

	f.Lock()
	// Prevent new entries from being added
	f.shuttingDown = true
	pf := make([]*info, len(f.current))
	i := 0
	for _, fe := range f.current {
		pf[i] = fe
		i++
	}
	f.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(len(pf))
	for _, fe := range pf {
		go func(fe *info) {
			defer wg.Done()
			fe.close()
			f.Lock()
			delete(f.current, fe.fh)
			f.Unlock()
			_ = fe.conn.Quit()
		}(fe)
	}
	wg.Wait()
	f.pool.quit()
	f.cancel()
}

// Flush is a noop in this implementation
func (f *fuseImpl) Flush(path string, fh uint64) int {
	log.Debugf("Flush(%s, %d)", path, fh)
	return 0
}

// Getattr gets file attributes. The UID and GID will always be the
// UID and GID of the caller. File mode is always 0644 and Directory
// mode is always 0755.
func (f *fuseImpl) Getattr(path string, s *fuse.Stat_t, fh uint64) int {
	log.Debugf("Getattr(%s, %d)", path, fh)
	var e *ftp.Entry
	var errCode int
	if fh != math.MaxUint64 {
		e, errCode = f.loadEntry(fh)
	}
	if e == nil {
		e, errCode = f.getEntry(path)
	}
	if errCode == 0 {
		toStat(e, s)
	}
	return errCode
}

// Init starts the garbage collector that removes cached items when they
// haven't been used for a period of time.
func (f *fuseImpl) Init() {
	log.Debug("Init")
}

func (f *fuseImpl) Mkdir(path string, mode uint32) int {
	log.Debugf("Mkdir(%s, %O)", path, mode)
	err := f.withConn(func(conn *ftp.ServerConn) error {
		return conn.MakeDir(relpath(path))
	})
	return f.errToFuseErr(err)
}

// Open ensures checks if the file exists, and if it doesn't, ensure that
// a file of size zero can be created in the server.
func (f *fuseImpl) Open(path string, flags int) (int, uint64) {
	log.Debugf("Open(%s, %#x)", path, flags)
	fe, _, errCode := f.openHandle(path, flags&os.O_CREATE == os.O_CREATE, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	log.Debugf("Open(%s, %#x) -> %d", path, flags, fe.fh)
	return 0, fe.fh
}

// Opendir is like Open but will fail unless the path represents a directory
func (f *fuseImpl) Opendir(path string) (int, uint64) {
	log.Debugf("Opendir(%s)", path)
	fe, e, errCode := f.openHandle(path, false, false)
	if errCode < 0 {
		return errCode, 0
	}
	if e.Type != ftp.EntryTypeFolder {
		f.delete(fe.fh)
		return -fuse.ENOTDIR, 0
	}
	log.Debugf("Opendir(%s) -> %d", path, fe.fh)
	return 0, fe.fh
}

// Read a chunk of data using ftp RETR. The result returned from the server
// is cached and used in subsequent reads until EOF is reached or the file
// handle is released.
//
// Read requires that fuse is started with -o sync_read to ensure that the
// read calls arrive in sequence.
func (f *fuseImpl) Read(path string, buff []byte, ofst int64, fh uint64) int {
	log.Debugf("Read(%s, sz=%d, off=%d, %d)", path, len(buff), ofst, fh)
	fe, errCode := f.loadHandle(fh)
	if errCode < 0 {
		return errCode
	}

	of := uint64(ofst)

	if fe.rof != of && fe.rr != nil {
		// Restart the read with new offset
		_ = fe.rr.Close()
		fe.rr = nil
	}

	if fe.rr == nil {
		// Obtain the ftp.Response. It acts as an io.Reader
		rr, err := fe.conn.RetrFrom(relpath(path), of)
		if errCode = f.errToFuseErr(err); errCode < 0 {
			return errCode
		}
		fe.rr = rr
		fe.rof = of
	}

	bytesRead := 0
	bytesToRead := len(buff)
	for bytesToRead-bytesRead > 0 {
		n, err := fe.rr.Read(buff[bytesRead:])
		bytesRead += n
		if err != nil {
			if err == io.EOF {
				// Retain the ftp.Response until the file handle is released
				break
			}
			if errCode = f.errToFuseErr(err); errCode < 0 {
				return errCode
			}
		}
	}
	fe.rof += uint64(bytesRead)

	// Errors are always negative and Read expects the number of bytes read to be returned here.
	return bytesRead
}

func relpath(path string) string {
	return strings.TrimPrefix(path, "/")
}

// Readdir will read the remote directory using an MLSD command and call the given fill function
// for each entry that was found. The ofst parameter is ignored.
func (f *fuseImpl) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, fh uint64) int {
	log.Debugf("ReadDir(%s, %d)", path, fh)
	var fe *info
	var errCode int
	if fh == math.MaxUint64 {
		fe, _, errCode = f.openHandle(path, false, false)
		defer f.delete(fe.fh)
	} else {
		fe, errCode = f.loadHandle(fh)
	}
	if errCode < 0 {
		return errCode
	}
	es, err := fe.conn.List(relpath(path))
	errCode = f.errToFuseErr(err)
	if errCode < 0 {
		return errCode
	}
	for _, e := range es {
		s := &fuse.Stat_t{}
		toStat(e, s)
		if !fill(e.Name, s, 0) {
			break
		}
	}
	return 0
}

// Release will release the resources associated with the given file handle
func (f *fuseImpl) Release(path string, fh uint64) int {
	log.Debugf("Release(%s, %d)", path, fh)
	f.delete(fh)
	return 0
}

// Releasedir will release the resources associated with the given file handle
func (f *fuseImpl) Releasedir(path string, fh uint64) int {
	log.Debugf("Releasedir(%s, %d)", path, fh)
	f.delete(fh)
	return 0
}

// Rename will rename or move oldpath to newpath
func (f *fuseImpl) Rename(oldpath string, newpath string) int {
	log.Debugf("Rename(%s, %s)", oldpath, newpath)
	if oldpath == newpath {
		return 0
	}
	err := f.withConn(func(conn *ftp.ServerConn) error {
		return conn.Rename(relpath(oldpath), relpath(newpath))
	})
	return f.errToFuseErr(err)
}

// Rmdir removes the directory at path. The directory must be empty
func (f *fuseImpl) Rmdir(path string) int {
	log.Debugf("Rmdir(%s)", path)
	return f.errToFuseErr(f.withConn(func(conn *ftp.ServerConn) error {
		if err := conn.RemoveDir(relpath(path)); err != nil {
			return err
		}
		f.clearPath(path)
		return nil
	}))
}

// Truncate will truncate the given file to a certain size using a STOR command
// with zero bytes and an offset. This behavior will only work with some servers.
func (f *fuseImpl) Truncate(path string, size int64, fh uint64) int {
	log.Debugf("Truncate(%s, sz=%d, %d)", path, size, fh)
	var fe *info
	var errCode int
	if fh == math.MaxUint64 {
		fe, _, errCode = f.openHandle(path, false, false)
		defer f.delete(fe.fh)
	} else {
		fe, errCode = f.loadHandle(fh)
	}
	if errCode < 0 {
		return errCode
	}
	sz := uint64(size)
	if errCode = f.errToFuseErr(fe.conn.StorFrom(relpath(path), bytes.NewReader(nil), sz)); errCode < 0 {
		return errCode
	}
	if sz < fe.entry.Size {
		fe.entry.Size = sz
	}
	return 0
}

// Unlink will remove the path from the file system.
func (f *fuseImpl) Unlink(path string) int {
	log.Debugf("Unlink(%s)", path)
	return f.errToFuseErr(f.withConn(func(conn *ftp.ServerConn) error {
		if err := conn.Delete(relpath(path)); err != nil {
			return err
		}
		f.clearPath(path)
		return nil
	}))
}

func (i *info) pipeCopy(of uint64) int {
	// A connection dedicated to the Write function is needed because there
	// might be simultaneous Read and Write operations on the same file handle.
	conn, err := i.pool.get()
	if errCode := i.errToFuseErr(err); errCode < 0 {
		return errCode
	}
	i.wof = of
	var reader io.ReadCloser
	reader, i.writer = io.Pipe()
	i.wg.Add(1)
	go func() {
		defer func() {
			i.wg.Done()
			i.pool.put(conn)
		}()
		if err := conn.StorFrom(relpath(i.path), reader, of); err != nil {
			log.Errorf("error storing: %v", err)
		}
	}()
	return 0
}

// Write writes the given data to a file at the given offset in that file. The data
// connection that is established to facilitate the data transfer will remain open
// until the handle is released by a call to Release
func (f *fuseImpl) Write(path string, buf []byte, ofst int64, fh uint64) int {
	log.Debugf("Write(%s, sz=%d, off=%d, %d)", path, len(buf), ofst, fh)
	fe, errCode := f.loadHandle(fh)
	if errCode < 0 {
		return errCode
	}
	of := uint64(ofst)

	var ec int
	if fe.writer == nil {
		// start the pipe pumper. It ends when the fe.writer closes. That
		// happens when Release is called
		ec = fe.pipeCopy(of)
	} else if fe.wof != of {
		// Drain and restart the write operation.
		_ = fe.writer.Close()
		fe.wg.Wait()
		ec = fe.pipeCopy(of)
	}
	if ec != 0 {
		return ec
	}
	n, err := fe.writer.Write(buf)
	if errCode = f.errToFuseErr(err); errCode < 0 {
		n = errCode
	} else {
		fe.wof += uint64(n)
		if fe.wof > fe.entry.Size {
			fe.entry.Size = fe.wof
		}
	}
	return n
}

func (f *fuseImpl) cacheSize() int {
	f.RLock()
	sz := len(f.current)
	f.RUnlock()
	return sz
}

func (f *fuseImpl) clearPath(p string) {
	var pf []*info
	f.RLock()
	for _, fe := range f.current {
		if strings.HasPrefix(fe.path, p) {
			pf = append(pf, fe)
		}
	}
	f.RUnlock()
	for _, fe := range pf {
		fe.close()
		f.Lock()
		delete(f.current, fe.fh)
		f.Unlock()
		f.pool.put(fe.conn)
	}
}

func (f *fuseImpl) delete(fh uint64) {
	f.RLock()
	fe, ok := f.current[fh]
	f.RUnlock()
	if ok {
		fe.close()
		f.Lock()
		delete(f.current, fe.fh)
		f.Unlock()
		f.pool.put(fe.conn)
	}
}

func (f *fuseImpl) errToFuseErr(err error) int {
	if err == nil {
		return 0
	}

	var tpe *textproto.Error
	if errors.As(err, &tpe) {
		if tpe.Code == ftp.StatusCommandOK {
			return 0
		}
		switch tpe.Code {
		case ftp.StatusClosingDataConnection:
			return -fuse.ECONNABORTED
		case ftp.StatusNotAvailable:
			return -fuse.EADDRNOTAVAIL
		case ftp.StatusCanNotOpenDataConnection:
			return -fuse.ECONNREFUSED
		case ftp.StatusTransfertAborted:
			return -fuse.ECONNABORTED
		case ftp.StatusInvalidCredentials:
			return -fuse.EACCES
		case ftp.StatusHostUnavailable:
			return -fuse.EHOSTUNREACH
		case ftp.StatusBadFileName:
			return -fuse.EINVAL
		}
	}
	em := err.Error()
	switch {
	case
		strings.Contains(em, errConnAborted),
		strings.Contains(em, errClosed):
		return -fuse.ECONNABORTED
	case containsAny(em, fs.ErrNotExist.Error(), errFileNotFound, errDirNotFound):
		return -fuse.ENOENT
	case strings.Contains(em, errDirNotEmpty):
		return -fuse.ENOTEMPTY
	case strings.Contains(em, errBrokenPipe):
		return -fuse.EPIPE
	case strings.Contains(em, io.EOF.Error()):
		return -fuse.EIO
	case strings.Contains(em, errConnRefused):
		return -fuse.ECONNREFUSED
	case strings.Contains(em, errIOTimeout):
		return -fuse.ETIMEDOUT
	case strings.Contains(em, errFileExists):
		return -fuse.EEXIST
	default:
		// TODO
		buf := make([]byte, 0x10000)
		n := runtime.Stack(buf, false)
		log.Printf("%T %v\n%s", err, err, string(buf[:n]))
		return -fuse.EIO
	}
}

func (f *fuseImpl) getEntry(path string) (e *ftp.Entry, fuseErr int) {
	f.RLock()
	for _, fe := range f.current {
		if fe.path == path {
			f.RUnlock()
			return &fe.entry, 0
		}
	}
	f.RUnlock()
	err := f.withConn(func(conn *ftp.ServerConn) error {
		var err error
		e, err = conn.GetEntry(relpath(path))
		return err
	})
	return e, f.errToFuseErr(err)
}

func (f *fuseImpl) loadEntry(fh uint64) (*ftp.Entry, int) {
	f.RLock()
	fe, ok := f.current[fh]
	f.RUnlock()
	if !ok {
		return nil, -fuse.ENOENT
	}
	return &fe.entry, 0
}

func (f *fuseImpl) loadHandle(fh uint64) (*info, int) {
	f.RLock()
	fe, ok := f.current[fh]
	f.RUnlock()
	if !ok {
		return nil, -fuse.ENOENT
	}
	return fe, 0
}

func (f *fuseImpl) openHandle(path string, create, append bool) (nfe *info, e *ftp.Entry, errCode int) {
	f.RLock()
	shuttingDown := f.shuttingDown
	f.RUnlock()
	if shuttingDown {
		return nil, nil, -fuse.ECANCELED
	}
	conn, err := f.pool.get()
	ec := f.errToFuseErr(err)
	if ec < 0 {
		return nil, nil, ec
	}

	defer func() {
		if errCode != 0 {
			f.pool.put(conn)
		}
	}()

	if e, err = conn.GetEntry(relpath(path)); err != nil {
		errCode = f.errToFuseErr(err)
		if !(create && errCode == -fuse.ENOENT) {
			return nil, nil, errCode
		}

		// Create an empty file to ensure that it can be created
		if ec = f.errToFuseErr(conn.Stor(relpath(path), bytes.NewReader(nil))); ec < 0 {
			return nil, nil, ec
		}
		e = &ftp.Entry{
			Name: filepath.Base(path),
			Type: ftp.EntryTypeFile,
			Time: time.Now(),
		}
	}

	nfe = &info{
		fuseImpl: f,
		path:     path,
		fh:       f.nextHandle,
		conn:     conn,
		entry:    *e,
	}
	if append {
		nfe.wof = e.Size
	}
	f.Lock()
	f.current[f.nextHandle] = nfe
	f.nextHandle++
	f.Unlock()
	return nfe, e, 0
}

func (f *fuseImpl) withConn(fn func(conn *ftp.ServerConn) error) error {
	conn, err := f.pool.get()
	if err != nil {
		return err
	}
	err = fn(conn)
	f.pool.put(conn)
	return err
}

func containsAny(str string, ss ...string) bool {
	for _, s := range ss {
		if strings.Contains(str, s) {
			return true
		}
	}
	return false
}

func toStat(e *ftp.Entry, s *fuse.Stat_t) {
	// TODO: ftp actually returns a line similar to what 'ls -l' produces. It is
	// possible to parse the mode from that but the client we use here doesn't do that.
	switch e.Type {
	case ftp.EntryTypeFolder:
		s.Mode = fuse.S_IFDIR | 0755
	case ftp.EntryTypeLink:
		s.Mode = fuse.S_IFLNK | 0755
	default:
		s.Mode = fuse.S_IFREG | 0644
	}
	tm := fuse.NewTimespec(e.Time)
	s.Size = int64(e.Size)
	s.Atim = tm
	s.Ctim = tm
	s.Mtim = tm
	s.Birthtim = s.Ctim
	s.Uid = uint32(os.Getuid())
	s.Gid = uint32(os.Getgid())
	s.Nlink = 1
	s.Flags = 0
}
