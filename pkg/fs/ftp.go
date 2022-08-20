// Package fs contains the FTP implementation of the fuse.FileSystemInterface, and the FuseHost that can mount it.
package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/winfsp/cgofuse/fuse"

	"github.com/datawire/dlib/dlog"
)

// fuseImpl implements the fuse.FileSystemInterface. The official documentation for the API
// can be found at https://libfuse.github.io/doxygen/structfuse__operations.html
type fuseImpl struct {
	*fuse.FileSystemBase

	// All exported functions are protected by this mutex.
	sync.Mutex

	// ctx is only used for logging purposes and final termination, because neither the ftp
	// nor the fuse implementation is context aware at this point.
	ctx context.Context

	// cancel the ctx, and hence the GC loop
	cancel context.CancelFunc

	// connPool is the pool of control connections to the remote FTP server.
	connPool connPool

	// Next file handle. File handles are opaque to FUSE, and much faster to use than
	// the full path
	nextHandle uint64

	// current maps file handles to info structs
	current map[uint64]*info
}

// info holds information about file or directory that has been obtained from
// using an FTP MLST call (or sometimes populated locally with known information to
// save extra calls). In addition to the FTP information, the info is also
// responsible for maintaining a ftp.Response during Read, and a pipe (reader/writer pair)
// during Write.
type info struct {
	*ftp.Entry

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
	of uint64

	// The reader and writer stems from an io.Pipe() used when writing data to a remote file.
	reader io.Reader
	writer io.WriteCloser

	// 1 is added to this wg when the reader/writer pair is created. Wait for it when closing the writer.
	wg sync.WaitGroup
}

func (f *info) toStat(s *fuse.Stat_t) {
	toStat(f.Entry, s)
}

// close this handle and free up any resources that it holds.
func (f *info) close(ctx context.Context, p *connPool) {
	if f.rr != nil {
		f.rr.Close()
		f.rr = nil
	}
	if f.writer != nil {
		f.writer.Close()
		f.writer = nil
		f.reader = nil
		f.wg.Wait()
	}
	if f.conn != nil {
		p.put(ctx, f.conn)
		f.conn = nil
	}
	f.of = 0
}

const stalePeriod = time.Second // Fuse default cache time

func (f *info) isStale(now time.Time) bool {
	return f.rr == nil && f.writer == nil && now.Sub(f.Time) >= stalePeriod
}

type FTPClient interface {
	fuse.FileSystemInterface

	// SetAddress will quit open connections, change the address, and reconnect
	// The method is intended to be used when a FUSE mount must survive a change of
	// FTP server address.
	SetAddress(ctx context.Context, addr netip.AddrPort) error
}

// NewFTPClient returns an implementation of the fuse.FileSystemInterface that is backed by
// an FTP server connection tp the address. The dir parameter is the directory that the
// FTP server changes to when connecting.
func NewFTPClient(ctx context.Context, addr netip.AddrPort, dir string, readTimeout time.Duration) (FTPClient, error) {
	f := &fuseImpl{
		current: make(map[uint64]*info),
		connPool: connPool{
			dir:         dir,
			readTimeout: readTimeout,
		},
	}

	ctx, cancel := context.WithCancel(ctx)
	if err := f.connPool.setAddr(ctx, addr); err != nil {
		cancel()
		return nil, err
	}
	f.ctx = ctx
	f.cancel = cancel
	return f, nil
}

func (f *fuseImpl) SetAddress(ctx context.Context, addr netip.AddrPort) error {
	return f.connPool.setAddr(ctx, addr)
}

func (f *fuseImpl) cacheSize() int {
	f.Lock()
	sz := len(f.current)
	f.Unlock()
	return sz
}

// Create will create a file of size zero unless the file already exists
// The third argument, the mode bits, are currently ignored
func (f *fuseImpl) Create(path string, flags int, _ uint32) (int, uint64) {
	dlog.Debugf(f.ctx, "Create(%s, %#x)", path, flags)
	fe, errCode := f.openDedicatedHandle(path, true, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	return 0, fe.fh
}

// Destroy will send the QUIT message to the FTP server and disconnect
func (f *fuseImpl) Destroy() {
	dlog.Debug(f.ctx, "Destroy")
	f.connPool.quit(f.ctx)
	f.cancel()
}

// Flush is a noop in this implementation
func (f *fuseImpl) Flush(path string, _ uint64) int {
	dlog.Debugf(f.ctx, "Flush(%s)", path)
	return 0
}

// Getattr gets file attributes. The UID and GID will always be the
// UID and GID of the caller. File mode is always 0644 and Directory
// mode is always 0755.
func (f *fuseImpl) Getattr(path string, s *fuse.Stat_t, fh uint64) int {
	dlog.Debugf(f.ctx, "Getattr(%s, %d)", path, fh)
	var fe *info
	var errCode int
	if fh == math.MaxUint64 {
		fe, errCode = f.openHandle(path)
	} else {
		fe, errCode = f.loadHandle(fh)
	}
	if errCode < 0 {
		return errCode
	}
	fe.toStat(s)
	return 0
}

// Init starts the garbage collector that removes cached items when they
// haven't been used for a period of time.
func (f *fuseImpl) Init() {
	dlog.Debug(f.ctx, "Init")
	go func() {
		ticker := time.NewTicker(stalePeriod)
		for {
			select {
			case <-f.ctx.Done():
				return
			case now := <-ticker.C:
				f.Lock()
				for fh, fe := range f.current {
					if fe.isStale(now) {
						fe.close(f.ctx, &f.connPool)
						delete(f.current, fh)
					}
				}
				f.Unlock()
				f.connPool.tidy(f.ctx)
			}
		}
	}()
}

func (f *fuseImpl) Mkdir(path string, mode uint32) int {
	dlog.Debugf(f.ctx, "Mkdir(%s, %O)", path, mode)
	err := f.connPool.withConn(f.ctx, func(conn *ftp.ServerConn) error {
		return conn.MakeDir(relpath(path))
	})
	if err != nil {
		return f.errToFuseErr(err)
	}

	// fuse will issue a Getattr after Mkdir, so we save a MLST call by adding what we already know
	f.Lock()
	f.current[f.nextHandle] = &info{
		Entry: &ftp.Entry{
			Name: filepath.Base(relpath(path)),
			Type: ftp.EntryTypeFolder,
			Time: time.Now(),
		},
		path: path,
		fh:   f.nextHandle,
	}
	f.nextHandle++
	f.Unlock()
	return 0
}

// Open ensures checks if the file exists, and if it doesn't, ensure that
// a file of size zero can be created in the server.
func (f *fuseImpl) Open(path string, flags int) (int, uint64) {
	dlog.Debugf(f.ctx, "Open(%s, %#x)", path, flags)
	fe, errCode := f.openDedicatedHandle(path, flags&os.O_CREATE == os.O_CREATE, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	return 0, fe.fh
}

// Opendir is like Open but will fail unless the path represents a directory
func (f *fuseImpl) Opendir(path string) (int, uint64) {
	dlog.Debugf(f.ctx, "Opendir(%s)", path)
	fe, errCode := f.openDedicatedHandle(path, false, false)
	if errCode < 0 {
		return errCode, 0
	}
	if fe.Type != ftp.EntryTypeFolder {
		f.delete(fe.fh)
		return -fuse.ENOTDIR, 0
	}
	dlog.Debugf(f.ctx, "Opendir(%s) -> %d", path, fe.fh)
	return 0, fe.fh
}

// Read a chunk of data using ftp RETR. The result returned from the server
// is cached and used in subsequent reads until EOF is reached or the file
// handle is released.
//
// Read requires that fuse is started with -o sync_read to ensure that the
// read calls arrive in sequence.
func (f *fuseImpl) Read(path string, buff []byte, ofst int64, fh uint64) int {
	dlog.Debugf(f.ctx, "Read(%s, %d, %d, %d)", path, len(buff), ofst, fh)
	fe, errCode := f.loadHandle(fh)
	if errCode < 0 {
		return errCode
	}

	of := uint64(ofst)
	if fe.of != of && fe.rr != nil {
		// This should normally not happen, but if it does, let's restart the read
		_ = fe.rr.Close()
		fe.rr = nil
	}

	if fe.rr == nil {
		// Obtain the ftp.Response. It acts as an io.Reader
		rr, err := fe.conn.RetrFrom(relpath(path), of)
		if err != nil {
			return f.errToFuseErr(err)
		}
		fe.rr = rr
		fe.of = of
	}

	bytesRead := 0
	bytesToRead := len(buff)
	for bytesToRead-bytesRead > 0 {
		if f.connPool.readTimeout != 0 {
			if err := fe.rr.SetDeadline(time.Now().Add(f.connPool.readTimeout)); err != nil {
				return f.errToFuseErr(err)
			}
		}
		n, err := fe.rr.Read(buff[bytesRead:])
		bytesRead += n
		if err != nil {
			if err == io.EOF {
				// Retain the ftp.Response until the file handle is released
				break
			}
			return f.errToFuseErr(err)
		}
	}
	fe.of += uint64(bytesRead)

	// Errors are always negative and Read expects the number of bytes read to be returned here.
	return bytesRead
}

func relpath(path string) string {
	return strings.TrimPrefix(path, "/")
}

// Readdir will read the remote directory using an MLSD command and call the given fill function
// for each entry that was found. The ofst parameter is ignored.
func (f *fuseImpl) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	dlog.Debugf(f.ctx, "ReadDir(%s, %d)", path, fh)
	var fe *info
	var errCode int
	if fh == math.MaxUint64 {
		fe, errCode = f.openDedicatedHandle(path, false, false)
		defer f.delete(fe.fh)
	} else {
		fe, errCode = f.loadHandle(fh)
	}
	if errCode < 0 {
		return errCode
	}
	es, err := fe.conn.List(relpath(path))
	if err != nil {
		return f.errToFuseErr(err)
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
	dlog.Debugf(f.ctx, "Release(%s, %d)", path, fh)
	f.delete(fh)
	return 0
}

// Releasedir will release the resources associated with the given file handle
func (f *fuseImpl) Releasedir(path string, fh uint64) int {
	dlog.Debugf(f.ctx, "Releasedir(%s, %d)", path, fh)
	f.delete(fh)
	return 0
}

// Rename will rename or move oldpath to newpath
func (f *fuseImpl) Rename(oldpath string, newpath string) int {
	dlog.Debugf(f.ctx, "Rename(%s, %s)", oldpath, newpath)
	return f.errToFuseErr(f.connPool.withConn(f.ctx, func(conn *ftp.ServerConn) error {
		if err := conn.Rename(relpath(oldpath), relpath(newpath)); err != nil {
			return err
		}
		f.Lock()
		for _, fe := range f.current {
			if fe.path == oldpath {
				fe.path = newpath
			}
		}
		f.Unlock()
		return nil
	}))
}

// Rmdir removes the directory at path. The directory must be empty
func (f *fuseImpl) Rmdir(path string) int {
	dlog.Debugf(f.ctx, "Rmdir(%s)", path)
	return f.errToFuseErr(f.connPool.withConn(f.ctx, func(conn *ftp.ServerConn) error {
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
	dlog.Debugf(f.ctx, "Truncate(%s, %d, %d)", path, size, fh)
	var fe *info
	var errCode int
	if fh == math.MaxUint64 {
		fe, errCode = f.openDedicatedHandle(path, false, false)
		defer f.delete(fe.fh)
	} else {
		fe, errCode = f.loadHandle(fh)
	}
	if errCode < 0 {
		return errCode
	}
	sz := uint64(size)
	if fe.Size <= sz {
		return 0
	}
	if err := fe.conn.StorFrom(relpath(path), bytes.NewReader(nil), sz); err != nil {
		return f.errToFuseErr(err)
	}
	return 0
}

// Unlink will remove the path from the file system.
func (f *fuseImpl) Unlink(path string) int {
	dlog.Debugf(f.ctx, "Unlink(%s)", path)
	return f.errToFuseErr(f.connPool.withConn(f.ctx, func(conn *ftp.ServerConn) error {
		if err := conn.Delete(relpath(path)); err != nil {
			return err
		}
		f.clearPath(path)
		return nil
	}))
}

// Write writes the gven data to a file at the given offset in that file. The
// data connection that is established to facilitate the write will remain open
// until the handle is released by a call to Release
func (f *fuseImpl) Write(path string, buff []byte, ofst int64, fh uint64) int {
	dlog.Debugf(f.ctx, "Write(%s, %d, %d, %d)", path, len(buff), ofst, fh)
	fe, errCode := f.loadHandle(fh)
	if errCode < 0 {
		return errCode
	}
	of := uint64(ofst)
	if fe.reader == nil {
		fe.reader, fe.writer = io.Pipe()
		fe.wg.Add(1)
		go func() {
			defer fe.wg.Done()
			if err := fe.conn.StorFrom(relpath(path), fe.reader, of); err != nil {
				dlog.Errorf(f.ctx, "error storing: %v", err)
			}
		}()
	}
	n, err := fe.writer.Write(buff)
	if err != nil {
		return f.errToFuseErr(err)
	}
	return n
}

func (f *fuseImpl) clearPath(p string) {
	f.Lock()
	for fh, fe := range f.current {
		if fe.path == p {
			fe.close(f.ctx, &f.connPool)
			delete(f.current, fh)
		}
	}
	f.Unlock()
}

func (f *fuseImpl) delete(fh uint64) {
	f.Lock()
	if fe, ok := f.current[fh]; ok {
		fe.close(f.ctx, &f.connPool)
		delete(f.current, fh)
	}
	f.Unlock()
}

func (f *fuseImpl) errToFuseErr(err error) int {
	if err == nil {
		return 0
	}
	em := err.Error()
	switch {
	case strings.HasPrefix(em, ftp.StatusText(ftp.StatusCommandOK)+" "):
		return 0
	case strings.HasPrefix(em, ftp.StatusText(ftp.StatusClosingDataConnection)+" "):
		return -fuse.ECONNABORTED
	case strings.Contains(em, "file does not exist"), strings.Contains(em, "no such file or directory"):
		dlog.Errorf(f.ctx, "%T %v", err, err)
		return -fuse.ENOENT
	case strings.Contains(em, "directory not empty"):
		return -fuse.ENOTEMPTY
	case strings.Contains(em, "broken pipe"):
		return -fuse.EPIPE
	case strings.Contains(em, "EOF"):
		return -fuse.EIO
	case strings.Contains(em, "connection refused"):
		return -fuse.ECONNREFUSED
	default:
		// TODO
		buf := make([]byte, 0x10000)
		n := runtime.Stack(buf, false)
		fmt.Fprintf(dlog.StdLogger(f.ctx, dlog.MaxLogLevel(f.ctx)).Writer(), "%T %v\n%s", err, err, string(buf[:n]))
		return -fuse.EIO
	}
}

func (f *fuseImpl) loadHandle(fh uint64) (*info, int) {
	f.Lock()
	fe, ok := f.current[fh]
	f.Unlock()
	if !ok {
		return nil, -fuse.ENOENT
	}
	return fe, 0
}

func (f *fuseImpl) openHandle(path string) (nfe *info, errCode int) {
	f.Lock()
	for _, fe := range f.current {
		if fe.path == path && fe.conn == nil {
			nfe = fe
			break
		}
	}
	f.Unlock()
	if nfe != nil {
		return nfe, 0
	}

	conn, err := f.connPool.get(f.ctx)
	if err != nil {
		return nil, f.errToFuseErr(err)
	}
	defer f.connPool.put(f.ctx, conn)

	nfe = &info{
		path: path,
		fh:   f.nextHandle,
	}
	nfe.Entry, err = conn.GetEntry(relpath(path))
	if err != nil {
		return nil, f.errToFuseErr(err)
	}
	f.Lock()
	f.current[f.nextHandle] = nfe
	f.nextHandle++
	f.Unlock()
	return nfe, 0
}

func (f *fuseImpl) openDedicatedHandle(path string, create, append bool) (nfe *info, errCode int) {
	conn, err := f.connPool.get(f.ctx)
	if err != nil {
		return nil, f.errToFuseErr(err)
	}
	defer func() {
		if errCode != 0 {
			f.connPool.put(f.ctx, conn)
		}
	}()

	f.Lock()
	for _, fe := range f.current {
		if fe.path == path {
			if fe.conn == nil {
				nfe = fe
			} else {
				nfe = &info{
					path:  path,
					fh:    f.nextHandle,
					Entry: fe.Entry,
				}
				f.current[f.nextHandle] = nfe
				f.nextHandle++
			}
			nfe.conn = conn
			if append {
				nfe.of = nfe.Size
			}
			break
		}
	}
	f.Unlock()
	if nfe != nil {
		return nfe, 0
	}

	nfe = &info{
		path: path,
		fh:   f.nextHandle,
		conn: conn,
	}

	rPath := relpath(path)
	nfe.Entry, err = conn.GetEntry(rPath)
	if err != nil {
		errCode = f.errToFuseErr(err)
		if !(create && errCode == -fuse.ENOENT) {
			return nil, errCode
		}

		// Create an empty file to ensure that it can be created
		dlog.Debugf(f.ctx, "conn.Stor(%s)", rPath)
		if err = conn.Stor(rPath, bytes.NewReader(nil)); err != nil {
			return nil, f.errToFuseErr(err)
		}
		nfe.Entry = &ftp.Entry{
			Name: filepath.Base(rPath),
			Type: ftp.EntryTypeFile,
			Time: time.Now(),
		}
	}
	if append {
		nfe.of = nfe.Size
	}
	f.Lock()
	f.current[f.nextHandle] = nfe
	f.nextHandle++
	f.Unlock()
	return nfe, 0
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
