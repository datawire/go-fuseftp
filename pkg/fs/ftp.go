// Package fs contains the FTP implementation of the fuse.FileSystemInterface, and the FuseHost that can mount it.
package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
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

	sync.Mutex

	// connPool is the pool of control connections to the remote FTP server.
	pool connPool

	// ctx is only used for logging purposes and final termination, because neither the ftp
	// nor the fuse implementation is context aware at this point.
	ctx context.Context

	// cancel the ctx, and hence the GC loop
	cancel context.CancelFunc

	// started is an optional channel guaranteed to be closed on the first call to Getattr
	started chan<- struct{}

	// Next file handle. File handles are opaque to FUSE, and much faster to use than
	// the full path
	nextHandle uint64

	// current maps file handles to info structs
	current map[uint64]*info

	infos map[string]*ftp.Entry
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
	of uint64

	// The writer is the writer side of a io.Pipe() used when writing data to a remote file.
	writer io.WriteCloser

	// 1 is added to this wg when the reader/writer pair is created. Wait for it when closing the writer.
	wg sync.WaitGroup
}

func (i *info) entry() *ftp.Entry {
	return i.infos[i.path]
}
func (i *info) toStat(s *fuse.Stat_t) {
	i.Lock()
	toStat(i.entry(), s)
	i.Unlock()
}

// close this handle and free up any resources that it holds.
func (i *info) close(ctx context.Context, p *connPool) {
	if i.rr != nil {
		i.rr.Close()
		i.rr = nil
	}
	if i.writer != nil {
		i.writer.Close()
		i.writer = nil
	}
	if i.conn != nil {
		p.put(ctx, i.conn)
		i.conn = nil
	}
	i.of = 0
	i.wg.Wait()
}

func (i *info) delete() {
	i.close(i.ctx, &i.pool)
	path := i.path
	delete(i.current, i.fh)
	found := false
	for _, fe := range i.current {
		if fe.path == path {
			found = true
			break
		}
	}
	if !found {
		delete(i.infos, path)
	}
}

const stalePeriod = time.Second // Fuse default cache time

func (i *info) isStale(now time.Time) bool {
	return i.rr == nil && i.writer == nil && now.Sub(i.entry().Time) >= stalePeriod
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
func NewFTPClient(ctx context.Context, addr netip.AddrPort, dir string, readTimeout time.Duration, started chan<- struct{}) (FTPClient, error) {
	f := &fuseImpl{
		current: make(map[uint64]*info),
		infos:   make(map[string]*ftp.Entry),
		pool: connPool{
			dir:         dir,
			readTimeout: readTimeout,
		},
		started: started,
	}

	ctx, cancel := context.WithCancel(ctx)
	if err := f.pool.setAddr(ctx, addr); err != nil {
		cancel()
		return nil, err
	}
	f.ctx = ctx
	f.cancel = cancel
	return f, nil
}

func (f *fuseImpl) SetAddress(ctx context.Context, addr netip.AddrPort) error {
	f.Lock()
	defer f.Unlock()
	return f.pool.setAddr(ctx, addr)
}

// Create will create a file of size zero unless the file already exists
// The third argument, the mode bits, are currently ignored
func (f *fuseImpl) Create(path string, flags int, _ uint32) (int, uint64) {
	dlog.Debugf(f.ctx, "Create(%s, %#x)", path, flags)
	fe, _, errCode := f.openDedicatedHandle(path, true, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	return 0, fe.fh
}

// Destroy will send the QUIT message to the FTP server and disconnect
func (f *fuseImpl) Destroy() {
	dlog.Debug(f.ctx, "Destroy")
	f.Lock()
	f.pool.quit(f.ctx)
	f.Unlock()
	f.cancel()
}

// Flush is a noop in this implementation
func (f *fuseImpl) Flush(path string, fh uint64) int {
	dlog.Debugf(f.ctx, "Flush(%s, %d)", path, fh)
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
				for _, fe := range f.current {
					if fe.isStale(now) {
						fe.delete()
					}
				}
				f.pool.tidy(f.ctx)
				f.Unlock()
			}
		}
	}()
}

func (f *fuseImpl) Mkdir(path string, mode uint32) int {
	dlog.Debugf(f.ctx, "Mkdir(%s, %O)", path, mode)
	err := f.withConn(f.ctx, func(conn *ftp.ServerConn) error {
		return conn.MakeDir(relpath(path))
	})
	if err != nil {
		return f.errToFuseErr(err)
	}

	// fuse will issue a Getattr after Mkdir, so we save a MLST call by adding what we already know
	f.Lock()
	f.infos[path] = &ftp.Entry{
		Name: filepath.Base(path),
		Type: ftp.EntryTypeFolder,
		Time: time.Now(),
	}
	f.current[f.nextHandle] = &info{
		fuseImpl: f,
		path:     path,
		fh:       f.nextHandle,
	}
	f.nextHandle++
	f.Unlock()
	return 0
}

// Open ensures checks if the file exists, and if it doesn't, ensure that
// a file of size zero can be created in the server.
func (f *fuseImpl) Open(path string, flags int) (int, uint64) {
	dlog.Debugf(f.ctx, "Open(%s, %#x)", path, flags)
	fe, _, errCode := f.openDedicatedHandle(path, flags&os.O_CREATE == os.O_CREATE, flags&os.O_APPEND == os.O_APPEND)
	if errCode < 0 {
		return errCode, 0
	}
	return 0, fe.fh
}

// Opendir is like Open but will fail unless the path represents a directory
func (f *fuseImpl) Opendir(path string) (int, uint64) {
	dlog.Debugf(f.ctx, "Opendir(%s)", path)
	fe, e, errCode := f.openDedicatedHandle(path, false, false)
	if errCode < 0 {
		return errCode, 0
	}
	if e.Type != ftp.EntryTypeFolder {
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
	dlog.Debugf(f.ctx, "Read(%s, sz=%d, off=%d, %d)", path, len(buff), ofst, fh)
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
		if f.pool.readTimeout != 0 {
			if err := fe.rr.SetDeadline(time.Now().Add(f.pool.readTimeout)); err != nil {
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
		fe, _, errCode = f.openDedicatedHandle(path, false, false)
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
	if oldpath == newpath {
		return 0
	}
	return f.errToFuseErr(f.withConn(f.ctx, func(conn *ftp.ServerConn) error {
		if err := conn.Rename(relpath(oldpath), relpath(newpath)); err != nil {
			return err
		}
		f.Lock()
		if e, ok := f.infos[oldpath]; ok {
			for _, fe := range f.current {
				if fe.path == oldpath {
					fe.path = newpath
				}
			}
			e.Name = filepath.Base(newpath)
			delete(f.infos, oldpath)
			f.infos[newpath] = e
		}
		f.Unlock()
		return nil
	}))
}

// Rmdir removes the directory at path. The directory must be empty
func (f *fuseImpl) Rmdir(path string) int {
	dlog.Debugf(f.ctx, "Rmdir(%s)", path)
	return f.errToFuseErr(f.withConn(f.ctx, func(conn *ftp.ServerConn) error {
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
	dlog.Debugf(f.ctx, "Truncate(%s, sz=%d, %d)", path, size, fh)
	var fe *info
	var e *ftp.Entry
	var errCode int
	if fh == math.MaxUint64 {
		fe, e, errCode = f.openDedicatedHandle(path, false, false)
		defer f.delete(fe.fh)
	} else {
		fe, errCode = f.loadHandle(fh)
		f.Lock()
		e = fe.entry()
		f.Unlock()
	}
	if errCode < 0 {
		return errCode
	}
	sz := uint64(size)
	if e.Size <= sz {
		return 0
	}
	if err := fe.conn.StorFrom(relpath(path), bytes.NewReader(nil), sz); err != nil {
		return f.errToFuseErr(err)
	}
	// macFUSE will check the size of an opened file that is truncated using a stat call
	// and perform weird things unless the truncated size is reported instantly.
	e.Size = sz
	return 0
}

// Unlink will remove the path from the file system.
func (f *fuseImpl) Unlink(path string) int {
	dlog.Debugf(f.ctx, "Unlink(%s)", path)
	return f.errToFuseErr(f.withConn(f.ctx, func(conn *ftp.ServerConn) error {
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
func (f *fuseImpl) Write(path string, buf []byte, ofst int64, fh uint64) int {
	dlog.Debugf(f.ctx, "Write(%s, sz=%d, off=%d, %d)", path, len(buf), ofst, fh)
	fe, errCode := f.loadHandle(fh)
	if errCode < 0 {
		return errCode
	}
	of := uint64(ofst)
	var err error

	// A connection dedicated to the Write function is needed because there
	// might be simultaneous Read and Write operations on the same file handle.
	f.Lock()
	if fe.writer == nil {
		var conn *ftp.ServerConn
		if conn, err = f.pool.get(f.ctx); err == nil {
			var reader io.Reader
			reader, fe.writer = io.Pipe()

			// start the pipe pumper. It ends when the fe.writer closes. That
			// happens when Release is called
			fe.wg.Add(1)
			go func() {
				defer func() {
					fe.wg.Done() // essential to call Done before Lock, or close will hang
					f.Lock()
					f.pool.put(f.ctx, conn)
					f.Unlock()
				}()
				if err := conn.StorFrom(relpath(path), reader, of); err != nil {
					dlog.Errorf(f.ctx, "error storing: %v", err)
				}
			}()
		}
	}
	f.Unlock()
	if err != nil {
		return f.errToFuseErr(err)
	}
	n, err := fe.writer.Write(buf)
	if err != nil {
		return f.errToFuseErr(err)
	}
	return n
}

func (f *fuseImpl) cacheSize() int {
	f.Lock()
	sz := len(f.current)
	f.Unlock()
	return sz
}

func (f *fuseImpl) clearPath(p string) {
	f.Lock()
	for _, fe := range f.current {
		if fe.path == p {
			fe.delete()
		}
	}
	f.Unlock()
}

func (f *fuseImpl) delete(fh uint64) {
	f.Lock()
	if fe, ok := f.current[fh]; ok {
		fe.delete()
	}
	f.Unlock()
}

func (f *fuseImpl) errToFuseErr(err error) int {
	if err == nil {
		return 0
	}

	em := err.Error()
	switch {
	case strings.HasPrefix(em, fmt.Sprintf("%d ", ftp.StatusCommandOK)):
		return 0
	case strings.HasPrefix(em, fmt.Sprintf("%d ", ftp.StatusClosingDataConnection)), strings.Contains(em, errConnAborted):
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

func (f *fuseImpl) openHandle(path string) (*info, int) {
	f.Lock()
	defer f.Unlock()
	if f.started != nil {
		defer func() {
			close(f.started)
			f.started = nil
		}()
	}
	for _, fe := range f.current {
		if fe.path == path && fe.conn == nil {
			return fe, 0
		}
	}

	conn, err := f.pool.get(f.ctx)
	if err != nil {
		return nil, f.errToFuseErr(err)
	}
	defer f.pool.put(f.ctx, conn)

	if err = f.getEntry(path, conn); err != nil {
		return nil, f.errToFuseErr(err)
	}
	nfe := &info{
		fuseImpl: f,
		path:     path,
		fh:       f.nextHandle,
	}
	f.current[f.nextHandle] = nfe
	f.nextHandle++
	return nfe, 0
}

func (f *fuseImpl) getEntry(path string, conn *ftp.ServerConn) error {
	var ok bool
	if _, ok = f.infos[path]; ok {
		return nil
	}
	return f.refreshEntry(path, conn)
}

func (f *fuseImpl) refreshEntry(path string, conn *ftp.ServerConn) error {
	e, err := conn.GetEntry(relpath(path))
	if err != nil {
		return err
	}
	f.infos[path] = e
	return nil
}

func (f *fuseImpl) openDedicatedHandle(path string, create, append bool) (nfe *info, e *ftp.Entry, errCode int) {
	f.Lock()
	defer f.Unlock()
	conn, err := f.pool.get(f.ctx)
	if err != nil {
		return nil, nil, f.errToFuseErr(err)
	}

	defer func() {
		if errCode != 0 {
			f.pool.put(f.ctx, conn)
		}
	}()

	if err = f.getEntry(path, conn); err != nil {
		errCode = f.errToFuseErr(err)
		if !(create && errCode == -fuse.ENOENT) {
			return nil, nil, errCode
		}

		// Create an empty file to ensure that it can be created
		if err = conn.Stor(relpath(path), bytes.NewReader(nil)); err != nil {
			return nil, nil, f.errToFuseErr(err)
		}
		f.infos[path] = &ftp.Entry{
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
	}
	if append {
		nfe.of = nfe.entry().Size
	}
	f.current[f.nextHandle] = nfe
	f.nextHandle++
	return nfe, nfe.entry(), 0
}

func (f *fuseImpl) withConn(ctx context.Context, fn func(conn *ftp.ServerConn) error) error {
	f.Lock()
	conn, err := f.pool.get(ctx)
	f.Unlock()
	if err != nil {
		return err
	}
	err = fn(conn)
	f.Lock()
	f.pool.put(ctx, conn)
	f.Unlock()
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
