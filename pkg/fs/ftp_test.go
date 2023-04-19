package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	fs2 "io/fs"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datawire/dlib/dlog"
	server "github.com/datawire/go-ftpserver"
)

type tbWrapper struct {
	testing.TB
	level  dlog.LogLevel
	fields map[string]any
}

type tbWriter struct {
	*tbWrapper
	l dlog.LogLevel
}

func (w *tbWriter) Write(data []byte) (n int, err error) {
	w.Helper()
	w.Log(w.l, strings.TrimSuffix(string(data), "\n")) // strip trailing newline if present, since the Log() call appends a newline
	return len(data), nil
}

func NewTestLogger(t testing.TB, level dlog.LogLevel) dlog.Logger {
	return &tbWrapper{TB: t, level: level}
}

func (w *tbWrapper) StdLogger(l dlog.LogLevel) *log.Logger {
	return log.New(&tbWriter{tbWrapper: w, l: l}, "", 0)
}

func (w *tbWrapper) WithField(key string, value any) dlog.Logger {
	ret := tbWrapper{
		TB:     w.TB,
		fields: make(map[string]any, len(w.fields)+1),
	}
	for k, v := range w.fields {
		ret.fields[k] = v
	}
	ret.fields[key] = value
	return &ret
}

func (w *tbWrapper) Log(level dlog.LogLevel, msg string) {
	if level > w.level {
		return
	}
	w.Helper()
	w.UnformattedLog(level, msg)
}

func (w *tbWrapper) MaxLevel() dlog.LogLevel {
	return w.level
}

func (w *tbWrapper) UnformattedLog(level dlog.LogLevel, args ...any) {
	if level > w.level {
		return
	}
	w.Helper()
	sb := strings.Builder{}
	sb.WriteString(time.Now().Format("15:04:05.0000"))
	for _, arg := range args {
		sb.WriteString(" ")
		fmt.Fprint(&sb, arg)
	}

	if len(w.fields) > 0 {
		parts := make([]string, 0, len(w.fields))
		for k := range w.fields {
			parts = append(parts, k)
		}
		sort.Strings(parts)

		for i, k := range parts {
			if i > 0 {
				sb.WriteString(" ")
			}
			fmt.Fprintf(&sb, "%s=%#v", k, w.fields[k])
		}
	}
	w.TB.Log(sb.String())
}

func (w *tbWrapper) UnformattedLogf(level dlog.LogLevel, format string, args ...any) {
	if level > w.level {
		return
	}
	w.Helper()
	w.UnformattedLog(level, fmt.Sprintf(format, args...))
}

func (w *tbWrapper) UnformattedLogln(level dlog.LogLevel, args ...any) {
	if level > w.level {
		return
	}
	w.Helper()
	w.UnformattedLog(level, fmt.Sprintln(args...))
}

func testContext(t *testing.T) context.Context {
	lr := logrus.New()
	lr.Level = logrus.DebugLevel
	lr.SetFormatter(&logrus.TextFormatter{
		ForceColors:               false,
		DisableColors:             true,
		ForceQuote:                false,
		DisableQuote:              true,
		EnvironmentOverrideColors: false,
		DisableTimestamp:          true,
		FullTimestamp:             false,
		TimestampFormat:           "",
		DisableSorting:            true,
		SortingFunc:               nil,
		DisableLevelTruncation:    true,
		PadLevelText:              false,
		QuoteEmptyFields:          false,
		FieldMap:                  nil,
		CallerPrettyfier:          nil,
	})
	return dlog.WithLogger(context.Background(), dlog.WrapLogrus(lr))
}

const remoteDir = "exported"

// startFTPServer starts an FTP server and returns the directory that it exports and the port that it is listening to
func startFTPServer(t *testing.T, ctx context.Context, dir string, wg *sync.WaitGroup) (string, uint16) {
	dir = filepath.Join(dir, "server")
	export := filepath.Join(dir, remoteDir)
	require.NoError(t, os.MkdirAll(export, 0755))
	portCh := make(chan uint16)

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, server.Start(ctx, "127.0.0.1", dir, portCh))
	}()

	select {
	case <-ctx.Done():
		return "", 0
	case port := <-portCh:
		return export, port
	}
}

func startFUSEHost(t *testing.T, ctx context.Context, port uint16, dir string) (FTPClient, *FuseHost, string) {
	// Start the client
	dir = filepath.Join(dir, "mount")
	require.NoError(t, os.Mkdir(dir, 0755))
	started := make(chan error, 1)
	fsh, err := NewFTPClient(ctx, netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port)), remoteDir, 30*time.Second)
	require.NoError(t, err)
	mp := dir
	if runtime.GOOS == "windows" {
		mp = "T:"
		dir = mp + `\`
	}
	host := NewHost(fsh, mp)
	host.Start(ctx, started)
	select {
	case err := <-started:
		require.NoError(t, err)
		dlog.Info(ctx, "FUSE started")
	case <-ctx.Done():
	}
	return fsh, host, dir
}

func TestConnectFailure(t *testing.T) {
	ctx := testContext(t)
	_, err := NewFTPClient(ctx, netip.MustParseAddrPort("198.51.100.32:21"), "", time.Second)
	require.Error(t, err)
}

func TestBrokenConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))

	wg := sync.WaitGroup{}
	serverCtx, serverCancel := context.WithCancel(ctx)
	defer serverCancel()

	tmp := t.TempDir()
	root, port := startFTPServer(t, serverCtx, tmp, &wg)
	require.NotEqual(t, uint16(0), port)

	fsh, host, mountPoint := startFUSEHost(t, ctx, port, tmp)
	defer func() {
		cancel()
		host.Stop()
	}()
	contents := []byte("Some text\n")
	require.NoError(t, os.WriteFile(filepath.Join(root, "test1.txt"), contents, 0644))
	time.Sleep(time.Millisecond)

	test1Mounted, err := os.ReadFile(filepath.Join(mountPoint, "test1.txt"))
	require.NoError(t, err, fmt.Sprintf("%s ReadFile: %v", time.Now().Format("15:04:05.0000"), err))
	assert.True(t, bytes.Equal(contents, test1Mounted))

	// Break the connection (stop the server)
	serverCancel()
	wg.Wait()

	broken := func(err error) bool {
		pe := &fs2.PathError{}
		if errors.As(err, &pe) {
			ee := pe.Error()
			return containsAny(ee, errIO, errBrokenPipe, errConnRefused, errConnAborted, errUnexpectedNetworkError)
		}
		return false
	}

	t.Run("Stat", func(t *testing.T) {
		_, err := os.Stat(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("Create", func(t *testing.T) {
		_, err := os.Create(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("Read", func(t *testing.T) {
		_, err := os.ReadFile(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("Mkdir", func(t *testing.T) {
		_, err := os.ReadDir(filepath.Join(mountPoint, "x"))
		require.True(t, broken(err))
	})

	t.Run("ReadDir", func(t *testing.T) {
		_, err := os.ReadDir(filepath.Join(mountPoint, "a"))
		require.True(t, broken(err))
	})

	t.Run("Open", func(t *testing.T) {
		_, err := os.Open(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("Write", func(t *testing.T) {
		// Write a file to the mounted directory
		require.True(t, broken(os.WriteFile(filepath.Join(mountPoint, "somefile.txt"), contents, 0644)))
	})

	t.Run("Restart", func(t *testing.T) {
		// Start a new server
		root, port = startFTPServer(t, ctx, tmp, &wg)
		require.NotEqual(t, uint16(0), port)
		require.NoError(t, os.WriteFile(filepath.Join(root, "test1.txt"), contents, 0644))

		// Assign the new address to the FTP client (it should now quit all connections and reconnect)
		require.NoError(t, fsh.SetAddress(ctx, netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port))))

		// Ensure that the connection is restored
		test1Mounted, err = os.ReadFile(filepath.Join(mountPoint, "test1.txt"))
		require.NoError(t, err, fmt.Sprintf("%s ReadFile: %v", time.Now().Format("15:04:05.0000"), err))
		assert.True(t, bytes.Equal(contents, test1Mounted))
	})
}

func TestConnectedToServer(t *testing.T) {
	ctx, cancel := context.WithCancel(testContext(t))

	wg := sync.WaitGroup{}
	tmp := t.TempDir()
	root, port := startFTPServer(t, ctx, tmp, &wg)
	require.NotEqual(t, uint16(0), port)

	fsh, host, mountPoint := startFUSEHost(t, ctx, port, tmp)
	t.Cleanup(func() {
		host.Stop()
		cancel()
		wg.Wait()
	})

	// Create a file on the server side
	const contentSize = 1024 * 1024 * 20
	testContents := make([]byte, contentSize)
	for i := 0; i < contentSize; i++ {
		testContents[i] = byte(i & 0xff)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "test1.txt"), testContents, 0644))

	t.Run("Read", func(t *testing.T) {
		test1Mounted, err := os.ReadFile(filepath.Join(mountPoint, "test1.txt"))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(testContents, test1Mounted))
	})

	t.Run("Read non-existing", func(t *testing.T) {
		_, err := os.ReadFile(filepath.Join(mountPoint, "nosuchfile.txt"))
		require.ErrorIs(t, err, fs2.ErrNotExist)
	})

	t.Run("Write", func(t *testing.T) {
		// Write a file to the mounted directory
		require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "test2.txt"), testContents, 0644))
		time.Sleep(time.Millisecond)

		// Read from the directory exported by the FTP server
		test2Exported, err := os.ReadFile(filepath.Join(root, "test2.txt"))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(testContents, test2Exported))
	})

	t.Run("Write non-existing", func(t *testing.T) {
		// Write a file to the mounted directory
		err := os.WriteFile(filepath.Join(mountPoint, "nodir", "test2.txt"), testContents, 0644)
		require.ErrorIs(t, err, fs2.ErrNotExist)
	})

	t.Run("MkdirAll", func(t *testing.T) {
		// Make directories in the mounted directory
		err := os.MkdirAll(filepath.Join(mountPoint, "a", "b"), 0755)
		require.NoError(t, err)

		// And assert that they were created in the directory exported by the FTP server
		st, err := os.Stat(filepath.Join(root, "a", "b"))
		require.NoError(t, err)
		require.True(t, st.IsDir())
	})

	t.Run("Create", func(t *testing.T) {
		f, err := os.Create(filepath.Join(mountPoint, "a", "test3.txt"))
		require.NoError(t, err)
		msg := "Hello World\n"
		n, err := f.WriteString(msg)
		assert.NoError(t, err)
		assert.NoError(t, f.Close())
		assert.Equal(t, len(msg), n)
		time.Sleep(time.Millisecond)

		// Check that the text was received by the FTP server
		test3Exported, err := os.ReadFile(filepath.Join(root, "a", "test3.txt"))
		require.NoError(t, err)
		assert.Equal(t, msg, string(test3Exported))
	})

	t.Run("Create dir-exists", func(t *testing.T) {
		_, err := os.Create(filepath.Join(mountPoint, "a", "b"))
		isDir := &fs2.PathError{}
		require.ErrorAs(t, err, &isDir)
		require.Contains(t, isDir.Error(), errIsDirectory)
	})

	t.Run("Create no-such-dir", func(t *testing.T) {
		_, err := os.Create(filepath.Join(mountPoint, "b", "test3.txt"))
		noSuchDir := &fs2.PathError{}
		require.ErrorAs(t, err, &noSuchDir)
		require.Contains(t, noSuchDir.Error(), errDirNotFound)
	})

	t.Run("Rename", func(t *testing.T) {
		// Move test1.txt and test2.txt to a/b in the mounted fs
		err := os.Rename(filepath.Join(mountPoint, "test1.txt"), filepath.Join(mountPoint, "a", "b", "test1.txt"))
		require.NoError(t, err)
		err = os.Rename(filepath.Join(mountPoint, "test2.txt"), filepath.Join(mountPoint, "a", "b", "test2.txt"))
		require.NoError(t, err)

		// And assert that they no longer exists in the root fs exported by the FTP server
		_, err = os.Stat(filepath.Join(root, "test1.txt"))
		require.ErrorIs(t, err, fs2.ErrNotExist)
		_, err = os.Stat(filepath.Join(root, "test2.txt"))
		require.ErrorIs(t, err, fs2.ErrNotExist)

		// but do exist in the a/b directory exported by the FTP server
		_, err = os.Stat(filepath.Join(root, "a", "b", "test1.txt"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(root, "a", "b", "test2.txt"))
		require.NoError(t, err)

		// and exist in the mounted a/b directory
		_, err = os.Stat(filepath.Join(mountPoint, "a", "b", "test1.txt"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(mountPoint, "a", "b", "test2.txt"))
		require.NoError(t, err)
	})

	hasName := func(es []fs2.DirEntry, n string) bool {
		for _, e := range es {
			if e.Name() == n {
				return true
			}
		}
		return false
	}

	t.Run("ReadDir", func(t *testing.T) {
		es, err := os.ReadDir(filepath.Join(mountPoint, "a", "b"))
		require.NoError(t, err)
		require.Len(t, es, 2)
		assert.True(t, hasName(es, "test1.txt"))
		assert.True(t, hasName(es, "test2.txt"))

		es, err = os.ReadDir(filepath.Join(mountPoint, "a"))
		require.NoError(t, err)
		require.Len(t, es, 2)
		assert.True(t, hasName(es, "b"))
		assert.True(t, hasName(es, "test3.txt"))
		assert.True(t, es[0].IsDir())
	})

	t.Run("File.ReadDir", func(t *testing.T) {
		df, err := os.Open(filepath.Join(mountPoint, "a", "b"))
		require.NoError(t, err)
		defer df.Close()
		es, err := df.ReadDir(0)
		require.NoError(t, err)
		require.Len(t, es, 2)
		assert.True(t, hasName(es, "test1.txt"))
		assert.True(t, hasName(es, "test2.txt"))
	})

	t.Run("File.ReadDir not-dir", func(t *testing.T) {
		df, err := os.Open(filepath.Join(mountPoint, "a", "test3.txt"))
		require.NoError(t, err)
		defer df.Close()
		_, err = df.ReadDir(0)
		notADir := &fs2.PathError{}
		require.ErrorAs(t, err, &notADir)
		require.Contains(t, notADir.Error(), errNotDirectory)
	})

	t.Run("Truncate", func(t *testing.T) {
		tcf := make([]byte, 0x1500)
		copy(tcf, testContents)
		require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "trunc1.txt"), tcf, 0644))
		require.NoError(t, os.Truncate(filepath.Join(mountPoint, "trunc1.txt"), 0x1000))

		// Read from the directory exported by the FTP server
		test1Exported, err := os.ReadFile(filepath.Join(root, "trunc1.txt"))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(tcf[:0x1000], test1Exported))
	})

	t.Run("Truncate Open", func(t *testing.T) {
		tcf := make([]byte, 1500)
		copy(tcf, testContents)
		// Open a mounted file, truncate it, then seek to EOF and write some text
		// Write a file to the mounted directory
		require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "trunc2.txt"), tcf, 0644))
		f, err := os.OpenFile(filepath.Join(mountPoint, "trunc2.txt"), os.O_CREATE|os.O_WRONLY, 0600)
		t.Log("OpenFile complete")
		require.NoError(t, err)
		require.NoError(t, f.Truncate(1000))
		t.Log("Truncate complete")
		msg := []byte("hello")
		_, err = f.Seek(1000, 0)
		t.Log("Seek complete")
		require.NoError(t, err)
		_, err = f.Write(msg)
		t.Log("Write complete")
		require.NoError(t, err)
		require.NoError(t, f.Close())
		t.Log("Close complete")

		time.Sleep(time.Millisecond)
		// Check that the text was received by the FTP server
		test2Exported, err := os.ReadFile(filepath.Join(root, "trunc2.txt"))
		require.NoError(t, err)
		copy(tcf[1000:], msg)
		assert.True(t, len(test2Exported) == 1005)
		assert.True(t, bytes.Equal(tcf[:1005], test2Exported))
	})

	t.Run("Remove not-empty-dir", func(t *testing.T) {
		err := os.Remove(filepath.Join(mountPoint, "a", "b"))
		notEmpty := &fs2.PathError{}
		require.ErrorAs(t, err, &notEmpty)
		require.Contains(t, notEmpty.Error(), errDirNotEmpty)
	})

	t.Run("Remove non-existent", func(t *testing.T) {
		err := os.Remove(filepath.Join(mountPoint, "a", "nodir"))
		notFound := &fs2.PathError{}
		require.ErrorAs(t, err, &notFound)
		require.Contains(t, notFound.Error(), errFileNotFound)
	})

	t.Run("RemoveAll", func(t *testing.T) {
		err := os.RemoveAll(filepath.Join(mountPoint, "a", "b"))
		require.NoError(t, err)
	})

	t.Run("GC cleans cache", func(t *testing.T) {
		time.Sleep(2200 * time.Millisecond)
		require.Equal(t, 0, fsh.(*fuseImpl).cacheSize())
	})

	t.Run("MkDir file-exists", func(t *testing.T) {
		err := os.Mkdir(filepath.Join(mountPoint, "a", "test3.txt"), 0755)
		isFile := &fs2.PathError{}
		require.ErrorAs(t, err, &isFile)
		require.Contains(t, isFile.Error(), errFileExists)
	})
}
