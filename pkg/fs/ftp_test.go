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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/go-fuseftp/pkg/server"
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

const remoteDir = "exported"

// startFTPServer starts an FTP server and returns the directory that it exports and the port that it is listening to
func startFTPServer(t *testing.T, ctx context.Context, wg *sync.WaitGroup) (string, uint16) {
	root := t.TempDir()
	export := filepath.Join(root, remoteDir)
	require.NoError(t, os.Mkdir(export, 0755))
	portCh := make(chan uint16)

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, server.Start(ctx, "127.0.0.1", root, portCh))
	}()

	select {
	case <-ctx.Done():
		return "", 0
	case port := <-portCh:
		return export, port
	}
}

func TestConnectionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = dlog.WithLogger(ctx, NewTestLogger(t, dlog.LogLevelInfo))

	wg := sync.WaitGroup{}
	serverCtx, serverCancel := context.WithCancel(ctx)
	root, port := startFTPServer(t, serverCtx, &wg)
	require.NotEqual(t, uint16(0), port)

	// Start the client
	fsh, err := NewFTPClient(ctx, netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port)), remoteDir, time.Second)
	require.NoError(t, err)

	mountPoint := t.TempDir()
	host := NewHost(fsh, mountPoint)
	host.Start(ctx)
	t.Cleanup(func() {
		host.Stop()
		cancel()
	})
	contents := []byte("Some text\n")
	require.NoError(t, os.WriteFile(filepath.Join(root, "test1.txt"), contents, 0644))
	// Wait for things to get set up.
	time.Sleep(3 * time.Second)

	test1Mounted, err := os.ReadFile(filepath.Join(mountPoint, "test1.txt"))
	require.NoError(t, err, fmt.Sprintf("%s ReadFile: %v", time.Now().Format("15:04:05.0000"), err))
	assert.True(t, bytes.Equal(contents, test1Mounted))

	// Break the connection (stop the server)
	serverCancel()
	wg.Wait()
	time.Sleep(2 * time.Second)

	broken := func(err error) bool {
		pe := &fs2.PathError{}
		if errors.As(err, &pe) {
			ee := pe.Error()
			return strings.Contains(ee, "input/output error") || strings.Contains(ee, "broken pipe") || strings.Contains(ee, "connection refused")
		}
		return false
	}

	t.Run("Create", func(t *testing.T) {
		_, err := os.Create(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("Read", func(t *testing.T) {
		_, err := os.ReadFile(filepath.Join(mountPoint, "somefile.txt"))
		require.True(t, broken(err))
	})

	t.Run("ReadDir", func(t *testing.T) {
		_, err := os.ReadDir(filepath.Join(mountPoint, "a", "b"))
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
		root, port = startFTPServer(t, ctx, &wg)
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
	ctx, cancel := context.WithCancel(context.Background())
	ctx = dlog.WithLogger(ctx, NewTestLogger(t, dlog.LogLevelDebug))

	wg := sync.WaitGroup{}
	root, port := startFTPServer(t, ctx, &wg)
	require.NotEqual(t, uint16(0), port)

	// Start the client
	fsh, err := NewFTPClient(ctx, netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port)), remoteDir, time.Second)
	require.NoError(t, err)

	mountPoint := t.TempDir()
	host := NewHost(fsh, mountPoint)
	host.Start(ctx)
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

	// Wait for things to get set up.
	time.Sleep(2 * time.Second)

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
		require.Contains(t, isDir.Error(), "is a directory")
	})

	t.Run("Create no-such-dir", func(t *testing.T) {
		_, err := os.Create(filepath.Join(mountPoint, "b", "test3.txt"))
		noSuchDir := &fs2.PathError{}
		require.ErrorAs(t, err, &noSuchDir)
		require.Contains(t, noSuchDir.Error(), "no such file or directory")
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
		require.Contains(t, notADir.Error(), "not a directory")
	})

	t.Run("Truncate", func(t *testing.T) {
		err := os.Truncate(filepath.Join(mountPoint, "a", "b", "test1.txt"), 0x10000)
		require.NoError(t, err)

		// Read from the directory exported by the FTP server
		test1Exported, err := os.ReadFile(filepath.Join(root, "a", "b", "test1.txt"))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(testContents[:0x10000], test1Exported))

		// Open a mounted file, truncate it, then seek to EOF and write some text
		f, err := os.OpenFile(filepath.Join(mountPoint, "a", "b", "test2.txt"), os.O_CREATE|os.O_WRONLY, 0600)
		require.NoError(t, err)
		require.NoError(t, f.Truncate(0x10000))
		msg := []byte("hello")
		_, err = f.Seek(0x10000, 0)
		require.NoError(t, err)
		_, err = f.Write(msg)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		time.Sleep(time.Millisecond)
		// Check that the text was received by the FTP server
		test2Exported, err := os.ReadFile(filepath.Join(root, "a", "b", "test2.txt"))
		require.NoError(t, err)
		copy(testContents[0x10000:], msg)
		assert.True(t, bytes.Equal(testContents[:0x10005], test2Exported))
	})

	t.Run("Remove not-empty-dir", func(t *testing.T) {
		err := os.Remove(filepath.Join(mountPoint, "a", "b"))
		notEmpty := &fs2.PathError{}
		require.ErrorAs(t, err, &notEmpty)
		require.Contains(t, notEmpty.Error(), "directory not empty")
	})

	t.Run("Remove non-existent", func(t *testing.T) {
		err := os.Remove(filepath.Join(mountPoint, "a", "nodir"))
		notEmpty := &fs2.PathError{}
		require.ErrorAs(t, err, &notEmpty)
		require.Contains(t, notEmpty.Error(), "no such file or directory")
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
		require.Contains(t, isFile.Error(), "file exists")
	})
}
