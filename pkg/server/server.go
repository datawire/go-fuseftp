// Package server contains an FTP server based on github.com/fclairamb/ftpserverlib
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"sync"

	ftp "github.com/fclairamb/ftpserverlib"
	"github.com/spf13/afero"

	"github.com/datawire/dlib/dlog"
)

type user struct {
	password string
	basePath string
}

type driver struct {
	ftp.Settings
	sync.Mutex
	ctx     context.Context
	clients []ftp.ClientContext
	users   map[string]*user
}

type client struct {
	afero.Fs
	ctx context.Context
}

// GetHandle implements ftpserver.ClientDriverExtentionFileTransfer
func (c *client) GetHandle(name string, flags int, offset int64) (ftp.FileTransfer, error) {
	dlog.Debugf(c.ctx, "GetHandle(%s, %#x, %d)", name, flags, offset)
	f, err := c.OpenFile(name, flags, 0600)
	if err != nil {
		return nil, err
	}
	if flags == os.O_CREATE|os.O_WRONLY {
		if err := f.Truncate(offset); err != nil {
			return nil, err
		}
	}
	if offset > 0 {
		_, err = f.Seek(offset, 0)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func newDriver(ctx context.Context, publicHost string, users map[string]*user, portAnnounceCh chan<- uint16) (*driver, error) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	a := l.Addr().(*net.TCPAddr)

	d := &driver{
		ctx:   ctx,
		users: users,
		Settings: ftp.Settings{
			Banner:              "Telepresence Traffic Agent",
			PublicHost:          publicHost,
			DefaultTransferType: ftp.TransferTypeBinary,
			EnableHASH:          true,
			Listener:            l,
			ListenAddr:          a.String(),
			IdleTimeout:         300,
		}}

	dlog.Infof(ctx, "FTP server listening on %s", d.ListenAddr)
	portAnnounceCh <- uint16(a.Port)
	return d, nil
}

// ClientConnected keeps track of the connected client so that it is properly closed
// if the server is stopped before a call to ClientDisconnected arrives.
func (d *driver) ClientConnected(cc ftp.ClientContext) (string, error) {
	d.Lock()
	expand := true
	for i, c := range d.clients {
		if c == nil {
			d.clients[i] = cc
			expand = false
			break
		}
	}
	if expand {
		d.clients = append(d.clients, cc)
	}
	d.Unlock()
	dlog.Infof(d.ctx, "Client connected, id %d, remoteAddr %s", cc.ID(), cc.RemoteAddr())
	cc.SetDebug(dlog.MaxLogLevel(d.ctx) >= dlog.LogLevelDebug)
	return "telepresence", nil
}

func (d *driver) ClientDisconnected(cc ftp.ClientContext) {
	d.Lock()
	for i, c := range d.clients {
		if c != nil && c.ID() == cc.ID() {
			d.clients[i] = nil
			break
		}
	}
	d.Unlock()
	dlog.Infof(d.ctx, "Client disconnected, id %d, remoteAddr %s", cc.ID(), cc.RemoteAddr())
}

func (d *driver) AuthUser(ctx ftp.ClientContext, userName, password string) (ftp.ClientDriver, error) {
	user, ok := d.users[userName]
	if !(ok && user.password == "*" || user.password == password) {
		return nil, errors.New("unknown user")
	}
	return &client{Fs: afero.NewBasePathFs(SymLinkResolvingFs(afero.NewOsFs()), user.basePath), ctx: d.ctx}, nil
}

func (d *driver) GetTLSConfig() (*tls.Config, error) {
	return nil, errors.New("not enabled")
}

func (d *driver) GetSettings() (*ftp.Settings, error) {
	return &d.Settings, nil
}

func Start(ctx context.Context, publicHost string, basePath string, portAnnounceCh chan<- uint16) error {
	defer close(portAnnounceCh)
	users := map[string]*user{
		"anonymous": {
			password: "*",
			basePath: basePath,
		},
	}
	d, err := newDriver(ctx, publicHost, users, portAnnounceCh)
	if err != nil {
		return err
	}
	s := ftp.NewFtpServer(d)
	s.Logger = Logger(ctx)
	go func() {
		<-ctx.Done()
		dlog.Infof(ctx, "Stopping FTP server")
		d.Lock()
		for _, c := range d.clients {
			if c != nil {
				c.Close()
			}
		}
		d.clients = nil
		d.Unlock()
		if err := s.Stop(); err != nil {
			dlog.Errorf(ctx, "failed to stop ftp server: %v", err)
		}
	}()
	dlog.Infof(ctx, "Starting FTP server")
	return s.ListenAndServe()
}
