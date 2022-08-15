package fs

import (
	"context"
	"net/netip"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"

	"github.com/datawire/dlib/dlog"
)

type connList struct {
	conn *ftp.ServerConn
	next *connList
}

type connPool struct {
	sync.Mutex
	addr        netip.AddrPort
	readTimeout time.Duration
	idleConns   *connList
	openConns   int32
	pooledConns int32
}

// connect returns a new connection without using the pool. Use get instead of connect.
func (p *connPool) connect(ctx context.Context) (*ftp.ServerConn, error) {
	opts := []ftp.DialOption{
		ftp.DialWithContext(ctx),
	}
	if p.readTimeout > 0 {
		opts = append(opts, ftp.DialWithTimeout(p.readTimeout))
	}
	conn, err := ftp.Dial(p.addr.String(), opts...)
	if err != nil {
		return nil, err
	}
	if err = conn.Login("anonymous", "anonymous"); err != nil {
		return nil, err
	}
	return conn, nil
}

// get returns a connection from the pool, or creates a new connection if needed
func (p *connPool) get(ctx context.Context) (conn *ftp.ServerConn, err error) {
	p.Lock()
	if idle := p.idleConns; idle != nil {
		p.idleConns = idle.next
		idle.next = nil
		conn = idle.conn
		p.pooledConns--
	}
	if conn == nil {
		// Assume connect succeeds and increase while holding the lock
		p.openConns++
	}
	p.logSz(ctx, "get")
	p.Unlock()
	if conn == nil {
		if conn, err = p.connect(ctx); err != nil {
			p.Lock()
			p.openConns--
			p.Unlock()
		}
	}
	return
}

func (p *connPool) logSz(ctx context.Context, pfx string) {
	dlog.Debugf(ctx, "%s: pooled %d, open %d", pfx, p.pooledConns, p.openConns)
}

// put returns a connection to the pool
func (p *connPool) put(ctx context.Context, conn *ftp.ServerConn) {
	p.Lock()
	cl := &connList{
		conn: conn,
		next: p.idleConns,
	}
	p.idleConns = cl
	p.pooledConns++
	p.logSz(ctx, "put")
	p.Unlock()
}

// quit calls the Quit method on all connections and empties the pool
func (p *connPool) quit(ctx context.Context) {
	p.Lock()
	defer p.Unlock()
	for idle := p.idleConns; idle != nil; idle = idle.next {
		if err := idle.conn.Quit(); err != nil {
			dlog.Errorf(ctx, "quit failed: %v", err)
		}
	}
	p.idleConns = nil
	p.openConns = 0
}

// tidy will attempt to shrink the number of open connections to two, but since it
// only closes connections that have been returned at the time the call is made, there
// might still be more than 2 connections after the call returns.
func (p *connPool) tidy(ctx context.Context) {
	p.Lock()
	defer p.Unlock()
	if p.openConns <= 2 {
		return
	}
	for idle := p.idleConns; idle != nil; idle = idle.next {
		p.idleConns = idle.next
		if err := idle.conn.Quit(); err != nil {
			dlog.Errorf(ctx, "quit failed: %v", err)
		}
		p.pooledConns--
		p.openConns--
		if p.openConns <= 2 {
			break
		}
	}
}

func (p *connPool) withConn(ctx context.Context, f func(conn *ftp.ServerConn) error) error {
	conn, err := p.get(ctx)
	if err != nil {
		return err
	}
	err = f(conn)
	p.put(ctx, conn)
	return err
}
