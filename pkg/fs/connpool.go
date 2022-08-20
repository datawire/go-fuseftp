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

// quit calls the Quit method on all connections and empties the pool
func (cl *connList) quit(ctx context.Context, silent bool) {
	for c := cl; c != nil; c = c.next {
		if err := c.conn.Quit(); err != nil && !silent {
			dlog.Errorf(ctx, "quit failed: %v", err)
		}
	}
}

func (cl *connList) size() int {
	sz := 0
	for n := cl; n != nil; n = n.next {
		sz++
	}
	return sz
}

type connPool struct {
	sync.Mutex
	addr        netip.AddrPort
	dir         string
	readTimeout time.Duration
	idleList    *connList
	busyList    *connList
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
	if p.dir != "" {
		if err = conn.ChangeDir(p.dir); err != nil {
			return nil, err
		}
	}
	// and add first in busyList
	cl := &connList{
		conn: conn,
		next: p.busyList,
	}
	p.busyList = cl
	return conn, nil
}

// get returns a connection from the pool, or creates a new connection if needed
func (p *connPool) get(ctx context.Context) (conn *ftp.ServerConn, err error) {
	p.Lock()
	if idle := p.idleList; idle != nil {
		p.idleList = idle.next
		idle.next = p.busyList
		p.busyList = idle
		conn = idle.conn
	}
	p.logSz(ctx, "get")
	p.Unlock()
	if conn == nil {
		conn, err = p.connect(ctx)
	}
	return
}

func (p *connPool) logSz(ctx context.Context, pfx string) {
	iz := p.idleList.size()
	dlog.Debugf(ctx, "%s: pooled %d, open %d", pfx, iz, iz+p.busyList.size())
}

// setAddr will call Quit on all open connections, both busy and idle, change
// the address, reconnect one connection, and put it in the idle list.
func (p *connPool) setAddr(ctx context.Context, addr netip.AddrPort) error {
	p.Lock()
	if p.addr == addr {
		p.Unlock()
		return nil
	}
	p.doQuit(ctx, true) // attempt to clear current connections, but ignore errors
	p.addr = addr
	p.Unlock()

	// Create the first connection up front, so that a failure to connect to the server is caught early
	conn, err := p.connect(ctx)
	if err != nil {
		return err
	}
	p.put(ctx, conn)
	return nil
}

// put returns a connection to the pool
func (p *connPool) put(ctx context.Context, conn *ftp.ServerConn) {
	p.Lock()
	// remove from busyList
	removed := false
	var prev *connList
	for c := p.busyList; c != nil; c = c.next {
		if c.conn == conn {
			if prev == nil {
				p.busyList = c.next
			} else {
				prev.next = c.next
			}
			removed = true
			break
		}
		prev = c
	}

	// we only add to the idleList if it was removed from the busyList because a call
	// to quit() might call Quit() a busy conn, which may result in a subsequent attempt
	// to return it to the pool.
	if removed {
		// and add first in idleList
		cl := &connList{
			conn: conn,
			next: p.idleList,
		}
		p.idleList = cl
		p.logSz(ctx, "put")
	}
	p.Unlock()
}

// quit calls the Quit method on all connections and empties the pool
func (p *connPool) doQuit(ctx context.Context, silent bool) {
	p.idleList.quit(ctx, silent)
	p.busyList.quit(ctx, silent)
	p.idleList = nil
	p.busyList = nil
}

// quit calls the Quit method on all connections and empties the pool
func (p *connPool) quit(ctx context.Context) {
	p.Lock()
	defer p.Unlock()
	p.doQuit(ctx, false)
}

// tidy will attempt to shrink the number of open connections to two, but since it
// only closes connections that are idle at the time the call is made, there
// might still be more than 2 connections after the call returns.
func (p *connPool) tidy(ctx context.Context) {
	p.Lock()
	defer p.Unlock()
	iz := p.idleList.size()
	bz := p.busyList.size()
	sz := iz + bz
	for idle := p.idleList; idle != nil && sz > 2; idle = idle.next {
		p.idleList = idle.next
		if err := idle.conn.Quit(); err != nil {
			dlog.Errorf(ctx, "quit failed: %v", err)
		}
		sz--
	}
	var prev *connList
	for idle := p.idleList; idle != nil; idle = idle.next {
		if err := idle.conn.NoOp(); err != nil {
			if prev == nil {
				p.idleList = idle.next
			} else {
				prev.next = idle.next
			}
			dlog.Errorf(ctx, "NoOp failed, dropping connection to %s: %v", p.addr, err)
			_ = idle.conn.Quit()
		}
		prev = idle
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
