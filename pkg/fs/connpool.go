package fs

import (
	"context"
	"net"
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

type timedConn struct {
	net.Conn
	timeout time.Duration
}

func (t timedConn) Read(b []byte) (n int, err error) {
	if err := t.SetReadDeadline(time.Now().Add(t.timeout)); err != nil {
		return 0, err
	}
	return t.Conn.Read(b)
}

func (t timedConn) Write(b []byte) (n int, err error) {
	if err := t.SetWriteDeadline(time.Now().Add(t.timeout)); err != nil {
		return 0, err
	}
	return t.Conn.Write(b)
}

func (cl *connList) conns() []*ftp.ServerConn {
	sz := cl.size()
	if sz == 0 {
		return nil
	}
	cs := make([]*ftp.ServerConn, sz)
	i := 0
	for c := cl; c != nil; c = c.next {
		cs[i] = c.conn
		i++
	}
	return cs
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
	addr     netip.AddrPort
	dir      string
	timeout  time.Duration
	idleList *connList
	busyList *connList
}

// connect returns a new connection without using the pool. Use get instead of connect.
func (p *connPool) connect(ctx context.Context) (*ftp.ServerConn, error) {
	opts := []ftp.DialOption{
		ftp.DialWithContext(ctx),
	}
	if p.timeout > 0 {
		opts = append(opts,
			ftp.DialWithTimeout(p.timeout),
			ftp.DialWithShutTimeout(p.timeout),
			ftp.DialWithDialFunc(func(network, address string) (net.Conn, error) {
				conn, err := net.DialTimeout(network, address, p.timeout)
				if err != nil {
					return nil, err
				}
				return &timedConn{Conn: conn, timeout: p.timeout}, nil
			}))
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
	p.Lock()
	cl := &connList{
		conn: conn,
		next: p.busyList,
	}
	p.busyList = cl
	p.Unlock()
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
	p.Unlock()
	if conn == nil {
		conn, err = p.connect(ctx)
	}
	return
}

// setAddr will call Quit on all open connections, both busy and idle, change
// the address, reconnect one connection, and put it in the idle list.
func (p *connPool) setAddr(ctx context.Context, addr netip.AddrPort) error {
	var idle []*ftp.ServerConn
	var busy []*ftp.ServerConn
	p.Lock()
	eq := p.addr == addr
	if !eq {
		idle = p.idleList.conns()
		busy = p.busyList.conns()
		p.idleList = nil
		p.busyList = nil
		p.addr = addr
	}
	p.Unlock()
	if eq {
		return nil
	}
	closeList(ctx, idle, true)
	closeList(ctx, busy, true)

	// Create the first connection up front, so that a failure to connect to the server is caught early
	conn, err := p.connect(ctx)
	if err != nil {
		return err
	}
	// return the connection to the pool
	p.put(ctx, conn)
	return nil
}

// put returns a connection to the pool
func (p *connPool) put(ctx context.Context, conn *ftp.ServerConn) {
	// remove from busyList
	p.Lock()
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
	}
	p.Unlock()
}

func closeList(ctx context.Context, conns []*ftp.ServerConn, silent bool) {
	for _, c := range conns {
		if err := c.Quit(); err != nil && !silent {
			dlog.Errorf(ctx, "quit failed: %v", err)
		}
	}
}

// quit calls the Quit method on all connections and empties the pool
func (p *connPool) quit(ctx context.Context) {
	p.Lock()
	idle := p.idleList.conns()
	busy := p.idleList.conns()
	p.idleList = nil
	p.busyList = nil
	p.Unlock()
	closeList(ctx, idle, false)
	closeList(ctx, busy, false)
}

// tidy will attempt to shrink the number of open connections to two, but since it
// only closes connections that are idle at the time the call is made, there
// might still be more than 2 connections after the call returns.
func (p *connPool) tidy(ctx context.Context) {
	p.Lock()
	idle := p.idleList.conns()
	idleCount := 64 - p.busyList.size()
	var cl []*ftp.ServerConn
	if idleCount > 0 && len(idle) > idleCount {
		cl = idle[idleCount:]
		p.idleList = nil
		for _, c := range idle[:idleCount] {
			p.idleList = &connList{conn: c, next: p.idleList}
		}
	}
	p.Unlock()
	closeList(ctx, cl, false)
}
