package pool

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	cp        *connectionPool
	createdAt time.Time

	sync.Mutex
	net.Conn
	closed      bool
	finalClosed bool

	// guard by cp.mu
	inUse bool
}

func (c *Connection) Release(err error) {
	c.cp.putConn(c, err)
}

func (c *Connection) Close() {
	withLock(c, func() {
		c.finalClosed = true
		c.cp.mu.Lock()
		c.cp.numOpen--
		c.cp.maybeOpenNewConnections()
		c.cp.mu.Unlock()

		atomic.AddUint64(&c.cp.numClosed, 1)
	})
}

func (c *Connection) IsExpired(timeout time.Duration) bool {
	if timeout < 0 {
		return false
	}
	return c.createdAt.Add(timeout).Before(nowFunc())
}

func withLock(lk sync.Locker, fn func()) {
	lk.Lock()
	defer lk.Unlock()
	fn()
}
