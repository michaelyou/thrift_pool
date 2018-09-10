package pool

import (
	"sync"
	"sync/atomic"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type Connection struct {
	sync.Mutex

	cp           *connectionPool
	socket       *thrift.TSocket // use this to close this connection
	ThriftClient interface{}     // this is what we return in Get
	closed       bool
	finalClosed  bool

	// guard by cp.mu
	inUse bool

	createdAt time.Time
}

func (c *Connection) Release(err error) {
	c.cp.putConn(c, err)
}

func (c *Connection) Close() {
	withLock(c, func() {
		c.socket.Close()
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
