package pool

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	MaxIdle = 2
	MaxOpen = 5
	network = "tcp"
	address = "127.0.0.1:7777"
	factory = func(ctx context.Context) (net.Conn, error) { return net.Dial(network, address) }
	ctx     = context.Background()
)

func init() {
	// used for factory function
	go simpleTCPServer()
	time.Sleep(time.Millisecond * 300) // wait until tcp server has been settled

	rand.Seed(time.Now().UTC().UnixNano())
}

func TestNew(t *testing.T) {
	t.Logf("begin TestNew")
	_, err := newConnectionPool()
	if err != nil {
		t.Errorf("New error: %s", err)
	}
}

func TestPool_Get(t *testing.T) {
	t.Logf("begin TestPool_Get")
	p, _ := newConnectionPool()
	defer p.Close()

	_, err := p.Get(ctx, CachedOrNewConn)
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	// after one get
	if p.numOpen != 1 {
		t.Errorf("Get error. Expecting %d, got %d",
			1, p.numOpen)
	}

	// get them all
	var wg sync.WaitGroup
	for i := 0; i < (MaxOpen + 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := make(chan *Connection, 1)
			ticker := time.NewTicker(1 * time.Second)
			go func() {
				c, _ := p.Get(ctx, CachedOrNewConn)
				// if err != nil {
				// 	t.Errorf("Get error: %s", err)
				// }
				conn <- c
			}()
			select {
			case <-conn:
			case <-ticker.C:
			}
		}()
	}
	wg.Wait()

	if p.numOpen != MaxOpen {
		t.Errorf("Get error. Expecting %d, got %d",
			MaxOpen, p.numOpen)
	}
	// the last Get will block
}

func TestPool_Put(t *testing.T) {
	p, err := newConnectionPool()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// get/create from the pool
	conns := make([]*Connection, MaxOpen)
	for i := 0; i < MaxOpen; i++ {
		conn, _ := p.Get(ctx, CachedOrNewConn)
		conns[i] = conn
	}

	if p.numOpen != MaxOpen {
		t.Errorf("Put error len. Expecting %d, got %d",
			MaxOpen, p.numOpen)
	}

	// now put them all back
	for _, conn := range conns {
		conn.Release(nil)
	}

	if p.numOpen != MaxIdle {
		t.Errorf("Put error len. Expecting %d, got %d",
			MaxIdle, p.numOpen)
	}

	if len(p.freeConn) != MaxIdle {
		t.Errorf("Put error len. Expecting %d, got %d",
			MaxIdle, len(p.freeConn))
	}

}

func TestPoolConcurrent(t *testing.T) {
	p, _ := newConnectionPool()
	pipe := make(chan *Connection, 0)

	go func() {
		p.Close()
	}()

	for i := 0; i < MaxOpen; i++ {
		go func() {
			conn, _ := p.Get(ctx, CachedOrNewConn)

			pipe <- conn
		}()

		go func() {
			conn := <-pipe
			if conn == nil {
				return
			}
			conn.Release(nil)
		}()
	}
}

func TestPoolWriteRead(t *testing.T) {
	p, _ := newConnectionPool()

	conn, _ := p.Get(ctx, CachedOrNewConn)

	msg := "hello"
	_, err := conn.Write([]byte(msg))
	if err != nil {
		t.Error(err)
	}
}

func TestPoolConcurrent2(t *testing.T) {
	p, _ := newConnectionPool()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		p.Close()
		wg.Done()
	}()

	if conn, err := p.Get(ctx, CachedOrNewConn); err == nil {
		conn.Release(nil)
	}

	wg.Wait()
}

func newConnectionPool() (*connectionPool, error) {
	p := NewConnectionPool(ctx, factory)
	p.SetMaxIdleConns(MaxIdle)
	p.SetMaxOpenConns(MaxOpen)
	return p, nil
}

func simpleTCPServer() {
	l, err := net.Listen(network, address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			buffer := make([]byte, 256)
			conn.Read(buffer)
		}()
	}
}
