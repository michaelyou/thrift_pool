package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var nowFunc = time.Now

// connectionPool implements the Pool interface based on buffered channels.
type connectionPool struct {
	numClosed uint64

	// thrift conf
	addrs            string // server地址，逗号分隔，eg: 127.0.0.1:2000,127.0.0.2:3000
	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory

	mu           sync.Mutex // protects following fields
	freeConn     []*Connection
	connRequests map[uint64]chan connRequest
	nextRequest  uint64
	numOpen      int

	openerCh chan struct{}
	closed   bool

	maxIdle     int
	maxOpen     int
	maxLifetime time.Duration
	cleanerCh   chan struct{}

	stop func()

	// connection generator
	factory Factory
}

type connRequest struct {
	conn *Connection
	err  error
}

// Factory is a function to create new connections.
type Factory func(ctx context.Context, c *thrift.TStandardClient) interface{}

// This is the size of the connectionOpener request chan (connectionPool.openerCh).
// This value should be larger than the maximum typical value
// used for cp.maxOpen. If maxOpen is significantly larger than
// connectionRequestQueueSize then it is possible for ALL calls into the *connectionPoll
// to block until the connectionOpener can satisfy the backlog of requests.
var connectionRequestQueueSize = 1000000

var errPoolClosed = errors.New("connection pool closed")
var ErrBadConn = errors.New("bad connection")

type connReuseStrategy uint8

const (
	AlwaysNewConn connReuseStrategy = iota
	CachedOrNewConn
)

/*
@addrs: server地址，逗号分隔，eg: 127.0.0.1:2000,127.0.0.2:3000
@protocol: Specify the protocol (binary, compact, json, simplejson)
@frame: Use framed transport
@buffered: Use buffered transport
*/
func NewConnectionPool(ctx context.Context, addrs, protocol string, frame, buffered bool, factory Factory) (*connectionPool, error) {
	var protocolFactory thrift.TProtocolFactory
	switch protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	default:
		return nil, errors.New("Invalid protocol specified, support (binary, compact, json, simplejson)")
	}

	ctx, cancel := context.WithCancel(ctx)
	var transportFactory thrift.TTransportFactory
	if buffered {
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	} else {
		transportFactory = thrift.NewTTransportFactory()
	}
	if frame {
		transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	}

	cp := &connectionPool{
		openerCh:         make(chan struct{}, connectionRequestQueueSize),
		connRequests:     make(map[uint64]chan connRequest),
		stop:             cancel,
		factory:          factory,
		addrs:            addrs,
		protocolFactory:  protocolFactory,
		transportFactory: transportFactory,
	}

	go cp.connectionOpener(ctx)
	// go cp.monitorConn()

	return cp, nil
}

func (cp *connectionPool) monitorConn() {
	for _ = range time.Tick(time.Millisecond * 1) {
		fmt.Printf("numclosed:%d, numOpen:%d, numFree:%d", cp.numClosed, cp.numOpen, len(cp.freeConn))
	}
}

func (cp *connectionPool) newConnection(ctx context.Context) (*Connection, error) {
	addrs := strings.Split(cp.addrs, ",")
	if len(addrs) == 0 {
		return nil, errors.New("thrift server addrs shouldn't be empty")
	}
	addr := addrs[rand.Intn(len(addrs))]
	socket, err := thrift.NewTSocket(addr)

	if err != nil {
		return nil, fmt.Errorf("Error opening socket: %s", err)
	}
	transport, err := cp.transportFactory.GetTransport(socket)
	if err != nil {
		return nil, err
	}
	if err := transport.Open(); err != nil {
		return nil, err
	}
	iprot := cp.protocolFactory.GetProtocol(transport)
	oprot := cp.protocolFactory.GetProtocol(transport)
	return &Connection{
		cp:           cp,
		ThriftClient: cp.factory(ctx, thrift.NewTStandardClient(iprot, oprot)),
		socket:       socket,
		createdAt:    nowFunc(),
		inUse:        true}, nil
}

func (cp *connectionPool) Get(ctx context.Context, strategy connReuseStrategy) (*Connection, error) {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return nil, errPoolClosed
	}
	select {
	default:
	case <-ctx.Done():
		cp.mu.Unlock()
		return nil, ctx.Err()
	}
	lifetime := cp.maxLifetime
	numFree := len(cp.freeConn)
	if strategy == CachedOrNewConn && numFree > 0 {
		conn := cp.freeConn[0]
		copy(cp.freeConn, cp.freeConn[1:])
		cp.freeConn = cp.freeConn[:numFree-1]

		conn.inUse = true
		cp.mu.Unlock()
		if lifetime != 0 && conn.IsExpired(lifetime) {
			conn.Close()
			return nil, ErrBadConn
		}
		return conn, nil
	}

	if cp.maxOpen > 0 && cp.numOpen >= cp.maxOpen {
		req := make(chan connRequest, 1)
		reqKey := cp.nextRequestKeyLocked()
		cp.connRequests[reqKey] = req
		cp.mu.Unlock()

		select {
		case <-ctx.Done():
			cp.mu.Lock()
			delete(cp.connRequests, reqKey)
			cp.mu.Unlock()
			select {
			default:
			case ret, ok := <-req:
				if ok {
					cp.putConn(ret.conn, ret.err)
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-req:
			if !ok {
				return nil, errPoolClosed
			}
			if ret.err == nil && lifetime != 0 && ret.conn.IsExpired(lifetime) {
				ret.conn.Close()
				return nil, ErrBadConn
			}
			return ret.conn, ret.err
		}
	}

	// 没有设置maxOpen时，maxOpen=0，也会走到这里
	cp.numOpen++
	cp.mu.Unlock()
	ci, err := cp.newConnection(ctx)
	if err != nil {
		cp.mu.Lock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		cp.mu.Unlock()
		return nil, err
	}
	return ci, nil
}

func (cp *connectionPool) Close() error {
	cp.mu.Lock()
	if cp.closed { // Close方法是幂等的
		cp.mu.Unlock()
		return nil
	}
	close(cp.openerCh)
	if cp.cleanerCh != nil {
		close(cp.cleanerCh)
	}
	var err error
	fns := make([]func(), 0, len(cp.freeConn))
	for _, conn := range cp.freeConn {
		fns = append(fns, conn.Close)
	}
	cp.freeConn = nil
	cp.closed = true
	for _, req := range cp.connRequests {
		close(req)
	}
	cp.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
	return err
}

func (cp *connectionPool) nextRequestKeyLocked() uint64 {
	next := cp.nextRequest
	cp.nextRequest++
	return next
}

func (cp *connectionPool) maybeOpenNewConnections() {
	numRequests := len(cp.connRequests)
	if cp.maxOpen > 0 {
		numCanOpen := cp.maxOpen - cp.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		cp.numOpen++
		numRequests--
		if cp.closed {
			return
		}
		cp.openerCh <- struct{}{}
	}
}

func (cp *connectionPool) connectionOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.openerCh:
			cp.openNewConnection(ctx)
		}
	}
}

func (cp *connectionPool) openNewConnection(ctx context.Context) {
	// maybeOpenNewConnctions has already executed cp.numOpen++ before it sent
	// on cp.openerCh. This function must execute cp.numOpen-- if the
	// connection fails or is closed before returning.
	ci, err := cp.newConnection(ctx)
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		if err == nil {
			ci.Close()
		}
		cp.numOpen--
		return
	}
	if err != nil {
		cp.numOpen--
		cp.putConnLocked(nil, err)
		cp.maybeOpenNewConnections()
		return
	}

	if !cp.putConnLocked(ci, err) {
		cp.numOpen--
		ci.Close()
	}
}

func (cp *connectionPool) putConn(conn *Connection, err error) {
	cp.mu.Lock()
	if !conn.inUse {
		panic("sql: connection returned that was never out")
	}
	conn.inUse = false

	if err == ErrBadConn {
		// Don't reuse bad connections.
		// Since the conn is considered bad and is being discarded, treat it
		// as closed. Don't decrement the open count here, finalClose will
		// take care of that.
		cp.maybeOpenNewConnections()
		cp.mu.Unlock()
		// 这里最终减掉numOpen
		conn.Close()
		return
	}
	added := cp.putConnLocked(conn, nil)
	cp.mu.Unlock()

	if !added {
		conn.Close()
	}
}

// 如果某个方法当且仅当会在加锁的情况下被调用，那么就会给这个方法加上Locked的后缀
func (cp *connectionPool) putConnLocked(conn *Connection, err error) bool {
	if cp.closed {
		return false
	}
	if cp.maxOpen > 0 && cp.numOpen > cp.maxOpen {
		return false
	}

	if c := len(cp.connRequests); c > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range cp.connRequests {
			// 从map中取出一条，所以立刻break
			break
		}
		delete(cp.connRequests, reqKey)
		if err == nil {
			conn.inUse = true
		}
		req <- connRequest{conn: conn, err: err}
		return true
	} else if err == nil && !cp.closed && cp.maxIdleConnsLocked() > len(cp.freeConn) {
		cp.freeConn = append(cp.freeConn, conn)
		// 启动协程定时检查feeConn中是否有过期连接，有则剔除
		cp.startCleanerLocked()
		return true
	}
	return false
}

func (cp *connectionPool) startCleanerLocked() {
	if cp.maxLifetime > 0 && cp.numOpen > 0 && cp.cleanerCh == nil {
		cp.cleanerCh = make(chan struct{}, 1)
		go cp.connectionCleaner(cp.maxLifetime)
	}
}

// 定时检查过期连接
func (cp *connectionPool) connectionCleaner(d time.Duration) {
	const minInterval = time.Second
	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-cp.cleanerCh: // maxLifetime was changed or cp was closed.
		}

		cp.mu.Lock()
		d = cp.maxLifetime
		if cp.closed || cp.numOpen == 0 || d <= 0 {
			cp.cleanerCh = nil
			cp.mu.Unlock()
			return
		}

		expiredSince := nowFunc().Add(-d)
		var closing []*Connection
		for i := 0; i < len(cp.freeConn); i++ {
			c := cp.freeConn[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(cp.freeConn) - 1
				cp.freeConn[i] = cp.freeConn[last]
				cp.freeConn[last] = nil
				cp.freeConn = cp.freeConn[:last]
				// 因为上面调整了cp.freeConn，所以要控制i
				i--
			}
		}
		cp.mu.Unlock()

		for _, c := range closing {
			c.Close()
		}

		if d < minInterval {
			d = minInterval
		}
		t.Reset(d)
	}
}

func (cp *connectionPool) Len() int {
	return cp.numOpen
}

var defaultMaxIdleConns = 2

func (cp *connectionPool) maxIdleConnsLocked() int {
	n := cp.maxIdle
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}

func (cp *connectionPool) SetMaxIdleConns(n int) {
	cp.mu.Lock()
	if n > 0 {
		cp.maxIdle = n
	} else {
		cp.maxIdle = -1
	}

	if cp.maxOpen > 0 && cp.maxIdleConnsLocked() > cp.maxOpen {
		cp.maxIdle = cp.maxOpen
	}

	var closing []*Connection
	idleCount := len(cp.freeConn)
	maxIdle := cp.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = cp.freeConn[maxIdle:]
		cp.freeConn = cp.freeConn[:maxIdle]
	}
	cp.mu.Unlock()
	for _, c := range closing {
		c.Close()
	}
}

func (cp *connectionPool) SetMaxOpenConns(n int) {
	cp.mu.Lock()
	cp.maxOpen = n
	if n < 0 {
		cp.maxOpen = 0
	}
	syncMaxIdle := cp.maxOpen > 0 && cp.maxIdleConnsLocked() > cp.maxOpen
	cp.mu.Unlock()
	if syncMaxIdle {
		cp.SetMaxIdleConns(n)
	}
}

func (cp *connectionPool) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	cp.mu.Lock()

	if d > 0 && d < cp.maxLifetime && cp.cleanerCh != nil {
		select {
		case cp.cleanerCh <- struct{}{}:
		default:
		}
	}
	cp.maxLifetime = d
	cp.startCleanerLocked()
	cp.mu.Unlock()
}
