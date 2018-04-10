package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/michaelyou/pool"
)

func createConn(ctx context.Context) (net.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	default:
		return net.Dial("tcp", "127.0.0.1:3306")
	}
}

func main() {

	// create a factory() to be used with channel based pool
	factory := createConn

	ctx := context.Background()
	p := pool.NewConnectionPool(ctx, factory)
	p.SetMaxIdleConns(5)
	p.SetConnMaxLifetime(time.Second * 10)

	// now you can get a connection from the pool, if there is no connection
	// available it will create a new one via the factory function.
	conn, err := p.Get(ctx, pool.CachedOrNewConn)
	fmt.Println(p.Len())
	if err != nil {
		log.Panic(err)
	}

	// do something with conn and put it back to the pool by closing the connection
	// (this doesn't close the underlying connection instead it's putting it back
	// to the pool).
	conn.Release(nil)
	fmt.Println(p.Len())
	_, err = p.Get(ctx, pool.CachedOrNewConn)

	// close the underlying connection instead of returning it to pool
	// it is useful when acceptor has already closed connection and conn.Write() returns error
	// if pc, ok := conn.(*pool.PoolConn); ok {
	// 	pc.MarkUnusable()
	// 	pc.Close()
	// }

	// close pool any time you want, this closes all the connections inside a pool
	p.Close()

	// currently available connections in the pool
	current := p.Len()
	fmt.Println(current)
}
