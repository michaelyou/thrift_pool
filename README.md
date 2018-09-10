# Pool [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/michaelyou/thrift_pool) [![Build Status](http://img.shields.io/travis/fatih/pool.svg?style=flat-square)](https://travis-ci.org/michael/thrift_pool)


实现了一个thrift client连接池

## 安装 

```bash
go get github.com/michaelyou/thrift_pool
```

## Example

```go
import (
    "github.com/michaelyou/thrift_pool"
    "thrift_practise/gen-go/tutorial"
)

// 创建一个函数用来生成需要的thrift client
func NewClient(ctx context.Context, c *thrift.TStandardClient) interface{} {
    return tutorial.NewCalculatorClient(c)
}

ctx := context.Background()
cp, err := pool.NewConnectionPool(ctx, "127.0.0.1:9090", "binary", false, false, NewClient)
// 意义同database/sql
cp.SetMaxOpenConns(10)
cp.SetMaxIdleConns(5)
// cp.SetConnMaxLifetime(60 * time.Second) 可以设置连接的过期时间，过期的连接将被回收，生成新的连接，如果不设置，将不对连接的过期进行检查
if err != nil {
	fmt.Println("new connection pool error", err)
	return err
}

// 取出一个连接
conn, err := cp.Get(ctx, pool.CachedOrNewConn)
if err != nil {
	fmt.Println("get conn error", err)
}
// 从conn拿出client，client是一个interface{}，需要type assertions
client := conn.ThriftClient
handleClient(client.(*tutorial.CalculatorClient))
// 千万不要忘记将连接重新放回池中
conn.Release(nil)

## License

The MIT License (MIT) - see LICENSE for more details
