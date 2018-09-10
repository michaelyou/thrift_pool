package main

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"thrift_p/gen-go/tutorial"
	// "time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/michaelyou/thrift_pool"
)

var defaultCtx = context.Background()

func handleClient(client *tutorial.CalculatorClient) (err error) {
	client.Ping(defaultCtx)
	fmt.Println("ping()")

	sum, err := client.Add(defaultCtx, 1, 1)
	if err != nil {
		fmt.Println("Add, error:", err)
	}
	fmt.Print("1+1=", sum, "\n")

	work := tutorial.NewWork()
	work.Op = tutorial.Operation_DIVIDE
	work.Num1 = 1
	work.Num2 = 0
	quotient, err := client.Calculate(defaultCtx, 1, work)
	if err != nil {
		switch v := err.(type) {
		case *tutorial.InvalidOperation:
			fmt.Println("Invalid operation:", v)
		default:
			fmt.Println("Error during operation:", err)
		}
		return err
	} else {
		fmt.Println("Whoa we can divide by 0 with new value:", quotient)
	}

	work.Op = tutorial.Operation_SUBTRACT
	work.Num1 = 15
	work.Num2 = 10
	diff, err := client.Calculate(defaultCtx, 1, work)
	if err != nil {
		switch v := err.(type) {
		case *tutorial.InvalidOperation:
			fmt.Println("Invalid operation:", v)
		default:
			fmt.Println("Error during operation:", err)
		}
		return err
	} else {
		fmt.Print("15-10=", diff, "\n")
	}

	log, err := client.GetStruct(defaultCtx, 1)
	if err != nil {
		fmt.Println("Unable to get struct:", err)
		return err
	} else {
		fmt.Println("Check log:", log.Value)
	}
	return err
}

func NewClientO() (interface{}, error) {
	var transport thrift.TTransport
	var err error
	if *secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(*addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(*addr)
	}
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return nil, err
	}
	transport, err = transportFactory.GetTransport(transport)
	if err != nil {
		return nil, err
	}
	// defer transport.Close()
	if err := transport.Open(); err != nil {
		return nil, err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client := tutorial.NewCalculatorClient(thrift.NewTStandardClient(iprot, oprot))
	return client, nil
}

func NewClient(ctx context.Context, c *thrift.TStandardClient) interface{} {
	return tutorial.NewCalculatorClient(c)
}

func runClient() error {
	ctx := context.Background()
	cp, err := pool.NewConnectionPool(ctx, "127.0.0.1:9090", "binary", false, false, NewClient)
	cp.SetMaxOpenConns(10)
	cp.SetMaxIdleConns(5)
	// cp.SetConnMaxLifetime(60 * time.Second)
	if err != nil {
		fmt.Println("new connection pool error", err)
		return err
	}

	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i < 20; i++ {
		go func() {
			conn, err := cp.Get(ctx, pool.CachedOrNewConn)
			if err != nil {
				fmt.Println("get conn error", err)
			}
			client := conn.ThriftClient
			handleClient(client.(*tutorial.CalculatorClient))
			conn.Release(nil)
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func runClientN() error {
	var transport thrift.TTransport
	var err error
	if *secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(*addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(*addr)
	}
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	transport, err = transportFactory.GetTransport(transport)
	if err != nil {
		return err
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client := tutorial.NewCalculatorClient(thrift.NewTStandardClient(iprot, oprot))
	return handleClient(client)
}
