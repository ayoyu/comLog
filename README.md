# ComLog

## About

### tl;dr

Commit log service in Golang similar to what Kafka is (The goal is to make it distributed in the future)

### The goal of the project

- Implement a distributed commit log service with Golang
- Learn a lot about database concurrency, commit log, distributed systems...etc And most important one having fun 😁️

What is a commitLog ? some resources to check out if you are curious:

- https://kafka.apache.org/documentation/#log
- https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html#:~:text=Commitlogs%20are%20an%20append%20only,will%20be%20applied%20to%20memtables.
- https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- https://dev.to/heroku/what-is-a-commit-log-and-why-should-you-care-pib

## Quick start

#### The commit log core library usage: [Core Library README](comLog/README.md)

- Start the server:

```shell
$ make default_server
```

- Async Producer example: Sending records with the client

```golang
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ayoyu/comLog/client"
	"go.uber.org/zap"
)

func main() {
	lg, err := zap.NewProduction()
	if err != nil {
		panic(err.Error())
	}
	defer lg.Sync()

	cli, err := client.New(
		context.TODO(),
		"localhost:50052",
		client.WithLinger(1*time.Second),
		client.WithBatchSize(40),
		client.WithLogger(lg),
	)
	if err != nil {
		panic(err.Error())
	}
	defer cli.Close()

	records := [][]byte{
		[]byte("aaaaaaaaaa"),
		[]byte("bbbbbbbbbb"),
		[]byte("cccccccccc"),
		[]byte("dddddddddd"),
		[]byte("eeeeeeeeee"),
		[]byte("ffffffffff"),
		[]byte("gggggggggg"),
		[]byte("hhhhhhhhhh"),
		[]byte("iiiiiiiiii"),
		[]byte("jjjjjjjjjj"),
		[]byte("kkkkkkkkkk"),
		[]byte("llllllllll"),
		[]byte("mmmmmmmmmm"),
		[]byte("nnnnnnnnnn"),
		[]byte("oooooooooo"),
		[]byte("pppppppppp"),
		[]byte("qqqqqqqqqq"),
		[]byte("rrrrrrrrrr"),
	}
	done := make(chan struct{}, len(records))

	callback := func(resp *client.SendAppendResponse, wait *sync.WaitGroup) {
		fmt.Println("Callback fired, Response: ", resp)

		done <- struct{}{}
		wait.Done()
	}

	fmt.Println("Start calling Send")
	for _, record := range records {
		err := cli.Send(context.TODO(), &client.Record{Data: record}, callback)

		if err != nil {
			panic(err.Error())
		}
	}
	fmt.Println("Done Calling Send")

	for i := 0; i < len(records); i++ {
		<-done
	}
}
```

## Tasks:

- [x] Write the commit log library (package)
- [x] Write the rpc service to expose the library
- [ ] Write the service discovery logic for the distribution purpose
- [ ] Write the service coordination/consensus logic
