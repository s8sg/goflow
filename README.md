# Go-Flow
A Golang based high performance, scalable and distributed workflow framework

It allows to programmatically author distributed workflows as Directed Acyclic Graphs (DAGs) of tasks. 
Goflow executes your tasks on an array of goflow workers by uniformly distribute the loads 

![Build](https://github.com/faasflow/goflow/workflows/GO-Flow-Build/badge.svg) 
[![GoDoc](https://godoc.org/github.com/faasflow/goflow?status.svg)](https://godoc.org/github.com/faasflow/goflow)

## Install It 
Install GoFlow
```sh
go mod init test
go get github.com/faasflow/goflow
```

## Write First Flow
> Library to Build Flow `github.com/faasflow/lib/goflow`

[![GoDoc](https://godoc.org/github.com/faasflow/lib/goflow?status.svg)](https://godoc.org/github.com/faasflow/lib/goflow)

Make a `flow.go` file
```go
package main

import (
	"fmt"
	"github.com/faasflow/goflow"
	flow "github.com/faasflow/lib/goflow"
)

// Workload function
func doSomething(data []byte, option map[string][]string) ([]byte, error) {
	return []byte(fmt.Sprintf("you said \"%s\"", string(data))), nil
}

// Define provide definition of the workflow
func DefineWorkflow(flow *flow.Workflow, context *flow.Context) error {
	flow.SyncNode().Apply("test", doSomething)
	return nil
}

func main() {
	fs := &goflow.FlowService{
		Port:                8080,
		RedisURL:            "localhost:6379",
		OpenTraceUrl:        "localhost:5775",
		WorkerConcurrency:   5,
	}
	fs.Start("myflow", DefineWorkflow)
}
```
> `Start()` runs a HTTP Server that listen on the provided port and as a flow worker that handles the workload

## Run It 
Start redis
```sh
docker run --name redis -p 5775:5775 -p 6379:6379 -d redis
```

Run the Flow
```sh
go build -o goflow
./goflow
```

## Invoke It
```sh
curl -d hallo localhost:8080
```

## Scale It
GoFlow scale horizontally, you can distribute the load by just adding more instances.

#### Worker Mode
Alternatively you can start your goflow in worker mode. As a worker goflow only handles the workload, 
and if required you can only scale the workers 
```go
fs := &goflow.FlowService{
    RedisURL:            "localhost:6379",
    OpenTraceUrl:        "localhost:5775",
    WorkerConcurrency:   5,
}
fs.StartWorker("myflow", DefineWorkflow)
```

#### Server Mode
Similarly you can start your goflow as a server. It only handles the incoming http requests you will 
need to add workers to distribute the workload
```go
fs := &goflow.FlowService{
    Port:                8080,
    RedisURL:            "localhost:6379",
}
fs.StartServer("myflow", DefineWorkflow)
```

## Execute It

#### Using Client
Using the client you can requests the flow directly without starting a http server. 
The requests are always async and gets queued for the worker to pick up
```go
fs := &goflow.FlowService{
    RedisURL: "localhost:6379",
}
fs.Execute("myflow", &goflow.Request{
    Body: []byte("hallo")
})
```

#### Using Resque/Ruby
For testing, it is helpful to use the redis-cli program to insert jobs onto the Redis queue:
```go
redis-cli -r 100 RPUSH resque:queue:myflow '{"class":"GoFlow","args":["hallo"]}'
```
this will insert 100 jobs for the `GoFlow` worker onto the `myflow` queue. It is equivalent to
```ruby
class GoFlow
  @queue = :myflow    # Flow name
end

100.times do
  Resque.enqueue GoFlow, ['hallo']
end
```
> Currently Resque based job only take one argument as string
