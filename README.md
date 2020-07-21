# Go-Flow
A Golang based high performance, scalable and distributed workflow framework

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
	fs.Start("my-flow", DefineWorkflow)
}
```

## Run It 
```sh
go build -o worker
./worker
```

## Invoke It
```sh
curl -d HelloWorld localhost:8080
```

## Scale It
GoFlow scale horizontally, you can distribute the load by just adding more instances.

#### Worker Mode
Alternatively you can start your goflow in worker mode. As a worker goflow only handles the workload, 
and if required you can only scale the workers 
```go
func main() {
	fs := &goflow.FlowService{
		RedisURL:            "localhost:6379",
		OpenTraceUrl:        "localhost:5775",
		WorkerConcurrency:   5,
	}
	fs.StartWorker(DefineWorkflow)
}
```

#### Server Mode
Similarly you can start your goflow as a server. As a server goflow only handles the incoming http requests,
and you will need to run workers to handle the workload
```go
func main() {
	fs := &goflow.FlowService{
	        Port:                8080,
		RedisURL:            "localhost:6379",
		OpenTraceUrl:        "localhost:5775",
		WorkerConcurrency:   5,
	}
	fs.StartWorker(DefineWorkflow)
}
```

> By default goflow runs both as a Server and a Worker mode
