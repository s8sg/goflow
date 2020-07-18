# Go-Flow
A Golang based workflow framework

![Build](https://github.com/faasflow/faas-flow-service/workflows/GO-Flow-Build/badge.svg)
[![GoDoc](https://godoc.org/github.com/faasflow/faas-flow-service?status.svg)](https://godoc.org/github.com/faasflow/faas-flow-service)

## Install It 
Install GoFlow
```sh
go mod init test
go get github.com/faasflow/goflow
```

## Write First Flow
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
	fs.Start(DefineWorkflow)
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
GoFlow scale horizontally, you can distribute the load by just adding more instances 
