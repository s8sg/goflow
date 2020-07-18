# faas-flow-service
FaaSFlow service is a go-based workflow engine

![Build](https://github.com/faasflow/faas-flow-service/workflows/Faas-Flow-Service-Build/badge.svg)
[![GoDoc](https://godoc.org/github.com/faasflow/faas-flow-service?status.svg)](https://godoc.org/github.com/faasflow/faas-flow-service)


## Install It 
Install FaasFlow Service
```sh
go get github.com/faasflow/faas-flow-service
```

## Write First Flow
Make a `flow.go` file
```go
package main

import (
	"fmt"
	flow "github.com/faasflow/lib/service"
	service "github.com/faasflow/faas-flow-service"
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
	fs := &service.FlowService{
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
