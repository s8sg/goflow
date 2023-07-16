package main

import (
	"fmt"
	"github.com/s8sg/goflow/samples/condition"
	"github.com/s8sg/goflow/samples/loop"
	"github.com/s8sg/goflow/samples/myflow"
	"github.com/s8sg/goflow/samples/parallel"
	"github.com/s8sg/goflow/samples/serial"
	"github.com/s8sg/goflow/samples/single"

	goflow "github.com/s8sg/goflow/v1"
)

func GetFlowService() *goflow.FlowService {
    fs := &goflow.FlowService{
    		Port:              8080,
    		RedisURL:          "localhost:6379",
    		RedisPassword:     "redis",
    		OpenTraceUrl:      "localhost:5775",
    		WorkerConcurrency: 5,
    		EnableMonitoring:  true,
    		DebugEnabled:      true,
    }
    fs.Register("single", single.DefineWorkflow)
    fs.Register("serial", serial.DefineWorkflow)
    fs.Register("parallel", parallel.DefineWorkflow)
    fs.Register("condition", condition.DefineWorkflow)
    fs.Register("loop", loop.DefineWorkflow)
    fs.Register("myflow", myflow.DefineWorkflow)
    return fs
}

func main() {
    fmt.Println(GetFlowService().Start())
}
