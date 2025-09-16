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

func main() {
	fs := &goflow.FlowService{
		Port:              8080,
		RedisURL:          "localhost:6379",
		RedisPassword:     "redis",
		OpenTraceUrl:      "localhost:5775",
		WorkerConcurrency: 5,
		EnableMonitoring:  true,
		DebugEnabled:      true,
	}
	if err := fs.Register("single", single.DefineWorkflow); err != nil {
		fmt.Println("Error registering single workflow:", err)
	}
	if err := fs.Register("serial", serial.DefineWorkflow); err != nil {
		fmt.Println("Error registering serial workflow:", err)
	}
	if err := fs.Register("parallel", parallel.DefineWorkflow); err != nil {
		fmt.Println("Error registering parallel workflow:", err)
	}
	if err := fs.Register("condition", condition.DefineWorkflow); err != nil {
		fmt.Println("Error registering condition workflow:", err)
	}
	if err := fs.Register("loop", loop.DefineWorkflow); err != nil {
		fmt.Println("Error registering loop workflow:", err)
	}
	if err := fs.Register("myflow", myflow.DefineWorkflow); err != nil {
		fmt.Println("Error registering myflow workflow:", err)
	}
	fmt.Println(fs.Start())
}
