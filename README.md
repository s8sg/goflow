# Go-Flow
A Golang based high performance, scalable and distributed workflow framework

It allows to programmatically author distributed workflows as Directed Acyclic Graphs (DAGs) of tasks. 
Goflow executes your tasks on an array of goflow workers by uniformly distribute the loads 

![Build](https://github.com/faasflow/goflow/workflows/GO-Flow-Build/badge.svg) 
[![GoDoc](https://godoc.org/github.com/faasflow/goflow?status.svg)](https://godoc.org/github.com/faasflow/goflow)

## Install It 
Install GoFlow
```sh
go mod init myflow
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
func DefineWorkflow(f *flow.Workflow, context *flow.Context) error {
	f.SyncNode().Apply("test", doSomething)
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

## Creating More Complex DAG
The initial example is a single vertex DAG.
Single vertex DAG (referred as `SyncNode`) are great for synchronous task

Using [GoFlow's DAG construct](https://godoc.org/github.com/faasflow/lib/goflow#Dag) one can achieve more complex compositions
with multiple vertexes and connect them using edges.
A multi-vertex flow is always asynchronous in nature where each nodes gets
distributed across the workers

Below is an example of a simple multi vertex flow
```go
func quote(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 1")
	quote := fmt.Sprintf("you said '%s'", string(data))
	return []byte(quote), nil
}

func capitalize(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 2")
	capitalized := strings.ToUpper(string(data))
	return []byte(capitalized), nil
}

func print(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 3")
	fmt.Println("Final Data: " + string(data))
	return data, nil
}

// This function defines the DAG
func DefineWorkflow(f *flow.Workflow, context *flow.Context) error {
	dag := f.Dag()
	dag.Node("task1").Apply("quote", quote)
	dag.Node("task2").Apply("capitalize", capitalize)
	dag.Node("task3").Apply("print", print)
	dag.Edge("task1", "task2")
	dag.Edge("task2", "task3")
	return nil
}
```

### Branching
Branching are great for parallelizing asynchronous independent workload in separate branch

Branching can be achieved with simple vertex and edges. GoFlow provides a special operator [Aggregator](https://godoc.org/github.com/faasflow/lib/goflow#Aggregator) to aggregate result of multiple branch on a converging node

Below is an example of a simple branching
```go
func quote(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 1")
	quote := fmt.Sprintf("you said '%s'", string(data))
	return []byte(quote), nil
}

func capitalize(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 2")
	capitalized := strings.ToUpper(string(data))
	return []byte(capitalized), nil
}

func lowercase(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 3")
	lower := strings.ToLower(string(data))
	return []byte(lower), nil
}

func print(data []byte, option map[string][]string) ([]byte, error) {
	fmt.Println("Executing task 4")
	fmt.Println("Final Data: " + string(data))
	return data, nil
}

// This function defines the DAG
func DefineWorkflow(f *flow.Workflow, context *flow.Context) error {
    dag := f.Dag()
    dag.Node("task1").Apply("quote", quote)
    dag.Node("task2").Apply("capitalize", capitalize)
    dag.Node("task3").Apply("lowercase", lowercase)

    // Using Aggregator to aggregate the result from different branch
    dag.Node("task4", flow.Aggregator(func(responses map[string][]byte) ([]byte, error) {
        task2Response := responses["task2"]
        task3Response := responses["task3"]
        return []byte(string(task2Response) + ", " + string(task3Response)), nil
    })).Apply("print", print)

    dag.Edge("task1", "task2")
    dag.Edge("task1", "task3")
    dag.Edge("task2", "task4")
    dag.Edge("task3", "task4")
    return nil
}
```