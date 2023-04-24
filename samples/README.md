# Samples
This section list a few example that demonstrates different patterns in goflow.  

Examples:

| **Name**                            | **Description**         |
|-------------------------------------|:------------------------|
| [single](single/single.go)          | Single node             |
| [serial](serial/serial.go)          | Serial sequential nodes |
| [parallel](parallel/parallel.go)    | Parallel nodes          |
| [condition](condition/condition.go) | Conditional nodes       |
| [loop](loop/loop.go)                | Foreach loop nodes      |


## How to run
Build
```bash
make
```
Run dependencies
```bash
docker-compose down && docker-compose up -d
```
Run flow
```
./examples
```

## How to execute
Execute with curl
```bash
curl -d hello localhost:8080/<name>
```
For example to execute `parallel` flow
```bash
curl -d hello localhost:8080/parallel
```
