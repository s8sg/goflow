# goflow-dashboard
A dashboard to visualize and monitor goflow

### Build it
```go
git clone https://github.com/s8sg/goflow-dashboard.git
go build -o goflow-dashboard
```

### Run it
```go
./goflow-dashboard
```
Default redis url is `localhost:6379`, to update set the env `REDIS_URL`. 

Default opentrace url is `http://localhost:16686/`, to update set the env `TRACE_URL`
