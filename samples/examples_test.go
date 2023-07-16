package main

import (
"testing"
"net/http"
"github.com/stretchr/testify/assert"
"time"
"log"
)



func runExamplesServer() {
    go func() {
      fs := GetFlowService()
      log.Fatalf(fs.Start().Error())
    }()
}

func waitForServer() {
    backoff := 50 * time.Millisecond
    log.Printf("waiting for server to start")
    for i := 0; i < 10; i++ {
        // TODO: Add health check endpoint
        _, err := http.Get("http://localhost:8080/flow/single")
        if err != nil {
            time.Sleep(backoff)
            continue
        }
        log.Printf("server started")
        return
    }
    log.Fatalf("Server on localhost:8080 not up after 10 attempts")
}


func Test_single_flow_execution(t *testing.T) {
	runExamplesServer()
	waitForServer()

	t.Run("it should return 200 when executing single flow", func(t *testing.T) {
		resp, err := http.Get("http://localhost:8080/flow/single")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		assert.Equal(t, 200, resp.StatusCode)
	})
}