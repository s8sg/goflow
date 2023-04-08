package main

import (
	"fmt"
	"github.com/gorilla/mux"
	pageGen "html/template"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	jsonType = "application/json"
	htmlType = "text/html"
)

var (
	publicUri                   = ""
	gen       *pageGen.Template = nil
)

// initialize globals
func initialize() error {
	publicUri = os.Getenv("public_uri")
	gen = pageGen.Must(pageGen.ParseGlob("views/*.html"))
	return nil
}

func parseIntOrDurationValue(val string, fallback time.Duration) time.Duration {
	if len(val) > 0 {
		parsedVal, parseErr := strconv.Atoi(val)
		if parseErr == nil && parsedVal >= 0 {
			return time.Duration(parsedVal) * time.Second
		}
	}

	duration, durationErr := time.ParseDuration(val)
	if durationErr != nil {
		return fallback
	}
	return duration
}

func main() {

	readTimeout := parseIntOrDurationValue(os.Getenv("read_timeout"), 10*time.Second)
	writeTimeout := parseIntOrDurationValue(os.Getenv("write_timeout"), 10*time.Second)

	var err error

	err = initialize()
	if err != nil {
		log.Fatal("failed to initialize the gateway, error: ", err.Error())
	}
	log.Printf("successfully initialized gateway")

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", 8082),
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	r := mux.NewRouter()

	// Template
	r.HandleFunc("/", dashboardPageHandler)
	r.HandleFunc("/flow/info", flowInfoPageHandler)
	r.HandleFunc("/flow/requests", flowRequestsPageHandler)
	r.HandleFunc("/flow/request/monitor", flowRequestMonitorPageHandler)

	// API request
	r.HandleFunc("/api/flow/list", listFlowsHandler)
	r.HandleFunc("/api/flow/info", flowDescHandler)
	r.HandleFunc("/api/flow/requests", listFlowRequestsHandler)
	r.HandleFunc("/api/flow/request/traces", requestTracesHandler)
	r.HandleFunc("/api/flow/request/execute", executeRequestHandler)
	r.HandleFunc("/api/flow/request/pause", pauseRequestHandler)
	r.HandleFunc("/api/flow/request/resume", resumeRequestHandler)
	r.HandleFunc("/api/flow/request/stop", stopRequestHandler)

	// Static content
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./assets/static/"))))

	http.Handle("/", r)

	log.Fatal(s.ListenAndServe())
}
