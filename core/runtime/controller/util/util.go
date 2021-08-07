package util

import (
	"net/url"
	"strings"
)

const (
	CallbackUrlHeader   = "X-Faas-Flow-Callback-Url"
	RequestIdHeader     = "X-Faas-Flow-Reqid"
	AuthSignatureHeader = "X-Hub-Signature"
)

// IsDagExportRequest check if dag export request
func IsDagExportRequest(query string) bool {
	values, err := url.ParseQuery(query)
	if err != nil {
		return false
	}

	if strings.ToUpper(values.Get("export-dag")) == "TRUE" {
		return true
	}
	return false
}

// GetStateRequestID check if state request and return the requestID
func GetStateRequestID(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}

	return values.Get("state")
}

// GetStopRequestID check if stop request and return the requestID
func GetStopRequestID(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}

	return values.Get("stop-flow")
}

// GetPauseRequestID check if pause request and return the requestID
func GetPauseRequestID(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}

	return values.Get("pause-flow")
}

// GetResumeRequestID check if resume request and return the requestID
func GetResumeRequestID(query string) string {
	values, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}

	return values.Get("resume-flow")
}
