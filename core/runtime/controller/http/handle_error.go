package http

import (
	"fmt"
	"net/http"
)

func handleError(w http.ResponseWriter, message string) {
	errorStr := fmt.Sprintf("[ Failed ] %v\n", message)
	fmt.Printf(errorStr)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(errorStr))
}
