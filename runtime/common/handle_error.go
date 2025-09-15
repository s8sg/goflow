package common

import (
	"fmt"
	"net/http"
)

func HandleError(w http.ResponseWriter, message string) {
	errorStr := fmt.Sprintf("[ Failed ] %v\n", message)
	fmt.Printf("%s", errorStr)
	w.WriteHeader(http.StatusInternalServerError)
	if _, err := w.Write([]byte(errorStr)); err != nil {
		fmt.Printf("Error writing response: %v\n", err)
	}
}
