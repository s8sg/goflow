package common

import (
	"fmt"
	"net/http"
)

func HandleError(w http.ResponseWriter, message string) {
	errorStr := fmt.Sprintf("[ Failed ] %v\n", message)
	fmt.Printf(errorStr)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(errorStr))
}
