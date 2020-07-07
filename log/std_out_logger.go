package log

import (
	"fmt"
)

// implements faasflow.Logger
type StdOutLogger struct{}

func (l *StdOutLogger) Configure(flowName string, requestId string) {}

func (l *StdOutLogger) Init() error {
	return nil
}
func (l *StdOutLogger) Log(str string) {
	fmt.Print(str)
}
