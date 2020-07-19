package log

import (
	"log"
)

// implements Logger
type StdErrLogger struct{}

func (l *StdErrLogger) Configure(flowName string, requestId string) {}

func (l *StdErrLogger) Init() error {
	return nil
}
func (l *StdErrLogger) Log(str string) {
	log.Print(str)
}
