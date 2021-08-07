package runtime

import (
	"github.com/s8sg/goflow/core/sdk/executor"
)

type Runtime interface {
	Init() error
	CreateExecutor(*Request) (executor.Executor, error)
}