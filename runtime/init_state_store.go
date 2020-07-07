package runtime

import (
	redisStateStore "github.com/faasflow/faas-flow-redis-statestore"
	"github.com/faasflow/sdk"
)

func initStateStore(redisURI string) (stateStore sdk.StateStore, err error) {
	stateStore, err = redisStateStore.GetRedisStateStore(redisURI)
	return stateStore, err
}
