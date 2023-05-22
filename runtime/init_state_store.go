package runtime

import (
	redisStateStore "github.com/s8sg/goflow/core/redis-statestore"
	"github.com/s8sg/goflow/core/sdk"
)

func initStateStore(redisURI string, password string) (stateStore sdk.StateStore, err error) {
	stateStore, err = redisStateStore.GetRedisStateStore(redisURI, password)
	return stateStore, err
}
