package runtime

import (
	redisDataStore "github.com/s8sg/goflow/core/redis-datastore"
	"github.com/s8sg/goflow/core/sdk"
)

func initDataStore(redisURI string) (dataStore sdk.DataStore, err error) {
	dataStore, err = redisDataStore.GetRedisDataStore(redisURI)
	return dataStore, err
}
