package runtime

import (
	redisDataStore "github.com/faasflow/faas-flow-redis-datastore"
	"github.com/faasflow/sdk"
)

func initDataStore(redisURI string) (dataStore sdk.DataStore, err error) {
	dataStore, err = redisDataStore.GetRedisDataStore(redisURI)
	return dataStore, err
}
