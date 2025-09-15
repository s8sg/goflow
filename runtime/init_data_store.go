package runtime

import (
	redisDataStore "github.com/s8sg/goflow/core/redis-datastore"
	"github.com/s8sg/goflow/core/sdk"
)

func initDataStore(redisURI string, password string, db int) (dataStore sdk.DataStore, err error) {
	dataStore, err = redisDataStore.GetRedisDataStore(redisURI, password, db)
	return dataStore, err
}
