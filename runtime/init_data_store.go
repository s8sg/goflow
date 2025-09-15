package runtime

import (
	datastore "github.com/s8sg/goflow/core/datastore"
	"github.com/s8sg/goflow/core/sdk"
)

func initDataStore(redisURI string, password string) (dataStore sdk.DataStore, err error) {
	dataStore, err = datastore.GetDatastore(redisURI, password)
	return dataStore, err
}
