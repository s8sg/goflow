package runtime

import (
	"github.com/s8sg/goflow/core/sdk"
	statestore "github.com/s8sg/goflow/core/statestore"
)

func initStateStore(redisURI string, password string) (stateStore sdk.StateStore, err error) {
	stateStore, err = statestore.GetStateStore(redisURI, password)
	return stateStore, err
}
