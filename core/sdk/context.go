package sdk

import (
	"encoding/json"
	"fmt"
	"net/url"
)

// Context execution context and execution state
type Context struct {
	requestId string     // the request id
	node      string     // the execution position
	dataStore DataStore  // underline DataStore
	Query     url.Values // provides request Query
	State     string     // state of the request
	Name      string     // name of the faas-flow

	NodeInput map[string][]byte // stores inputs form each node
}

const (
	// StateSuccess denotes success state
	StateSuccess = "success"
	// StateFailure denotes failure state
	StateFailure = "failure"
	// StateOngoing denotes ongoing state
	StateOngoing = "ongoing"
)

// CreateContext create request context (used by template)
func CreateContext(id string, node string, name string,
	dstore DataStore) *Context {

	context := &Context{}
	context.requestId = id
	context.node = node
	context.Name = name
	context.State = StateOngoing
	context.dataStore = dstore
	context.NodeInput = make(map[string][]byte)

	return context
}

// GetRequestId returns the request id
func (context *Context) GetRequestId() string {
	return context.requestId
}

// GetPhase return the node no
func (context *Context) GetNode() string {
	return context.node
}

// Set put a value in the context using DataStore
func (context *Context) Set(key string, data interface{}) error {
	c := struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}{Key: key, Value: data}
	b, err := json.Marshal(&c)
	if err != nil {
		return fmt.Errorf("Failed to marshal data, error %v", err)
	}

	return context.dataStore.Set(key, b)
}

// Get retrieve a value from the context using DataStore
func (context *Context) Get(key string) (interface{}, error) {
	data, err := context.dataStore.Get(key)
	if err != nil {
		return nil, err
	}
	c := struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}{}
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal data, error %v", err)
	}
	return c.Value, err
}

// GetInt retrieve a integer value from the context using DataStore
func (context *Context) GetInt(key string) int {
	data, err := context.dataStore.Get(key)
	if err != nil {
		panic(fmt.Sprintf("error %v", err))
	}

	c := struct {
		Key   string `json:"key"`
		Value int    `json:"value"`
	}{}
	err = json.Unmarshal(data, &c)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal data, error %v", err))
	}

	return c.Value
}

// GetString retrieve a string value from the context using DataStore
func (context *Context) GetString(key string) string {
	data, err := context.dataStore.Get(key)
	if err != nil {
		panic(fmt.Sprintf("error %v", err))
	}

	c := struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{}
	err = json.Unmarshal(data, &c)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal data, error %v", err))
	}

	return c.Value
}

// GetBytes retrieve a byte array from the context using DataStore
func (context *Context) GetBytes(key string) []byte {
	data, err := context.dataStore.Get(key)
	if err != nil {
		panic(fmt.Sprintf("error %v", err))
	}

	c := struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}{}
	err = json.Unmarshal(data, &c)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal data, error %v", err))
	}

	return c.Value
}

// GetBool retrieve a boolean value from the context using DataStore
func (context *Context) GetBool(key string) bool {
	data, err := context.dataStore.Get(key)
	if err != nil {
		panic(fmt.Sprintf("error %v", err))
	}

	c := struct {
		Key   string `json:"key"`
		Value bool   `json:"value"`
	}{}
	err = json.Unmarshal(data, &c)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal data, error %v", err))
	}

	return c.Value
}

// Del deletes a value from the context using DataStore
func (context *Context) Del(key string) error {
	return context.dataStore.Del(key)
}
