package sdk

type Operation interface {
	GetId() string
	Encode() []byte
	GetProperties() map[string][]string
	// Execute executes an operation, executor can pass configuration
	Execute([]byte, map[string]interface{}) ([]byte, error)
}

type BlankOperation struct {
}

func (ops *BlankOperation) GetId() string {
	return "end"
}

func (ops *BlankOperation) Encode() []byte {
	return []byte("")
}

func (ops *BlankOperation) GetProperties() map[string][]string {
	return make(map[string][]string)
}

func (ops *BlankOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {
	return data, nil
}
