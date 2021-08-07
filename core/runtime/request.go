package runtime

type Request struct {
	FlowName  string
	RequestID string
	Header    map[string][]string
	RawQuery  string
	Query     map[string][]string
	Body      []byte
}

func (request *Request) GetHeader(header string) string {
	val := request.Header[header]
	if len(val) >= 1 {
		return val[0]
	}
	return ""
}
