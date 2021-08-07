package runtime

type Response struct {
	RequestID string
	Header    map[string][]string
	Body      []byte
}

func (response *Response) SetHeader(header string, value string) {
	response.Header[header] = []string{value}
}
