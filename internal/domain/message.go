package domain

type Message struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"data"`
	Hash  string `json:"hash"`
}
