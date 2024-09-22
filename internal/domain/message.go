package domain

type Message struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
	Hash  string `json:"hash"`
}
