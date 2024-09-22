package adapters

import (
	"github.com/isatay012or02/kafka-diode-caster/internal/domain"
	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafka.Reader
}

func NewKafkaReader(brokers []string, topic string, groupID string) *KafkaReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	return &KafkaReader{reader: reader}
}

func (kr *KafkaReader) ReadMessage() (domain.Message, error) {
	msg, err := kr.reader.ReadMessage(nil)
	if err != nil {
		return domain.Message{}, err
	}

	return domain.Message{
		Topic: msg.Topic,
		Data:  string(msg.Value),
	}, nil
}
