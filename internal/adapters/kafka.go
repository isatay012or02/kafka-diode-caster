package adapters

import (
	"context"
	"github.com/isatay012or02/kafka-diode-caster/internal/domain"
	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafka.Reader
}

func NewKafkaReader(brokers []string, topics []string, groupID string) *KafkaReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupTopics: topics,
		GroupID:     groupID,
		MinBytes:    10e3,
		MaxBytes:    10e6,
	})
	return &KafkaReader{reader: reader}
}

func (kr *KafkaReader) ReadMessage() (domain.Message, error) {
	msg, err := kr.reader.ReadMessage(context.Background())
	if err != nil {
		return domain.Message{}, err
	}

	return domain.Message{
		Topic: msg.Topic,
		Data:  string(msg.Value),
	}, nil
}
