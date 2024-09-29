package adapters

import (
	"bytes"
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
)

type KafkaLogger struct {
	logger      *log.Logger
	kafkaWriter *kafka.Writer
}

func NewKafkaLogger(brokers []string, topic string) *KafkaLogger {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	baseLogger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	return &KafkaLogger{
		logger:      baseLogger,
		kafkaWriter: writer,
	}
}

func (kl *KafkaLogger) Log(message string) {
	kl.logger.Println(message)

	err := kl.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("log-key"),
		Value: []byte(message),
	})

	if err != nil {
		panic(err)
	}
}

func (kl *KafkaLogger) SendMetricsToKafka() {
	// Сбор всех зарегистрированных метрик
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		log.Println("Ошибка сбора метрик:", err)
		return
	}

	var buf bytes.Buffer
	encoder := expfmt.NewEncoder(&buf, expfmt.FmtOpenMetrics_0_0_1)

	for _, mf := range metricFamilies {
		err := encoder.Encode(mf)
		if err != nil {
			log.Println("Ошибка кодирования метрик:", err)
			return
		}
	}

	err = kl.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("metrics"),
		Value: buf.Bytes(),
	})

	if err != nil {
		panic(err)
	}
}

func (kl *KafkaLogger) Close() {
	kl.kafkaWriter.Close()
}
