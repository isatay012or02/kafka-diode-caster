package ports

import "github.com/isatay012or02/kafka-diode-caster/internal/domain"

// MessageHashCalculator - интерфейс для вычисления хэша сообщения
type MessageHashCalculator interface {
	Calculate(data string) string
}

// MessageDuplicator - интерфейс для реализации дублирования сообщений
type MessageDuplicator interface {
	Duplicate(message domain.Message, copies int) []domain.Message
}
