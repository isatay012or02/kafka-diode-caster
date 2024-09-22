package adapters

import (
	"crypto/sha1"
	"fmt"
	"github.com/isatay012or02/kafka-diode-caster/internal/domain"
)

type SHA1HashCalculator struct{}

func NewSHA1HashCalculator() *SHA1HashCalculator {
	return &SHA1HashCalculator{}
}

func (h *SHA1HashCalculator) Calculate(data string) string {
	hash := sha1.Sum([]byte(data))
	return fmt.Sprintf("%x", hash)
}

type MessageDuplicator struct{}

func NewMessageDuplicator() *MessageDuplicator {
	return &MessageDuplicator{}
}

// Duplicate создает несколько копий одного сообщения для обеспечения избыточности
func (d *MessageDuplicator) Duplicate(message domain.Message, copies int) []domain.Message {
	duplicatedMessages := make([]domain.Message, copies)
	for i := 0; i < copies; i++ {
		duplicatedMessages[i] = message
	}
	return duplicatedMessages
}
