package application

import (
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"github.com/isatay012or02/kafka-diode-caster/internal/ports"
	"time"
)

type CasterService struct {
	KafkaReader    *adapters.KafkaReader
	UDPSender      *adapters.UDPSender
	HashCalculator ports.MessageHashCalculator
	Duplicator     ports.MessageDuplicator
	Copies         int
}

func NewCasterService(kafkaReader *adapters.KafkaReader, udpSender *adapters.UDPSender,
	hashCalculator ports.MessageHashCalculator,
	duplicator ports.MessageDuplicator, copies int) *CasterService {

	return &CasterService{
		KafkaReader:    kafkaReader,
		UDPSender:      udpSender,
		HashCalculator: hashCalculator,
		Duplicator:     duplicator,
		Copies:         copies,
	}
}

func (c *CasterService) ProcessAndSendMessages() error {

	timeStart := time.Now()

	for {
		msg, err := c.KafkaReader.ReadMessage()
		if err != nil {
			adapters.BroadcastStatus(-1, msg.Topic, "ERROR", time.Since(timeStart))
			adapters.BroadcastStatusInc(-1, msg.Topic, "ERROR")
			return err
		}

		hash := c.HashCalculator.Calculate(msg.Data)
		msg.Hash = hash

		duplicatedMessages := c.Duplicator.Duplicate(msg, c.Copies)

		for _, duplicate := range duplicatedMessages {
			err := c.UDPSender.Send(duplicate)
			if err != nil {
				adapters.BroadcastStatus(-2, msg.Topic, "ERROR", time.Since(timeStart))
				adapters.BroadcastStatusInc(-2, msg.Topic, "ERROR")
				return err
			}
		}

		adapters.BroadcastStatus(0, msg.Topic, "SUCCESS", time.Since(timeStart))
	}
}
