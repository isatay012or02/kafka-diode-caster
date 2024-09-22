package application

import (
	"fmt"
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"github.com/isatay012or02/kafka-diode-caster/internal/ports"
)

type CatcherService struct {
	UDPReceiver    *adapters.UDPReceiver
	KafkaWriter    *adapters.KafkaWriter
	HashCalculator ports.MessageHashCalculator
}

func NewCatcherService(udpReceiver *adapters.UDPReceiver, kafkaWriter *adapters.KafkaWriter,
	calculator ports.MessageHashCalculator) *CatcherService {

	return &CatcherService{
		UDPReceiver:    udpReceiver,
		KafkaWriter:    kafkaWriter,
		HashCalculator: calculator,
	}
}

func (c *CatcherService) ReceiveAndPublishMessages() error {
	for {
		msg, err := c.UDPReceiver.Receive()
		if err != nil {
			return err
		}

		calculatedHash := c.HashCalculator.Calculate(msg.Data)
		if calculatedHash != msg.Hash {
			return fmt.Errorf("hash mismatch")
		}

		err = c.KafkaWriter.WriteMessage(msg)
		if err != nil {
			return err
		}
	}
}
