package main

import (
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"github.com/isatay012or02/kafka-diode-caster/internal/application"
	"log"
)

func main() {
	kafkaReader := adapters.NewKafkaReader([]string{"localhost:9092"}, "example-topic", "caster-group")
	udpSender, err := adapters.NewUDPSender("127.0.0.1:9999")
	if err != nil {
		log.Fatal(err)
	}

	hashCalculator := adapters.NewSHA1HashCalculator()
	duplicator := adapters.NewMessageDuplicator()

	casterService := application.NewCasterService(kafkaReader, udpSender, hashCalculator, duplicator, 2)

	err = casterService.ProcessAndSendMessages()
	if err != nil {
		log.Fatal(err)
	}

	udpReceiver, err := adapters.NewUDPReceiver(9999)
	if err != nil {
		log.Fatal(err)
	}

	kafkaWriter := adapters.NewKafkaWriter([]string{"localhost:9092"}, "example-topic")

	catcherService := application.NewCatcherService(udpReceiver, kafkaWriter, hashCalculator)

	err = catcherService.ReceiveAndPublishMessages()
	if err != nil {
		log.Fatal(err)
	}
}
