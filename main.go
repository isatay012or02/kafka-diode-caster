package main

import (
	"fmt"
	"github.com/isatay012or02/kafka-diode-caster/config"
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"github.com/isatay012or02/kafka-diode-caster/internal/application"
	"github.com/isatay012or02/kafka-diode-caster/internal/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.Init("config.json")
	if err != nil {
		panic(err)
	}

	kafkaReader := adapters.NewKafkaReader(cfg.Queue.Brokers, cfg.Queue.Topic, cfg.Queue.GroupID)
	udpSender, err := adapters.NewUDPSender(cfg.UdpAddress)
	if err != nil {
		panic(err)
	}

	hashCalculator := adapters.NewSHA1HashCalculator()
	duplicator := adapters.NewMessageDuplicator()

	casterService := application.NewCasterService(kafkaReader, udpSender, hashCalculator, duplicator, 2)

	err = casterService.ProcessAndSendMessages()
	if err != nil {
		panic(err)
	}

	srv, err := http.NewServer(cfg)
	if err != nil {
		panic(err)
	}

	startServerErrorCH := srv.Start()

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err = <-startServerErrorCH:
		{
			panic(err)
		}
	case q := <-quit:
		{
			fmt.Printf("receive signal %s, stopping server...\n", q.String())
			if err = srv.Stop(); err != nil {
				fmt.Printf("stop server error: %s\n", err.Error())
			}
		}
	}
}
