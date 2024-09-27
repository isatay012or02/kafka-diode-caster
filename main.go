package main

import (
	"fmt"
	"github.com/isatay012or02/kafka-diode-caster/config"
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"github.com/isatay012or02/kafka-diode-caster/internal/application"
	"github.com/isatay012or02/kafka-diode-caster/internal/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	cfg, err := config.Init("config.json")
	if err != nil {
		panic(err)
	}

	go func(cfg *config.Config) {
		udpAddr := os.Getenv("UDP_ADDRESS")
		topicsEnv := os.Getenv("TOPICS")
		if topicsEnv == "" {
			fmt.Println("TOPICS не задан")
			return
		}

		topics := strings.Split(topicsEnv, ",")
		copiesCountStr := os.Getenv("DUPLICATES_COUNT")
		copiesCount, err := strconv.Atoi(copiesCountStr)
		if err != nil {
			panic(err)
		}

		enableHashEnv := os.Getenv("ENABLE_HASH")
		enableHash := false
		if enableHashEnv == "true" {
			enableHash = true
		}

		kafkaReader := adapters.NewKafkaReader(cfg.Queue.Brokers, topics, cfg.Queue.GroupID)
		udpSender, err := adapters.NewUDPSender(udpAddr)
		if err != nil {
			panic(err)
		}

		hashCalculator := adapters.NewSHA1HashCalculator()
		duplicator := adapters.NewMessageDuplicator()

		casterService := application.NewCasterService(kafkaReader, udpSender, hashCalculator, duplicator, copiesCount, enableHash)

		err = casterService.ProcessAndSendMessages()
		if err != nil {
			panic(err)
		}
	}(cfg)

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
