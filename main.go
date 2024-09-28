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
	"time"
)

func main() {
	cfg, err := config.Init("config.json")
	if err != nil {
		panic(err)
	}

	go func(cfg *config.Config) {
		loggerTopic := os.Getenv("KAFKA_LOGGER_TOPIC")

		logger := adapters.NewKafkaLogger(cfg.Queue.Brokers, loggerTopic)
		logger.Log(fmt.Sprintf("[%v][INFO]Caster service started", time.Now()))

		udpAddr := os.Getenv("UDP_ADDRESS")
		if udpAddr == "" {
			logger.Log(fmt.Sprintf("[%v][Error] %v", time.Now(), "UDP_ADDRESS environment variable not set"))
			panic(err)
		}

		topicsEnv := os.Getenv("TOPICS")
		if topicsEnv == "" {
			logger.Log(fmt.Sprintf("[%v][Error] %v", time.Now(), "TOPICS environment variable not set"))
			panic(err)
		}

		topics := strings.Split(topicsEnv, ",")
		copiesCountStr := os.Getenv("DUPLICATES_COUNT")
		copiesCount, err := strconv.Atoi(copiesCountStr)
		if err != nil {
			logger.Log(fmt.Sprintf("[%v][Error] %v", time.Now(), err.Error()))
			panic(err)
		}

		enableHashEnv := os.Getenv("ENABLE_HASH")
		if enableHashEnv == "" {
			logger.Log(fmt.Sprintf("[%v][Error] %v", time.Now(), "ENABLE_HASH environment variable not set"))
			panic(err)
		}
		enableHash := false
		if enableHashEnv == "true" {
			enableHash = true
		}

		kafkaReader := adapters.NewKafkaReader(cfg.Queue.Brokers, topics, cfg.Queue.GroupID)
		udpSender, err := adapters.NewUDPSender(udpAddr)
		if err != nil {
			logger.Log(fmt.Sprintf("[%v][Error] %v", time.Now(), err.Error()))
			panic(err)
		}

		hashCalculator := adapters.NewSHA1HashCalculator()
		duplicator := adapters.NewMessageDuplicator()

		casterService := application.NewCasterService(kafkaReader, udpSender, hashCalculator, duplicator, copiesCount, enableHash, logger)

		err = casterService.ProcessAndSendMessages()
		logger.SendMetricsToKafka()
		logger.Close()
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
