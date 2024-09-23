package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Http HttpCfg
	//CorsSettings ginmiddlewares.CORSSettings
	Queue      Queue
	UdpAddress string
}

type HttpCfg struct {
	Port int `json:"port"`
	Gin  struct {
		ReleaseMode bool `json:"ReleaseMode"`
		UseLogger   bool `json:"UseLogger"`
		UseRecovery bool `json:"UseRecovery"`
	}
	ProfilingEnabled bool `json:"ProfilingEnabled"`
	StopTimeout      int  `json:"StopTimeout"`
}

type Queue struct {
	Brokers []string
	GroupID string
	Topic   string
	Metrics struct {
		Enabled         bool
		Label           string
		DurationBuckets []float64
	}
}

func Init(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(file)
	cfg := new(Config)
	err = decoder.Decode(&cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
