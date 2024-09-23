package adapters

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"sync"
	"time"
)

var (
	broadcastClientStatusCnt    *prometheus.CounterVec
	kafkaDurationInSec          *prometheus.HistogramVec
	broadcastL                  sync.Mutex
	defaultKafkaDurationBuckets = []float64{0.001, 0.003, 0.005, 0.007, 0.01, 0.015, 0.02, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 2, 3}
)

func RegisterKafkaDurationHistogram(subSystem string, buckets []float64) error {
	if kafkaDurationInSec != nil {
		return nil
	}
	broadcastL.Lock()
	defer broadcastL.Unlock()
	if kafkaDurationInSec != nil {
		return nil
	}

	if buckets == nil || len(buckets) == 0 {
		buckets = defaultKafkaDurationBuckets
	}

	if subSystem == "" {
		return errors.New("register cache response durations histogram error: SubSystem not specified")
	}

	kafkaDurationInSec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: subSystem,
		Name:      "request_duration_in_seconds_kafka",
		Help:      "Histogram для отображения длительности запросов в kafka",
		Buckets:   buckets,
	},
		[]string{"code", "topic", "status"})

	return prometheus.Register(kafkaDurationInSec)
}

func BroadcastStatusInc(code int, clientID, status string) {
	if broadcastClientStatusCnt == nil {
		return
	}
	broadcastClientStatusCnt.WithLabelValues(strconv.Itoa(code), clientID, status).Inc()
}

func BroadcastStatus(code int, clientID, status string, duration time.Duration) {
	if kafkaDurationInSec == nil {
		return
	}
	kafkaDurationInSec.WithLabelValues(strconv.Itoa(code), clientID, status).Observe(duration.Seconds())
}

func RegisterMetrics() error {
	if broadcastClientStatusCnt != nil {
		return nil
	}

	broadcastL.Lock()
	defer broadcastL.Unlock()

	broadcastClientStatusCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "halykidEvents",
			Name:      "ClientsStatus",
			Help:      "счетчик количества стартов в сокет сессиях",
		},
		[]string{"code", "clientID", "status"},
	)

	return prometheus.Register(broadcastClientStatusCnt)
}
