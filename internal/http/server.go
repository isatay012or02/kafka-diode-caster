package http

import (
	"context"
	"fmt"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/isatay012or02/kafka-diode-caster/config"
	"github.com/isatay012or02/kafka-diode-caster/internal/adapters"
	"net/http"
	"time"
)

// Server описывает веб сервер
type Server interface {
	// Start запускает веб сервер
	Start() <-chan error
	// Stop останавливает веб сервер
	Stop() error
}

// impl реализация веб сервера
type impl struct {
	config *config.Config
	router *gin.Engine
	server *http.Server
}

// Start запускает веб сервер
func (srv *impl) Start() <-chan error {
	ch := make(chan error, 1)

	go func() {
		if err := srv.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ch <- err
		}
	}()

	return ch
}

// Stop останавливает веб сервер
func (srv *impl) Stop() error {
	timeout := time.Duration(srv.config.Http.StopTimeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.server.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

// NewServer создает новый веб сервер
func NewServer(config *config.Config) (Server, error) {

	fmt.Println(config)

	if config.Http.Gin.ReleaseMode {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	if config.Http.Gin.UseLogger {
		r.Use(gin.Logger())
	}
	if config.Http.Gin.UseRecovery {
		r.Use(gin.Recovery())
	}

	if config.Http.ProfilingEnabled {
		pprof.Register(r)
	}

	//r.Use(ginmiddlewares.CORS(config.CorsSettings))

	srv := &impl{
		config: config,
		router: r,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", config.Http.Port),
			Handler: r,
		},
	}

	adapters.RegisterMetrics()
	adapters.RegisterKafkaDurationHistogram(config.Queue.Metrics.Label, config.Queue.Metrics.DurationBuckets)

	srv.routers()

	return srv, nil
}
