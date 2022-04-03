package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/promql"

	"github.com/lwangrabbit/promRemoteRead/api"
	"github.com/lwangrabbit/promRemoteRead/config"
	"github.com/lwangrabbit/promRemoteRead/storage/remote"
)

var (
	DefaultListenAddr = "0.0.0.0:12345"

	DefaultQueryMaxConcurrency = 20
	DefaultQueryMaxSamples     = 50000000
	DefaultQueryTimeout        = 2 * time.Minute

	DefaultStorageRemoteReadSampleLimit      = 5e7
	DefaultStorageRemoteReadConcurrencyLimit = 10
	DefaultStorageRemoteReadBytesInFrame     = 1048576

	DefaultApiMaxConnections = 512
	DefaultApiReadTimeout    = 5 * time.Minute
)

func main() {
	var configFile string
	var listenAddr string

	flag.StringVar(&configFile, "config.file", "config.yaml", "Configuration file path")
	flag.StringVar(&listenAddr, "listen-address", DefaultListenAddr, "web listen address")
	flag.Parse()

	logLevel := promlog.AllowedLevel{}
	logLevel.Set("info")
	logger := promlog.New(&promlog.Config{
		Level: &logLevel,
	})

	engineOpts := promql.EngineOpts{
		Logger:        nil,
		Reg:           prometheus.DefaultRegisterer,
		MaxConcurrent: DefaultQueryMaxConcurrency,
		MaxSamples:    DefaultQueryMaxSamples,
		Timeout:       DefaultQueryTimeout,
	}
	queryEngine := promql.NewEngine(engineOpts)

	remoteStorage := remote.NewStorage()

	option := api.Options{
		Storage:                    remoteStorage,
		QueryEngine:                queryEngine,
		RemoteReadSampleLimit:      int(DefaultStorageRemoteReadSampleLimit),
		RemoteReadConcurrencyLimit: DefaultStorageRemoteReadConcurrencyLimit,
		RemoteReadBytesInFrame:     DefaultStorageRemoteReadBytesInFrame,
		ListenAddress:              listenAddr,
		MaxConnections:             DefaultApiMaxConnections,
		ReadTimeout:                DefaultApiReadTimeout,
	}
	webHandler := api.New(logger, &option)

	reloaders := []func(cfg *config.Config) error{
		remoteStorage.ApplyConfig,
	}

	ctxWeb, cancelWeb := context.WithCancel(context.Background())

	var g run.Group
	{
		//Termination handler
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
				case <-webHandler.Quit():
					level.Warn(logger).Log("msg", "Received termination request via web service, exiting gracefully...")
				case <-cancel:
					break
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	{
		g.Add(
			func() error {
				if err := webHandler.Run(ctxWeb); err != nil {
					return errors.Wrapf(err, "error starting web server")
				}
				return nil
			},
			func(err error) {
				cancelWeb()
			},
		)
	}
	{
		cancel := make(chan struct{})
		g.Add(
			func() error {
				if err := reloadConfig(configFile, logger, reloaders...); err != nil {
					return fmt.Errorf("error loading config file: %v, err: %v", configFile, err)
				}
				<-cancel
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")
}

func reloadConfig(filename string, logger log.Logger, rls ...func(*config.Config) error) (err error) {
	level.Info(logger).Log("msg", "Loading configuration file", "filename", filename)

	conf, err := config.LoadFile(filename)
	if err != nil {
		return fmt.Errorf("couldn't load configuration (--config.file=%q): %v", filename, err)
	}

	failed := false
	for _, rl := range rls {
		if err := rl(conf); err != nil {
			level.Error(logger).Log("msg", "Failed to apply configuration", "err", err)
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("one or more errors occurred while applying the new configuration (--config.file=%q)", filename)
	}
	level.Info(logger).Log("msg", "Completed loading of configuration file", "filename", filename)
	return nil
}
