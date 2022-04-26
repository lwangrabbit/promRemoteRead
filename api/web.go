package api

import (
	"context"
	stdlog "log"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/route"

	"github.com/lwangrabbit/promRemoteRead/promql"
	"github.com/lwangrabbit/promRemoteRead/storage"
)

type Handler struct {
	logger log.Logger

	queryEngine *promql.Engine
	storage     storage.Storage

	api *API

	router  *route.Router
	options *Options

	quitCh chan struct{}
}

type Options struct {
	Storage                    storage.Storage
	QueryEngine                *promql.Engine
	RemoteReadSampleLimit      int
	RemoteReadConcurrencyLimit int
	RemoteReadBytesInFrame     int

	ListenAddress  string
	MaxConnections int
	ReadTimeout    time.Duration
}

func New(logger log.Logger, o *Options) *Handler {
	router := route.New()
	h := &Handler{
		logger:      logger,
		router:      router,
		queryEngine: o.QueryEngine,
		storage:     o.Storage,
		options:     o,
		quitCh:      make(chan struct{}),
	}
	h.api = NewAPI(h.queryEngine, h.storage,
		logger,
		h.options.RemoteReadSampleLimit,
		h.options.RemoteReadConcurrencyLimit,
		h.options.RemoteReadBytesInFrame,
	)
	return h
}

func (h *Handler) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/", h.router)

	av1 := route.New()
	h.api.Register(av1)
	apiPath := "/api"
	mux.Handle(apiPath+"/v1/", http.StripPrefix(apiPath+"/v1", av1))

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)
	httpSrv := &http.Server{
		Addr:        h.options.ListenAddress,
		Handler:     mux,
		ErrorLog:    errlog,
		ReadTimeout: h.options.ReadTimeout,
	}
	errCh := make(chan error)
	go func() {
		errCh <- httpSrv.ListenAndServe()
	}()
	select {
	case e := <-errCh:
		return e
	case <-ctx.Done():
		httpSrv.Shutdown(ctx)
		return nil
	}
}

func (h *Handler) Quit() <-chan struct{} {
	return h.quitCh
}
