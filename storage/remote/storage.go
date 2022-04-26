package remote

import (
	"context"
	"sync"

	"github.com/prometheus/common/model"

	"github.com/lwangrabbit/promRemoteRead/config"
	"github.com/lwangrabbit/promRemoteRead/pkg/labels"
	"github.com/lwangrabbit/promRemoteRead/storage"
)

type Storage struct {
	mtx        sync.Mutex
	queryables []storage.Queryable
}

func NewStorage() *Storage {
	return &Storage{}
}

func (s *Storage) ApplyConfig(conf *config.Config) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.queryables = make([]storage.Queryable, 0, len(conf.RemoteReadConfigs))
	for i, rrConf := range conf.RemoteReadConfigs {
		c, err := NewClient(i, &ClientConfig{
			URL:              rrConf.URL,
			Timeout:          rrConf.RemoteTimeout,
			HTTPClientConfig: rrConf.HTTPClientConfig,
		})
		if err != nil {
			return err
		}
		q := QueryableClient(c)
		if len(rrConf.RequiredMatchers) > 0 {
			q = RequiredMatchersFilter(q, labelsToEqualityMatchers(rrConf.RequiredMatchers))
		}
		s.queryables = append(s.queryables, q)
	}
	return nil
}

func (s *Storage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	s.mtx.Lock()
	queryables := s.queryables
	s.mtx.Unlock()

	queriers := make([]storage.Querier, 0, len(queryables))
	for _, queryable := range queryables {
		q, err := queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		queriers = append(queriers, q)
	}
	return storage.NewMergeQuerier(queriers), nil
}

func (s *Storage) Close() error {
	return nil
}

func (s *Storage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func (s *Storage) Appender() (storage.Appender, error) {
	return s, nil
}

// Add implements storage.Appender.
func (s *Storage) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	return 0, nil
}

// AddFast implements storage.Appender.
func (s *Storage) AddFast(l labels.Labels, _ uint64, t int64, v float64) error {
	_, err := s.Add(l, t, v)
	return err
}

// Commit implements storage.Appender.
func (*Storage) Commit() error {
	return nil
}

// Rollback implements storage.Appender.
func (*Storage) Rollback() error {
	return nil
}

func labelsToEqualityMatchers(ls model.LabelSet) []*labels.Matcher {
	ms := make([]*labels.Matcher, 0, len(ls))
	for k, v := range ls {
		ms = append(ms, &labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  string(k),
			Value: string(v),
		})
	}
	return ms
}
