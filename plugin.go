package smp

import (
	"context"
	"github.com/hibiken/asynq"
	"github.com/roadrunner-server/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"sync"
)

const PluginName = "scheduler_mongo_provider"

type Plugin struct {
	log  *zap.Logger
	s    *storage
	done chan struct{}
	wg   sync.WaitGroup
}

func (p *Plugin) Init(log *zap.Logger, db *mongo.Database) error {
	p.log = log
	p.s = newStorage(db)
	p.done = make(chan struct{})

	return nil
}

func (p *Plugin) Serve() chan error {
	errCh := make(chan error, 1)

	p.wg.Add(1)
	go p.indexes(errCh)

	return errCh
}

func (p *Plugin) Stop() error {
	close(p.done)
	p.wg.Wait()
	return nil
}

// Name returns user-friendly plugin name
func (p *Plugin) Name() string {
	return PluginName
}

// Provides declares factory methods.
func (p *Plugin) Provides() []any {
	return []any{
		p.Storage,
		p.PeriodicTaskProvider,
	}
}

func (p *Plugin) Storage() PeriodicTaskStorage {
	return p.s
}

func (p *Plugin) PeriodicTaskProvider() asynq.PeriodicTaskConfigProvider {
	return &provider{s: p.s}
}

func (p *Plugin) indexes(errCh chan error) {
	defer p.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const op = errors.Op("scheduler-mongo plugin indexes")

	go func() {
		if err := p.s.indexes(ctx); err != nil {
			errCh <- errors.E(op, errors.Serve, err)
		}
	}()

	select {
	case <-p.done:
		p.log.Debug("scheduler mongo done")
	case <-ctx.Done():
		if ctx.Err() != nil {
			errCh <- errors.E(op, errors.Serve, ctx.Err())
		}
	}
}
