package smp

import (
	"context"
	"fmt"
	"github.com/go-funcards/slice"
	"github.com/hibiken/asynq"
	"github.com/rumorsflow/mongo-ext"
)

const query = "index=%d&size=%d&f[0][0][value]=true&f[0][0][field]=enabled"

type provider struct {
	s PeriodicTaskStorage
}

func (p *provider) GetConfigs() ([]*asynq.PeriodicTaskConfig, error) {
	var data []*asynq.PeriodicTaskConfig

	for index, size := int64(0), int64(20); ; index += size {
		items, err := p.getTasks(context.Background(), index, size)
		if err != nil {
			return nil, err
		}

		data = append(data, slice.Map(items, toConfig)...)

		if len(items) < int(size) {
			break
		}
	}

	return data, nil
}

func (p *provider) getTasks(ctx context.Context, index, size int64) ([]PeriodicTask, error) {
	return p.s.Find(ctx, mongoext.MustC(fmt.Sprintf(query, index, size), "f"))
}

func toConfig(task PeriodicTask) *asynq.PeriodicTaskConfig {
	return &asynq.PeriodicTaskConfig{
		Cronspec: task.CronExpr,
		Task:     task.Task(),
		Opts:     task.Options(),
	}
}
