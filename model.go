package smp

import (
	"github.com/hibiken/asynq"
	"github.com/spf13/cast"
	"time"
)

type OptionType string

const (
	MaxRetryOpt  OptionType = "max-retry"
	QueueOpt     OptionType = "queue"
	TimeoutOpt   OptionType = "timeout"
	DeadlineOpt  OptionType = "deadline"
	UniqueOpt    OptionType = "unique"
	ProcessAtOpt OptionType = "process-at"
	ProcessInOpt OptionType = "process-in"
	TaskIdOpt    OptionType = "task-id"
	RetentionOpt OptionType = "retention"
	GroupOpt     OptionType = "group"
)

type Option struct {
	Type  OptionType `json:"type" bson:"type"`
	Value string     `json:"value" bson:"value"`
}

func (o Option) option() asynq.Option {
	switch o.Type {
	case QueueOpt:
		return asynq.Queue(o.Value)
	case TimeoutOpt:
		return asynq.Timeout(cast.ToDuration(o.Value))
	case DeadlineOpt:
		return asynq.Deadline(cast.ToTime(o.Value))
	case UniqueOpt:
		return asynq.Unique(cast.ToDuration(o.Value))
	case ProcessAtOpt:
		return asynq.ProcessAt(cast.ToTime(o.Value))
	case ProcessInOpt:
		return asynq.ProcessIn(cast.ToDuration(o.Value))
	case TaskIdOpt:
		return asynq.TaskID(o.Value)
	case RetentionOpt:
		return asynq.Retention(cast.ToDuration(o.Value))
	case GroupOpt:
		return asynq.Group(o.Value)
	}
	return asynq.MaxRetry(cast.ToInt(o.Value))
}

type PeriodicTask struct {
	Id        string    `json:"id,omitempty" bson:"_id"`
	CronExpr  string    `json:"cron_expr,omitempty" bson:"cron_expr,omitempty"`
	JobCode   string    `json:"job_code,omitempty" bson:"job_code,omitempty"`
	Payload   *string   `json:"payload,omitempty" bson:"payload,omitempty"`
	Opts      *[]Option `json:"opts,omitempty" bson:"opts,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty" bson:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty" bson:"updated_at,omitempty"`
	Enabled   *bool     `json:"enabled,omitempty" bson:"enabled,omitempty"`
}

func (pt *PeriodicTask) SetPayload(payload string) *PeriodicTask {
	pt.Payload = &payload
	return pt
}

func (pt *PeriodicTask) SetOpts(opts []Option) *PeriodicTask {
	pt.Opts = &opts
	return pt
}

func (pt *PeriodicTask) SetEnabled(enabled bool) *PeriodicTask {
	pt.Enabled = &enabled
	return pt
}

func (pt *PeriodicTask) HasOpts() bool {
	return pt.Opts != nil
}

func (pt *PeriodicTask) Active() bool {
	return pt.Enabled != nil && *pt.Enabled
}

func (pt *PeriodicTask) Task() *asynq.Task {
	var payload []byte
	if pt.Payload != nil {
		payload = []byte(*pt.Payload)
	}
	return asynq.NewTask(pt.JobCode, payload)
}

func (pt *PeriodicTask) Options() []asynq.Option {
	var aopts []asynq.Option
	if pt.Opts != nil {
		for _, o := range *pt.Opts {
			aopts = append(aopts, o.option())
		}
	}
	return aopts
}
