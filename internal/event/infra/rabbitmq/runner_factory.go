package rabbitmq

import (
	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
)

type RunnerFactory struct {
	opts boot.RabbitMqOptions
}

func NewRunnerFactory(opts boot.RabbitMqOptions) *RunnerFactory {
	return &RunnerFactory{opts: opts}
}

func (f *RunnerFactory) Build(registration consumer.Registration) (consumer.Reader, error) {
	var deadLetter *RabbitMqDeadLetterOptions
	if f.opts.Read.DeadLetter != nil {
		deadLetter = &RabbitMqDeadLetterOptions{
			Exchange:   f.opts.Read.DeadLetter.Exchange,
			RoutingKey: f.opts.Read.DeadLetter.RoutingKey,
		}
	}
	return NewRabbitMqReader(RabbitMqOptions{
		URL:                    f.opts.URL,
		AllowInsecureTransport: f.opts.AllowInsecureTransport,
		Read: &RabbitMqReadOptions{
			Queue:          registration.Topic,
			Exchange:       f.opts.Read.Exchange,
			RoutingKey:     registration.Topic,
			FailurePolicy:  RabbitMqFailurePolicy(f.opts.Read.EffectiveFailurePolicy()),
			DeadLetter:     deadLetter,
			RequeueOnError: f.opts.Read.RequeueOnError,
		},
	})
}

func (f *RunnerFactory) ConsumerRetryPolicy() consumer.TransportRetryPolicy {
	return consumer.NewTransportRetryPolicy(
		f.opts.ConsumerRetry.InitialDelay,
		f.opts.ConsumerRetry.MaxDelay,
		f.opts.ConsumerRetry.Multiplier,
		f.opts.ConsumerRetry.Jitter,
		f.opts.ConsumerRetry.MaxAttempts,
	)
}
