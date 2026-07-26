package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/NARUBROWN/spine/internal/pipeline"
)

type runnerFactory interface {
	Build(reg Registration) (Reader, error)
}

type Runtime struct {
	registry    *Registry
	factory     runnerFactory
	pipeline    *pipeline.Pipeline
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopOnce    sync.Once
	cancel      context.CancelFunc
	errChan     chan error
	done        chan struct{}
}

const (
	consumerReadRetryInitialDelay = 100 * time.Millisecond
	consumerReadRetryMaxDelay     = 5 * time.Second
)

// TransportRetryPolicy는 브로커 전송 오류가 발생한 뒤 Reader를 다시 생성하는 방식을 제어합니다.
// 핸들러 실패는 이 정책과 별개로 메시지 ACK/NACK에 따라 처리됩니다.
type TransportRetryPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
	MaxAttempts  int
}

type retryPolicyProvider interface {
	ConsumerRetryPolicy() TransportRetryPolicy
}

func NewTransportRetryPolicy(initialDelay, maxDelay time.Duration, multiplier, jitter float64, maxAttempts int) TransportRetryPolicy {
	policy := TransportRetryPolicy{
		InitialDelay: initialDelay,
		MaxDelay:     maxDelay,
		Multiplier:   multiplier,
		Jitter:       jitter,
		MaxAttempts:  maxAttempts,
	}
	if policy.InitialDelay == 0 {
		policy.InitialDelay = consumerReadRetryInitialDelay
	}
	if policy.MaxDelay == 0 {
		policy.MaxDelay = consumerReadRetryMaxDelay
	}
	if policy.Multiplier == 0 {
		policy.Multiplier = 2
	}
	if policy.Jitter == 0 {
		policy.Jitter = 0.2
	}
	return policy
}

func NewRuntime(registry *Registry, factory runnerFactory, pipeline *pipeline.Pipeline) *Runtime {
	if registry == nil {
		panic("consumer: registry cannot be nil")
	}
	if factory == nil {
		panic("consumer: factory cannot be nil")
	}
	if pipeline == nil {
		panic("consumer: pipeline cannot be nil")
	}

	return &Runtime{
		registry: registry,
		factory:  factory,
		pipeline: pipeline,
		errChan:  make(chan error, max(1, len(registry.Registrations()))),
		done:     make(chan struct{}),
	}
}

func (r *Runtime) retryPolicy() TransportRetryPolicy {
	if provider, ok := r.factory.(retryPolicyProvider); ok {
		return provider.ConsumerRetryPolicy()
	}
	return NewTransportRetryPolicy(0, 0, 0, 0, 0)
}

// Errors는 런타임 내부에서 발생한 치명적 에러를 전달받기 위한 채널입니다.
// 치명적 에러로 중단할 때는 에러를 먼저 채널에 기록한 뒤 Done을 닫습니다.
// Errors와 Done을 함께 기다리는 호출자는 Done을 관찰한 경우 Errors를 한 번 더
// 확인해야 합니다. 채널은 close되지 않습니다.
func (r *Runtime) Errors() <-chan error {
	return r.errChan
}

func (r *Runtime) Done() <-chan struct{} {
	return r.done
}

func (r *Runtime) Start(ctx context.Context) {
	r.lifecycleMu.Lock()
	if r.started || r.stopped {
		r.lifecycleMu.Unlock()
		return
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	for _, registration := range r.registry.Registrations() {
		log.Printf("[Event Consumer] Starting consumer for topic '%s'", registration.Topic)
		go func(reg Registration) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			reader, err := r.factory.Build(reg)
			if err == nil && reader == nil {
				err = errors.New("consumer factory returned a nil reader")
			}
			if err != nil {
				startErr := fmt.Errorf(
					"[Event Consumer] Consumer initialization failed (topic=%s): %w",
					reg.Topic,
					err,
				)
				select {
				case r.errChan <- startErr:
				default:
					log.Printf("%v (could not forward because the error channel is full)", startErr)
				}
				// 초기화 실패는 치명적이므로 전체 런타임을 중단한다.
				r.Stop()
				return
			}
			defer func() {
				if reader != nil {
					_ = reader.Close()
				}
			}()

			retryPolicy := r.retryPolicy()
			readRetryDelay := retryPolicy.InitialDelay
			retryAttempts := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					msg, err := reader.Read(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						log.Printf("[Event Consumer] Transport read failed (topic=%s): %v", reg.Topic, err)
						_ = reader.Close()
						reader = nil

						for reader == nil {
							retryAttempts++
							if retryPolicy.MaxAttempts > 0 && retryAttempts > retryPolicy.MaxAttempts {
								runtimeErr := fmt.Errorf("[Event Consumer] Transport reconnect attempts exhausted (topic=%s attempts=%d): %w", reg.Topic, retryPolicy.MaxAttempts, err)
								r.forwardError(runtimeErr)
								r.Stop()
								return
							}
							if !waitForRetry(ctx, jitteredDelay(readRetryDelay, retryPolicy.Jitter)) {
								return
							}
							candidate, buildErr := r.factory.Build(reg)
							readRetryDelay = nextRetryDelay(readRetryDelay, retryPolicy)
							if buildErr != nil || candidate == nil {
								if candidate != nil {
									_ = candidate.Close()
								}
								if buildErr == nil {
									buildErr = errors.New("consumer factory returned a nil reader")
								}
								err = buildErr
								log.Printf("[Event Consumer] Transport reconnect failed (topic=%s attempt=%d): %v", reg.Topic, retryAttempts, err)
								continue
							}
							reader = candidate
						}
						continue
					}
					readRetryDelay = retryPolicy.InitialDelay
					retryAttempts = 0

					// 컨슈머 실행 컨텍스트 생성
					reqCtx := NewRequestContext(ctx, msg, nil)

					// 핸들러 실행
					if err := r.pipeline.Execute(reqCtx); err != nil {
						log.Printf(
							"[Event Consumer] Handler execution failed (%s): %v",
							reg.Topic,
							err,
						)
						// 핸들러 실패 시 NACK
						if nackErr := msg.Nack(); nackErr != nil {
							log.Printf(
								"[Event Consumer] NACK failed (%s): %v",
								reg.Topic,
								nackErr,
							)
						}
						continue
					}

					// 핸들러 성공 시 ACK
					if ackErr := msg.Ack(); ackErr != nil {
						log.Printf(
							"[Event Consumer] ACK failed (%s): %v",
							reg.Topic,
							ackErr,
						)
					}
				}
			}
		}(registration)
	}
	r.lifecycleMu.Unlock()
}

func (r *Runtime) forwardError(err error) {
	select {
	case r.errChan <- err:
	default:
		log.Printf("%v (could not forward because the error channel is full)", err)
	}
}

func nextRetryDelay(delay time.Duration, policy TransportRetryPolicy) time.Duration {
	if float64(delay)*policy.Multiplier >= float64(policy.MaxDelay) {
		return policy.MaxDelay
	}
	return time.Duration(float64(delay) * policy.Multiplier)
}

func jitteredDelay(delay time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return delay
	}
	factor := 1 - jitter + rand.Float64()*(2*jitter)
	return max(0, time.Duration(float64(delay)*factor))
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Runtime) Validate() error {
	for _, reg := range r.registry.Registrations() {
		reader, err := r.factory.Build(reg)
		if err != nil {
			return fmt.Errorf("Consumer initialization failed (%s): %w", reg.Topic, err)
		}
		if reader == nil {
			return fmt.Errorf("Consumer initialization failed (%s): %w", reg.Topic, errors.New("consumer factory returned a nil reader"))
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("Consumer shutdown failed (%s): %w", reg.Topic, err)
		}
	}
	return nil
}

func (r *Runtime) Stop() {
	r.stopOnce.Do(func() {
		r.lifecycleMu.Lock()
		r.stopped = true
		cancel := r.cancel
		r.lifecycleMu.Unlock()
		if cancel != nil {
			cancel() // 모든 goroutine 중지
		}
		close(r.done)
		log.Printf("[Event Consumer] All consumers stopped")
	})
}
