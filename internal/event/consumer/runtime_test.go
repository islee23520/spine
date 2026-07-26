package consumer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/core"
	"github.com/NARUBROWN/spine/internal/container"
	eventresolver "github.com/NARUBROWN/spine/internal/event/consumer/resolver"
	"github.com/NARUBROWN/spine/internal/invoker"
	"github.com/NARUBROWN/spine/internal/pipeline"
)

type runtimeTestRouter struct {
	meta core.HandlerMeta
}

func (r *runtimeTestRouter) Route(ctx core.ExecutionContext) (core.HandlerMeta, error) {
	return r.meta, nil
}

type runtimeTestReader struct {
	msg  *Message
	sent bool
}

func (r *runtimeTestReader) Read(ctx context.Context) (*Message, error) {
	if !r.sent {
		r.sent = true
		return r.msg, nil
	}

	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *runtimeTestReader) Close() error { return nil }

type runtimeTestFactory struct {
	reader Reader
}

type lifecycleRuntimeTestReader struct {
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (r *lifecycleRuntimeTestReader) Read(ctx context.Context) (*Message, error) {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	close(r.stopped)
	return nil, ctx.Err()
}

func (*lifecycleRuntimeTestReader) Close() error { return nil }

type lifecycleRuntimeTestFactory struct {
	builds atomic.Int32
	reader Reader
}

func (f *lifecycleRuntimeTestFactory) Build(Registration) (Reader, error) {
	f.builds.Add(1)
	return f.reader, nil
}

type failingRuntimeTestReader struct{ reads atomic.Int32 }

func (r *failingRuntimeTestReader) Read(ctx context.Context) (*Message, error) {
	r.reads.Add(1)
	return nil, errors.New("broker channel closed")
}

func (r *failingRuntimeTestReader) Close() error { return nil }

type reconnectingRuntimeTestFactory struct {
	builds atomic.Int32
	first  Reader
	second Reader
	policy TransportRetryPolicy
}

type nilRuntimeTestFactory struct{}

func (*nilRuntimeTestFactory) Build(Registration) (Reader, error) { return nil, nil }

type exhaustingRuntimeTestFactory struct {
	builds atomic.Int32
	reader Reader
}

func (f *exhaustingRuntimeTestFactory) Build(Registration) (Reader, error) {
	if f.builds.Add(1) == 1 {
		return f.reader, nil
	}
	return nil, errors.New("broker remains unavailable")
}

func (*exhaustingRuntimeTestFactory) ConsumerRetryPolicy() TransportRetryPolicy {
	return NewTransportRetryPolicy(time.Millisecond, time.Millisecond, 1, 0.01, 1)
}

func (f *reconnectingRuntimeTestFactory) Build(reg Registration) (Reader, error) {
	if f.builds.Add(1) == 1 {
		return f.first, nil
	}
	return f.second, nil
}

func (f *reconnectingRuntimeTestFactory) ConsumerRetryPolicy() TransportRetryPolicy {
	return f.policy
}

func (f *runtimeTestFactory) Build(reg Registration) (Reader, error) {
	return f.reader, nil
}

type runtimeTestPostHook struct {
	err error
}

func (h *runtimeTestPostHook) AfterExecution(ctx core.ExecutionContext, result []any, err error) error {
	return h.err
}

type runtimeTestController struct{}

func (c *runtimeTestController) Handle(payload []byte) {}

func (c *runtimeTestController) Panic(payload []byte) {
	panic("boom")
}

func newRuntimePipeline(t *testing.T, methodName string, hookErr error) *pipeline.Pipeline {
	t.Helper()

	ctr := container.New()
	if err := ctr.RegisterConstructor(func() *runtimeTestController {
		return &runtimeTestController{}
	}); err != nil {
		t.Fatalf("생성자 등록 실패: %v", err)
	}

	controllerType := reflect.TypeOf(&runtimeTestController{})
	method, ok := controllerType.MethodByName(methodName)
	if !ok {
		t.Fatalf("메서드를 찾을 수 없습니다: %s", methodName)
	}

	p := pipeline.NewPipeline(
		&runtimeTestRouter{
			meta: core.HandlerMeta{
				ControllerType: controllerType,
				Method:         method,
			},
		},
		invoker.NewInvoker(ctr),
	)

	p.AddArgumentResolver(&eventresolver.PayloadResolver{})
	if hookErr != nil {
		p.AddPostExecutionHook(&runtimeTestPostHook{err: hookErr})
	}

	return p
}

func waitSignal(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("신호 대기 타임아웃")
		return ""
	}
}

func TestRuntime_NackOnPostHookFailure(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}

	signals := make(chan string, 2)
	msg := &Message{
		EventName: "topic",
		Payload:   []byte(`hello`),
	}
	msg.SetAckHandler(func() error {
		signals <- "ack"
		return nil
	})
	msg.SetNackHandler(func() error {
		signals <- "nack"
		return nil
	})

	runtime := NewRuntime(
		registry,
		&runtimeTestFactory{reader: &runtimeTestReader{msg: msg}},
		newRuntimePipeline(t, "Handle", context.DeadlineExceeded),
	)

	runtime.Start(context.Background())
	defer runtime.Stop()

	if got := waitSignal(t, signals); got != "nack" {
		t.Fatalf("post hook 실패 시 NACK 되어야 합니다. 실제=%s", got)
	}
}

func TestRuntime_NackOnRecoveredPanic(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Panic); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}

	signals := make(chan string, 2)
	msg := &Message{
		EventName: "topic",
		Payload:   []byte(`hello`),
	}
	msg.SetAckHandler(func() error {
		signals <- "ack"
		return nil
	})
	msg.SetNackHandler(func() error {
		signals <- "nack"
		return nil
	})

	runtime := NewRuntime(
		registry,
		&runtimeTestFactory{reader: &runtimeTestReader{msg: msg}},
		newRuntimePipeline(t, "Panic", nil),
	)

	runtime.Start(context.Background())
	defer runtime.Stop()

	if got := waitSignal(t, signals); got != "nack" {
		t.Fatalf("panic 복구 시 NACK 되어야 합니다. 실제=%s", got)
	}
}

func TestRuntime_BacksOffAfterReaderErrors(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}
	reader := &failingRuntimeTestReader{}
	runtime := NewRuntime(registry, &runtimeTestFactory{reader: reader}, newRuntimePipeline(t, "Handle", nil))
	runtime.Start(context.Background())
	t.Cleanup(runtime.Stop)

	time.Sleep(250 * time.Millisecond)
	if got := reader.reads.Load(); got > 4 {
		t.Fatalf("reader errors must be backed off, got %d reads in 250ms", got)
	}
}

func TestRuntime_RebuildsReaderAfterTransportError(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}

	signals := make(chan string, 1)
	msg := &Message{EventName: "topic", Payload: []byte("hello")}
	msg.SetAckHandler(func() error { signals <- "ack"; return nil })
	factory := &reconnectingRuntimeTestFactory{
		first:  &failingRuntimeTestReader{},
		second: &runtimeTestReader{msg: msg},
		policy: NewTransportRetryPolicy(5*time.Millisecond, 10*time.Millisecond, 2, 0.01, 3),
	}
	runtime := NewRuntime(registry, factory, newRuntimePipeline(t, "Handle", nil))
	runtime.Start(context.Background())
	t.Cleanup(runtime.Stop)

	if got := waitSignal(t, signals); got != "ack" {
		t.Fatalf("rebuilt reader should resume message handling, got %q", got)
	}
	if builds := factory.builds.Load(); builds < 2 {
		t.Fatalf("reader should be rebuilt, builds=%d", builds)
	}
}

func TestRuntime_ValidateRejectsNilReader(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}
	runtime := NewRuntime(registry, &nilRuntimeTestFactory{}, newRuntimePipeline(t, "Handle", nil))
	err := runtime.Validate()
	if err == nil || !strings.Contains(err.Error(), "consumer factory returned a nil reader") {
		t.Fatalf("nil reader must return a validation error, got %v", err)
	}
}

func TestRuntime_StartAfterStopDoesNotStartConsumers(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("register consumer: %v", err)
	}
	reader := &lifecycleRuntimeTestReader{started: make(chan struct{}), stopped: make(chan struct{})}
	factory := &lifecycleRuntimeTestFactory{reader: reader}
	runtime := NewRuntime(registry, factory, newRuntimePipeline(t, "Handle", nil))

	runtime.Stop()
	runtime.Start(context.Background())

	if got := factory.builds.Load(); got != 0 {
		t.Fatalf("Start after Stop must not build consumers: builds=%d", got)
	}
	select {
	case <-runtime.Done():
	default:
		t.Fatal("stopped runtime must keep Done closed")
	}
}

func TestRuntime_StopCancelsPublishedStartContext(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("register consumer: %v", err)
	}
	reader := &lifecycleRuntimeTestReader{started: make(chan struct{}), stopped: make(chan struct{})}
	runtime := NewRuntime(
		registry,
		&lifecycleRuntimeTestFactory{reader: reader},
		newRuntimePipeline(t, "Handle", nil),
	)

	runtime.Start(context.Background())
	select {
	case <-reader.started:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not start")
	}
	runtime.Stop()
	select {
	case <-reader.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel the context published by Start")
	}
}

func TestRuntime_ConcurrentStartStopIsRaceFreeAndStartsAtMostOnce(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("register consumer: %v", err)
	}
	p := newRuntimePipeline(t, "Handle", nil)

	for range 200 {
		reader := &lifecycleRuntimeTestReader{started: make(chan struct{}), stopped: make(chan struct{})}
		factory := &lifecycleRuntimeTestFactory{reader: reader}
		runtime := NewRuntime(registry, factory, p)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			runtime.Start(context.Background())
		}()
		go func() {
			defer wg.Done()
			<-start
			runtime.Stop()
		}()
		close(start)
		wg.Wait()

		// 동시 상태 전환 이후 다시 호출해도 Start는 한 번만 실행됩니다.
		runtime.Start(context.Background())
		if got := factory.builds.Load(); got > 1 {
			t.Fatalf("consumer was built more than once: builds=%d", got)
		}
	}
}

func TestRuntime_StopsAfterTransportReconnectAttemptsAreExhausted(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("topic", (*runtimeTestController).Handle); err != nil {
		t.Fatalf("등록 실패: %v", err)
	}
	factory := &exhaustingRuntimeTestFactory{reader: &failingRuntimeTestReader{}}
	runtime := NewRuntime(registry, factory, newRuntimePipeline(t, "Handle", nil))
	runtime.Start(context.Background())
	t.Cleanup(runtime.Stop)

	select {
	case err := <-runtime.Errors():
		if !strings.Contains(err.Error(), "reconnect attempts exhausted") {
			t.Fatalf("unexpected runtime error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exhausted reconnect error")
	}
	select {
	case <-runtime.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop after reconnect attempts were exhausted")
	}
}
