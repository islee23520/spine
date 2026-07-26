package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/core"
	"github.com/NARUBROWN/spine/internal/container"
	"github.com/NARUBROWN/spine/internal/event/consumer"
	spineRouter "github.com/NARUBROWN/spine/internal/router"
	"github.com/NARUBROWN/spine/pkg/boot"
)

type testController struct{}

func (c *testController) Handle() string { return "ok" }

type testTransport struct {
	initErr    error
	startErr   error
	initCalls  atomic.Int32
	startCalls atomic.Int32
	stopCalls  atomic.Int32
}

type listenerFatalTransport struct {
	address          string
	fatalErr         error
	listenerObserved atomic.Bool
	stopCalls        atomic.Int32
}

func (*listenerFatalTransport) Init(core.Container) error { return nil }

func (t *listenerFatalTransport) Start() error {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", t.address, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.listenerObserved.Store(true)
			return t.fatalErr
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("HTTP listener did not start before custom transport timeout: %w", t.fatalErr)
}

func (t *listenerFatalTransport) Stop(context.Context) error {
	t.stopCalls.Add(1)
	return nil
}

func (t *testTransport) Init(container core.Container) error {
	t.initCalls.Add(1)
	return t.initErr
}

func (t *testTransport) Start() error {
	t.startCalls.Add(1)
	return t.startErr
}

func (t *testTransport) Stop(ctx context.Context) error {
	t.stopCalls.Add(1)
	return nil
}

func TestJoinPath(t *testing.T) {
	if got, err := joinPath("", "users"); err != nil || got != "/users" {
		t.Fatalf("leading slash가 보정되어야 합니다: %s", got)
	}
	if got, err := joinPath("/api", "/users"); err != nil || got != "/api/users" {
		t.Fatalf("prefix 결합이 잘못되었습니다: %s", got)
	}
}

func TestJoinPath_EmptyReturnsError(t *testing.T) {
	if _, err := joinPath("", ""); err == nil {
		t.Fatal("빈 path는 에러여야 합니다")
	}
}

func TestAssertNoAmbiguousRoute(t *testing.T) {
	if err := assertNoAmbiguousRoute("GET", "/users/:id", []string{"/users/me"}); err == nil {
		t.Fatal("모호한 라우트는 에러여야 합니다")
	}
	if err := assertNoAmbiguousRoute("GET", "/users/:id", []string{"/teams/me"}); err != nil {
		t.Fatalf("예상하지 못한 에러입니다: %v", err)
	}
	if err := assertNoAmbiguousRoute("GET", "/users/:id/posts", []string{"/users/me"}); err != nil {
		t.Fatalf("예상하지 못한 에러입니다: %v", err)
	}
}

func TestRun_InvalidGlobalPrefixReturnsError(t *testing.T) {
	for _, prefix := range []string{"api", "/api/:id", "/api/*"} {
		prefix := prefix
		err := Run(Config{
			HTTP: &boot.HTTPOptions{
				GlobalPrefix: prefix,
			},
		})
		if err == nil {
			t.Fatalf("잘못된 prefix는 에러여야 합니다: %s", prefix)
		}
	}
}

func TestRun_AmbiguousRoutesReturnsError(t *testing.T) {
	err := Run(Config{
		HTTP: &boot.HTTPOptions{},
		Routes: []spineRouter.RouteSpec{
			{Method: "GET", Path: "/users/:id", Handler: (*testController).Handle},
			{Method: "GET", Path: "/users/me", Handler: (*testController).Handle},
		},
	})
	if err == nil {
		t.Fatal("모호한 라우트는 에러여야 합니다")
	}
}

func TestRun_CustomTransportInitError(t *testing.T) {
	transport := &testTransport{initErr: errors.New("init fail")}

	err := Run(Config{
		CustomTransports: []core.CustomTransport{transport},
		ShutdownTimeout:  10 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "init fail") {
		t.Fatalf("Init 에러가 반환되어야 합니다: %v", err)
	}
	if transport.startCalls.Load() != 0 {
		t.Fatalf("Init 실패 시 Start는 호출되면 안 됩니다: %d", transport.startCalls.Load())
	}
	if transport.stopCalls.Load() != 0 {
		t.Fatalf("Init 실패 시 Stop은 호출되면 안 됩니다: %d", transport.stopCalls.Load())
	}
}

func TestRun_CustomTransportStartError(t *testing.T) {
	transport := &testTransport{startErr: errors.New("start fail")}

	err := Run(Config{
		CustomTransports: []core.CustomTransport{transport},
		ShutdownTimeout:  10 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "start fail") {
		t.Fatalf("Start 에러가 반환되어야 합니다: %v", err)
	}
	if transport.initCalls.Load() != 1 || transport.startCalls.Load() != 1 {
		t.Fatalf("Init/Start 호출 수가 잘못되었습니다: init=%d start=%d", transport.initCalls.Load(), transport.startCalls.Load())
	}
	if transport.stopCalls.Load() != 1 {
		t.Fatalf("Start 실패 후에도 Stop이 호출되어야 합니다: %d", transport.stopCalls.Load())
	}
}

func TestRequiresShutdownSignalForEveryBlockingRuntimeShape(t *testing.T) {
	registry := consumer.NewRegistry()
	if err := registry.Register("topic", (*testController).Handle); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	tests := []struct {
		name   string
		config Config
		want   bool
	}{
		{name: "HTTP graceful", config: Config{HTTP: &boot.HTTPOptions{}, EnableGracefulShutdown: true}, want: true},
		{name: "HTTP non-graceful", config: Config{HTTP: &boot.HTTPOptions{}}, want: false},
		{name: "custom transport only", config: Config{CustomTransports: []core.CustomTransport{&testTransport{}}}, want: true},
		{name: "Kafka consumer only", config: Config{ConsumerRegistry: registry, Kafka: &boot.KafkaOptions{Read: &boot.KafkaReadOptions{}}}, want: true},
		{name: "RabbitMQ consumer only", config: Config{ConsumerRegistry: registry, RabbitMQ: &boot.RabbitMqOptions{Read: &boot.RabbitMqReadOptions{}}}, want: true},
		{name: "registry without runtime", config: Config{ConsumerRegistry: registry}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresShutdownSignal(tt.config); got != tt.want {
				t.Fatalf("requiresShutdownSignal() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRun_CustomTransportFatalErrorShutsDownHTTPListener(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve HTTP address: %v", err)
	}
	address := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatalf("release reserved HTTP address: %v", err)
	}

	fatalErr := errors.New("custom transport fatal")
	transport := &listenerFatalTransport{address: address, fatalErr: fatalErr}
	runErr := Run(Config{
		Address:          address,
		HTTP:             &boot.HTTPOptions{},
		CustomTransports: []core.CustomTransport{transport},
		ShutdownTimeout:  time.Second,
	})
	if !errors.Is(runErr, fatalErr) {
		t.Fatalf("original custom transport error must be preserved: %v", runErr)
	}
	if !transport.listenerObserved.Load() {
		t.Fatal("custom transport failed before observing the HTTP listener")
	}
	if transport.stopCalls.Load() != 1 {
		t.Fatalf("custom transport Stop calls = %d, want 1", transport.stopCalls.Load())
	}

	reused, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("HTTP address was not reusable after App.Run returned: %v", err)
	}
	if err := reused.Close(); err != nil {
		t.Fatalf("close reused HTTP listener: %v", err)
	}
}

func TestWaitConsumerError_DrainsFatalErrorWhenDoneIsAlsoReady(t *testing.T) {
	errorsCh := make(chan error, 1)
	done := make(chan struct{})
	fatalErr := errors.New("consumer failed")
	errorsCh <- fatalErr
	close(done)

	for range 100 {
		// 각 반복에서 두 채널을 동시에 준비해 select 선택 순서와 무관한 계약을 검증합니다.
		repeatedErrors := make(chan error, 1)
		repeatedDone := make(chan struct{})
		repeatedErrors <- fatalErr
		close(repeatedDone)
		if got := waitConsumerError(repeatedErrors, repeatedDone); !errors.Is(got, fatalErr) {
			t.Fatalf("fatal consumer error was lost: %v", got)
		}
	}

	if got := waitConsumerError(errorsCh, done); !errors.Is(got, fatalErr) {
		t.Fatalf("fatal consumer error was lost: %v", got)
	}
}

func TestWaitConsumerError_NormalStopDoesNotProduceError(t *testing.T) {
	errorsCh := make(chan error, 1)
	done := make(chan struct{})
	close(done)

	if got := waitConsumerError(errorsCh, done); got != nil {
		t.Fatalf("normal consumer stop must not produce an error: %v", got)
	}
}

func TestRun_WarmUpResolveFailureReturnsError(t *testing.T) {
	err := Run(Config{
		HTTP: &boot.HTTPOptions{},
		Routes: []spineRouter.RouteSpec{
			{Method: "GET", Path: "/users", Handler: (*testController).Handle},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "warm-up failed") {
		t.Fatalf("Warm-up 실패가 에러로 반환되어야 합니다: %v", err)
	}
}

func TestRun_NilGlobalInterceptorReturnsError(t *testing.T) {
	err := Run(Config{
		HTTP:         &boot.HTTPOptions{},
		Interceptors: []core.Interceptor{nil},
	})
	if err == nil || !strings.Contains(err.Error(), "interceptor is nil") {
		t.Fatalf("nil 인터셉터는 에러여야 합니다: %v", err)
	}
}

func TestValidate_RejectsInvalidInterceptorScopeBeforeStartup(t *testing.T) {
	err := Validate(Config{
		HTTP: &boot.HTTPOptions{},
		ScopedInterceptors: []InterceptorBinding{{
			Interceptor: &bootstrapTestInterceptor{},
			Scope:       boot.InterceptorScope(1 << 7),
		}},
	})
	configErr, ok := err.(*boot.ConfigError)
	if !ok {
		t.Fatalf("Validate error = %T, want *boot.ConfigError", err)
	}
	if len(configErr.Issues) != 1 || configErr.Issues[0].Code != "INTERCEPTOR_SCOPE_INVALID" {
		t.Fatalf("unexpected validation issues: %v", configErr.Issues)
	}
}

func TestSpineVersionMatchesV05Migration(t *testing.T) {
	if spineVersion != "v0.5.0" {
		t.Fatalf("runtime version = %q, want v0.5.0", spineVersion)
	}
}

func TestResolveGlobalInterceptorsKeepsDistinctStatefulInstancesByScope(t *testing.T) {
	httpOnly := &bootstrapTestInterceptor{}
	wsOnly := &bootstrapTestInterceptor{}
	httpInterceptors, wsInterceptors, err := resolveGlobalInterceptors(container.New(), Config{
		ScopedInterceptors: []InterceptorBinding{
			{Interceptor: httpOnly, Scope: boot.InterceptorHTTP},
			{Interceptor: wsOnly, Scope: boot.InterceptorWebSocket},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(httpInterceptors) != 1 || len(wsInterceptors) != 1 {
		t.Fatalf("resolved interceptor counts: HTTP=%d WS=%d", len(httpInterceptors), len(wsInterceptors))
	}
	if err := httpInterceptors[0].PreHandle(nil, core.HandlerMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := wsInterceptors[0].PreHandle(nil, core.HandlerMeta{}); err != nil {
		t.Fatal(err)
	}
	if httpOnly.calls.Load() != 1 || wsOnly.calls.Load() != 1 {
		t.Fatalf("scoped instances were not executed independently: HTTP=%d WS=%d", httpOnly.calls.Load(), wsOnly.calls.Load())
	}
}

func TestResolveGlobalInterceptorsMergesOnlyTheSameInstance(t *testing.T) {
	shared := &bootstrapTestInterceptor{}
	httpInterceptors, wsInterceptors, err := resolveGlobalInterceptors(container.New(), Config{
		ScopedInterceptors: []InterceptorBinding{
			{Interceptor: shared, Scope: boot.InterceptorHTTP},
			{Interceptor: shared, Scope: boot.InterceptorHTTP},
			{Interceptor: shared, Scope: boot.InterceptorWebSocket},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(httpInterceptors) != 1 || len(wsInterceptors) != 1 {
		t.Fatalf("same instance must be registered once per selected scope: HTTP=%d WS=%d", len(httpInterceptors), len(wsInterceptors))
	}
}

func TestResolveGlobalInterceptorsMergesTypedNilPlaceholderByType(t *testing.T) {
	ctr := container.New()
	resolved := &bootstrapTestInterceptor{}
	if err := ctr.RegisterConstructor(func() *bootstrapTestInterceptor { return resolved }); err != nil {
		t.Fatal(err)
	}
	var placeholder *bootstrapTestInterceptor
	httpInterceptors, wsInterceptors, err := resolveGlobalInterceptors(ctr, Config{
		ScopedInterceptors: []InterceptorBinding{
			{Interceptor: placeholder, Scope: boot.InterceptorHTTP},
			{Interceptor: placeholder, Scope: boot.InterceptorWebSocket},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(httpInterceptors) != 1 || len(wsInterceptors) != 1 || httpInterceptors[0] != resolved || wsInterceptors[0] != resolved {
		t.Fatalf("typed nil placeholder must resolve once for both scopes: HTTP=%v WS=%v", httpInterceptors, wsInterceptors)
	}
}

type bootstrapTestInterceptor struct{ calls atomic.Int32 }

func (i *bootstrapTestInterceptor) PreHandle(core.ExecutionContext, core.HandlerMeta) error {
	i.calls.Add(1)
	return nil
}
func (*bootstrapTestInterceptor) PostHandle(core.ExecutionContext, core.HandlerMeta) {}
func (*bootstrapTestInterceptor) BeforeResponse(core.ExecutionContext, core.HandlerMeta, error) error {
	return nil
}
func (*bootstrapTestInterceptor) AfterCompletion(core.ExecutionContext, core.HandlerMeta, error) {
}
