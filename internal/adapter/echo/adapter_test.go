package echo

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/internal/container"
	"github.com/NARUBROWN/spine/internal/handler"
	"github.com/NARUBROWN/spine/internal/invoker"
	"github.com/NARUBROWN/spine/internal/pipeline"
	"github.com/NARUBROWN/spine/internal/resolver"
	"github.com/NARUBROWN/spine/internal/router"
	"github.com/NARUBROWN/spine/pkg/boot"
	"github.com/NARUBROWN/spine/pkg/httperr"
	"github.com/labstack/echo/v4"
)

type malformedJSONDTO struct {
	Name string `json:"name"`
}

func TestServerListenerAddress_DefaultsEmptyAddressToHTTP(t *testing.T) {
	server := NewServer(nil, "", nil, boot.HTTPOptions{})
	if got := server.listenAddress(); got != ":http" {
		t.Fatalf("empty listener address = %q, want %q", got, ":http")
	}
	server = NewServer(nil, "127.0.0.1:0", nil, boot.HTTPOptions{})
	if got := server.listenAddress(); got != "127.0.0.1:0" {
		t.Fatalf("explicit listener address = %q, want %q", got, "127.0.0.1:0")
	}
}

func TestServerListenerReady_ServesBoundAddressAndCloses(t *testing.T) {
	ready := make(chan net.Addr, 1)
	var calls atomic.Int32
	server := NewServer(nil, "127.0.0.1:0", nil, boot.HTTPOptions{
		ListenerReady: func(address net.Addr) {
			calls.Add(1)
			ready <- address
		},
	})
	server.echo.GET("/ready", func(c echo.Context) error {
		c.Response().Header().Set("X-Listener-Proof", "owned")
		return c.String(http.StatusOK, "listener ready")
	})
	done := make(chan error, 1)
	go func() { done <- server.Start() }()
	t.Cleanup(func() { _ = server.httpServer.Close() })
	var address net.Addr
	select {
	case address = <-ready:
	case err := <-done:
		t.Fatalf("Start returned before readiness: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("listener readiness was not delivered")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + address.String() + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "listener ready" || response.Header.Get("X-Listener-Proof") != "owned" {
		t.Fatalf("HTTP response: status=%d headers=%v body=%q error=%v", response.StatusCode, response.Header, body, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Start terminal error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Start did not terminate after Shutdown")
	}
	if calls.Load() != 1 {
		t.Fatalf("readiness calls = %d, want 1", calls.Load())
	}
	reused, err := net.Listen("tcp", address.String())
	if err != nil {
		t.Fatalf("listener remains open: %v", err)
	}
	if err := reused.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("listener proof: address=%s calls=%d HTTP=%d headers=%v body=%q Start=ErrServerClosed Shutdown=nil listener_closed=true", address, calls.Load(), response.StatusCode, response.Header, body)
}

func TestServerListenerReady_BindFailureDoesNotNotify(t *testing.T) {
	owned, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer owned.Close()
	var calls atomic.Int32
	server := NewServer(nil, owned.Addr().String(), nil, boot.HTTPOptions{
		ListenerReady: func(net.Addr) { calls.Add(1) },
	})
	err = server.Start()
	var bindError *net.OpError
	if !errors.As(err, &bindError) || bindError.Op != "listen" {
		t.Fatalf("Start error = %v, want listen error", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("readiness calls after terminal bind failure = %d, want 0", calls.Load())
	}
	t.Logf("bind failure proof: address=%s calls=%d error=%v", owned.Addr(), calls.Load(), err)
}

type malformedJSONController struct{}

func (*malformedJSONController) Create(*malformedJSONDTO) string { return "created" }

func TestContextBind_NormalizesMalformedJSONToBadRequest(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{"))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	ctx := NewContext(e.NewContext(req, httptest.NewRecorder())).(*echoContext)

	err := ctx.Bind(&struct {
		Name string `json:"name"`
	}{})
	var httpErr *httperr.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest {
		t.Fatalf("잘못된 JSON은 400 Spine 오류여야 합니다: %T %v", err, err)
	}
}

func TestPipeline_MalformedJSONRespondsBadRequest(t *testing.T) {
	ctr := container.New()
	if err := ctr.RegisterConstructor(func() *malformedJSONController { return &malformedJSONController{} }); err != nil {
		t.Fatal(err)
	}
	meta, err := router.NewHandlerMeta((*malformedJSONController).Create)
	if err != nil {
		t.Fatal(err)
	}
	r := router.NewRouter()
	r.Register(http.MethodPost, "/items", meta)
	p := pipeline.NewPipeline(r, invoker.NewInvoker(ctr))
	p.AddArgumentResolver(&resolver.DTOResolver{})
	p.AddReturnValueHandler(&handler.StringReturnHandler{})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader("{"))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	echoCtx := e.NewContext(req, recorder)
	ctx := NewContext(echoCtx)
	ctx.Set("spine.response_writer", NewEchoResponseWriter(echoCtx))

	if err := p.Execute(ctx); err == nil {
		t.Fatal("잘못된 JSON은 실행 오류여야 합니다")
	}
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("잘못된 JSON 응답 상태 = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
}

func TestNormalizeRequestError_PreservesMaxBytesError(t *testing.T) {
	maxBytesErr := &http.MaxBytesError{Limit: 64}
	err := normalizeRequestError(echo.NewHTTPError(http.StatusBadRequest).SetInternal(maxBytesErr))
	var got *http.MaxBytesError
	if !errors.As(err, &got) || got.Limit != 64 {
		t.Fatalf("요청 크기 초과 오류를 보존해야 합니다: %T %v", err, err)
	}
}

func TestNormalizeRequestError_MapsRawRequestErrorToBadRequest(t *testing.T) {
	err := normalizeRequestError(errors.New("malformed multipart body"))
	var httpErr *httperr.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest {
		t.Fatalf("요청 파싱 오류는 400 Spine 오류여야 합니다: %T %v", err, err)
	}
}

func TestNewServer_AppliesSecureDefaults(t *testing.T) {
	server := NewServer(nil, ":0", nil, boot.HTTPOptions{})

	if server.httpServer.ReadHeaderTimeout != defaultReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout 기본값이 적용되지 않았습니다: %s", server.httpServer.ReadHeaderTimeout)
	}
	if server.httpServer.ReadTimeout != defaultReadTimeout {
		t.Fatalf("ReadTimeout 기본값이 적용되지 않았습니다: %s", server.httpServer.ReadTimeout)
	}
	if server.httpServer.WriteTimeout != defaultWriteTimeout {
		t.Fatalf("WriteTimeout 기본값이 적용되지 않았습니다: %s", server.httpServer.WriteTimeout)
	}
	if server.httpServer.IdleTimeout != defaultIdleTimeout {
		t.Fatalf("IdleTimeout 기본값이 적용되지 않았습니다: %s", server.httpServer.IdleTimeout)
	}
	if server.httpServer.MaxHeaderBytes != defaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes 기본값이 적용되지 않았습니다: %d", server.httpServer.MaxHeaderBytes)
	}
}

func TestNewServer_AppliesCustomOptions(t *testing.T) {
	server := NewServer(nil, ":0", nil, boot.HTTPOptions{
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      4 * time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4096,
		MaxBodyBytes:      128,
	})

	if server.httpServer.ReadHeaderTimeout != 2*time.Second {
		t.Fatalf("ReadHeaderTimeout가 반영되지 않았습니다: %s", server.httpServer.ReadHeaderTimeout)
	}
	if server.httpServer.ReadTimeout != 3*time.Second {
		t.Fatalf("ReadTimeout가 반영되지 않았습니다: %s", server.httpServer.ReadTimeout)
	}
	if server.httpServer.WriteTimeout != 4*time.Second {
		t.Fatalf("WriteTimeout가 반영되지 않았습니다: %s", server.httpServer.WriteTimeout)
	}
	if server.httpServer.IdleTimeout != 5*time.Second {
		t.Fatalf("IdleTimeout가 반영되지 않았습니다: %s", server.httpServer.IdleTimeout)
	}
	if server.httpServer.MaxHeaderBytes != 4096 {
		t.Fatalf("MaxHeaderBytes가 반영되지 않았습니다: %d", server.httpServer.MaxHeaderBytes)
	}
}
