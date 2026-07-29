package echo

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
