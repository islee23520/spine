package core

import (
	"context"
	"mime/multipart"
)

type ContextCarrier interface {
	Context() context.Context
}

type EventBusCarrier interface {
	EventBus() EventBus
}

/*
ExecutionContext
- 파이프라인과 라우터 전용
- HTTP 전송 실행 흐름에서만 사용
*/
type ExecutionContext interface {
	ContextCarrier
	EventBusCarrier

	Method() string
	Path() string
	Params() map[string]string
	Header(name string) string
	PathKeys() []string
	Queries() map[string][]string
	Set(key string, value any)
	Get(key string) (any, bool)
}

/*
ControllerContext
- 컨트롤러 전용 컨텍스트 뷰
- ExecutionContext의 읽기 전용 퍼사드
- 인터셉터에서 주입한 값을 컨트롤러에서 참조하는 공식 통로
*/
type ControllerContext interface {
	Get(key string) (any, bool)
}

/*
HttpRequestContext
- HTTP 전용 컨텍스트 계약
*/
type HttpRequestContext interface {
	ContextCarrier
	EventBusCarrier

	// 개별 접근
	Param(name string) string
	Query(name string) string
	Header(name string) string

	// 전체 뷰 접근
	Params() map[string]string
	Queries() map[string][]string
	Headers() map[string][]string

	// 요청 본문
	Bind(out any) error

	// 멀티파트
	MultipartForm() (*multipart.Form, error)
}

/*
ConsumerRequestContext
- 이벤트 컨슈머 전용 컨텍스트
*/
type ConsumerRequestContext interface {
	ContextCarrier
	EventBusCarrier

	EventName() string
	Payload() []byte
}

/*
WebSocketContext
- WebSocket 전용 ExecutionContext 확장
*/
type WebSocketContext interface {
	ExecutionContext

	ConnID() string
	MessageType() int
	Payload() []byte
}

/*
WebSocketRequestContext
- WebSocket upgrade 요청에서 보존한 불변 요청 정보 뷰
- WebSocketContext와 별도인 추가 계약이므로 기존 사용자 구현을 깨지 않습니다.
*/
type WebSocketRequestContext interface {
	ContextCarrier

	Path() string
	Header(name string) string
	Headers() map[string][]string
	Query(name string) string
	Queries() map[string][]string
	Cookie(name string) (string, bool)
	Cookies() map[string]string
	RemoteAddr() string
	Host() string
	RequestURI() string
}

// WebSocketHandshakeContext는 HTTP upgrade 전에 인증/인가에 사용할 요청 뷰입니다.
type WebSocketHandshakeContext interface {
	WebSocketRequestContext
}

// WebSocketMessageContext는 메시지 처리와 원래 handshake 요청 정보를 함께 제공합니다.
type WebSocketMessageContext interface {
	WebSocketContext
	WebSocketRequestContext
}
