package core

import (
	"context"
	"reflect"
)

// CustomTransport는 Spine HTTP 파이프라인 외부에서 독립 실행되는 전송 방식의 계약입니다.
type CustomTransport interface {
	// Init은 DI 컨테이너가 준비된 후 호출됩니다.
	Init(container Container) error
	// Start는 Init 이후 별도 고루틴에서 호출됩니다.
	Start() error
	// Stop은 정상 종료 시 호출됩니다.
	Stop(ctx context.Context) error
}

// Container는 CustomTransport에서 DI에 접근할 때 사용하는 퍼사드입니다.
type Container interface {
	Resolve(t reflect.Type) (any, error)
}
