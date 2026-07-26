package core

import (
	"reflect"
)

// HandlerMeta는 실제로 실행할 핸들러에 대한 메타데이터입니다.
type HandlerMeta struct {
	// 컨트롤러 타입(Container의 Resolve 대상)
	ControllerType reflect.Type
	// 호출할 메서드 이름
	Method reflect.Method
	// 라우트에 선언된 경로 매개변수 키의 순서
	PathKeys []string
	// 핸들러에 적용된 인터셉터
	Interceptors []Interceptor
}
