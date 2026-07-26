package core

// ResponseWriter는 전송 구현(Echo, net/http 등)에 의존하지 않는
// Spine의 응답 출력 계약이다.
// 실제 구현은 어댑터 계층에서 제공한다.
type ResponseWriter interface {
	// 헤더 조작
	SetHeader(key, value string)
	AddHeader(key, value string)

	// 응답이 이미 커밋(헤더/바디 작성 시작)되었는지 여부
	IsCommitted() bool

	// 상태 코드만 기록(응답 본문 없음)
	WriteStatus(status int) error

	// 응답 본문과 상태 코드 기록
	WriteJSON(status int, value any) error
	WriteString(status int, value string) error
	WriteBytes(status int, value []byte) error
}
