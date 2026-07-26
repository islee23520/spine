package boot

import (
	"crypto/tls"
	"time"

	"github.com/segmentio/kafka-go"
)

/*
Kafka 관련 설정을 담는 옵션 구조체입니다.
Spine 부트스트랩 단계에서 Kafka 프로듀서와 컨슈머 구성을 제어합니다.
*/
type KafkaOptions struct {
	// Kafka 브로커 주소 목록
	Brokers []string

	// TLS는 컨슈머와 퍼블리셔가 함께 사용하는 사용자 지정 설정입니다. nil이면 Spine이
	// TLS 1.2 이상의 안전한 기본 설정을 생성합니다.
	TLS *tls.Config

	// Dialer는 리더의 고급 TLS/SASL 전송 방식을 설정합니다. nil이면 Spine이 안전한
	// 기본 Dialer를 생성합니다.
	Dialer *kafka.Dialer

	// Transport는 라이터의 고급 TLS/SASL 전송 방식을 설정합니다. nil이면 Spine이 안전한
	// 기본 Transport를 생성합니다.
	Transport *kafka.Transport

	// AllowInsecureTransport는 Kafka의 평문 기본 연결을 허용합니다. 격리된 개발 환경에서만
	// 사용해야 하며 명시적으로 활성화해야 합니다.
	AllowInsecureTransport bool

	// ConsumerRetry는 브로커 읽기 또는 연결 실패 후 복구 방식을 제어합니다. 핸들러 실패를
	// 재시도하거나 메시지 ACK/NACK 동작을 변경하지는 않습니다.
	ConsumerRetry ConsumerRetryOptions

	/*
		이벤트 소비(컨슈머) 설정
		nil이면 Kafka 컨슈머 런타임은 활성화되지 않습니다.
	*/
	Read *KafkaReadOptions

	/*
		이벤트 발행(프로듀서) 설정
		nil이면 Kafka로 이벤트를 발행하지 않습니다.
	*/
	Write *KafkaWriteOptions
}

// ConsumerRetryOptions는 Reader 오류 후 전송 계층의 재연결 방식을 제어합니다.
// 별도로 지정하지 않으면 안전한 기본값인 최초 지연 100ms, 최대 지연 5초,
// 배수 2, 지터 20%, 무제한 재시도를 사용합니다.
type ConsumerRetryOptions struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
	MaxAttempts  int
}

// ValidateIssues는 네트워크에 연결하지 않고 Kafka 설정을 검증합니다.
func (o KafkaOptions) ValidateIssues(prefix string) []ConfigIssue {
	if prefix == "" {
		prefix = "Kafka"
	}
	if o.Read == nil && o.Write == nil {
		return nil
	}
	var issues []ConfigIssue
	if len(o.Brokers) == 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "Brokers"), "KAFKA_BROKERS_REQUIRED", "Kafka brokers are not configured.", "Provide at least one broker address."})
	}
	secureReader := o.TLS != nil || (o.Dialer != nil && o.Dialer.TLS != nil)
	secureWriter := o.TLS != nil || (o.Transport != nil && o.Transport.TLS != nil)
	if o.AllowInsecureTransport && ((o.Read != nil && secureReader) || (o.Write != nil && secureWriter)) {
		issues = append(issues, ConfigIssue{configPath(prefix, "AllowInsecureTransport"), "KAFKA_TLS_INSECURE_CONFLICT", "TLS and insecure transport opt-in cannot be enabled together.", "Remove AllowInsecureTransport when TLS is configured."})
	}
	if o.Read != nil {
		if o.Read.GroupID == "" {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.GroupID"), "KAFKA_GROUP_ID_REQUIRED", "Kafka consumer group ID is empty.", "Set a stable consumer group ID."})
		}
		issues = append(issues, o.ConsumerRetry.validateIssues(configPath(prefix, "ConsumerRetry"))...)
	}
	return issues
}

func (o KafkaOptions) Validate() error { return ValidationError(o.ValidateIssues("Kafka")...) }

func (o ConsumerRetryOptions) validateIssues(prefix string) []ConfigIssue {
	var issues []ConfigIssue
	if o.InitialDelay < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "InitialDelay"), "CONSUMER_RETRY_INITIAL_DELAY_INVALID", "Initial delay cannot be negative.", "Use zero for the default or a positive duration."})
	}
	if o.MaxDelay < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxDelay"), "CONSUMER_RETRY_MAX_DELAY_INVALID", "Maximum delay cannot be negative.", "Use zero for the default or a positive duration."})
	}
	if o.InitialDelay > 0 && o.MaxDelay > 0 && o.MaxDelay < o.InitialDelay {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxDelay"), "CONSUMER_RETRY_DELAY_RANGE_INVALID", "Maximum delay is shorter than the initial delay.", "Set MaxDelay greater than or equal to InitialDelay."})
	}
	if o.Multiplier < 0 || (o.Multiplier > 0 && o.Multiplier < 1) {
		issues = append(issues, ConfigIssue{configPath(prefix, "Multiplier"), "CONSUMER_RETRY_MULTIPLIER_INVALID", "Multiplier must be at least 1.", "Use zero for the default or a value of 1 or greater."})
	}
	if o.Jitter < 0 || o.Jitter > 1 {
		issues = append(issues, ConfigIssue{configPath(prefix, "Jitter"), "CONSUMER_RETRY_JITTER_INVALID", "Jitter must be between 0 and 1.", "Use zero for the default or a fraction such as 0.2."})
	}
	if o.MaxAttempts < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxAttempts"), "CONSUMER_RETRY_MAX_ATTEMPTS_INVALID", "Maximum attempts cannot be negative.", "Use zero for unlimited retries or a positive attempt count."})
	}
	return issues
}

/*
Kafka 이벤트 발행 시 사용되는 설정입니다.
토픽 이름 규칙과 관련된 정책을 정의합니다.
*/
type KafkaWriteOptions struct {
	// 이벤트 이름 앞에 붙일 토픽 접두사
	TopicPrefix string
}

/*
Kafka 이벤트 소비 시 사용되는 설정입니다.
컨슈머 그룹 단위의 실행을 제어합니다.
*/
type KafkaReadOptions struct {
	// Kafka 컨슈머 그룹 식별자
	GroupID string
}
