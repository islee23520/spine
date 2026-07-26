package boot

import (
	"net/url"
	"time"
)

/*
RabbitMqOptions는 RabbitMQ 사용 여부와 Read(읽기), Write(쓰기) 역할을 정의합니다.
Read와 Write는 서로 독립적이므로 하나만 설정해도 동작합니다.
*/
type RabbitMqOptions struct {
	/*
		URL은 RabbitMQ AMQP 연결 문자열입니다.
		예: amqp://guest:guest@localhost:5672/
	*/
	URL string

	// AllowInsecureTransport는 amqp:// URL을 허용합니다. 격리된 개발 환경에서만 사용해야 하며,
	// 기본적으로는 amqps://가 필요합니다.
	AllowInsecureTransport bool

	// ConsumerRetry는 전송 계층 또는 읽기 오류 후 브로커 재연결 방식을 제어합니다.
	// 아래의 메시지별 FailurePolicy와는 별개입니다.
	ConsumerRetry ConsumerRetryOptions

	// PublisherRetry는 연결/채널 단절 또는 publisher confirm 실패 후 재연결과
	// 재발행 방식을 제어합니다. 재발행은 at-least-once 전달이므로 호출자는 이벤트를
	// 멱등하게 처리해야 합니다.
	PublisherRetry PublisherRetryOptions

	/*
		Read는 이벤트 소비(컨슈머) 설정입니다.
		nil이면 RabbitMQ 컨슈머 런타임은 활성화되지 않습니다.
	*/
	Read *RabbitMqReadOptions

	/*
		Write는 이벤트 발행(퍼블리셔) 설정입니다.
		nil이면 RabbitMQ로 이벤트를 발행하지 않습니다.
	*/
	Write *RabbitMqWriteOptions
}

/*
RabbitMqReadOptions는 RabbitMQ 컨슈머(런타임) 설정입니다.
큐 선언과 Exchange 바인딩을 담당합니다.
*/
type RabbitMqReadOptions struct {
	// Exchange는 큐가 바인딩될 Exchange 이름입니다.
	Exchange string

	// PrefetchCount는 한 consumer에 동시에 전달될 수 있는 미확인 메시지 수입니다.
	// 0이면 안전한 기본값 1을 사용하며, 설정할 때는 양수여야 합니다.
	PrefetchCount int

	// FailurePolicy는 핸들러 실패 후 수행할 동작을 명시적으로 선택합니다.
	// 별도로 지정하지 않으면 유해 메시지의 반복 처리를 막기 위해 재큐잉하지 않고 거부합니다.
	FailurePolicy RabbitMqFailurePolicy

	// DeadLetter는 큐의 데드 레터 대상을 설정합니다. 거부된 메시지는 브로커에 일치하는
	// DLX 토폴로지가 구성되어 있을 때만 보존됩니다.
	DeadLetter *RabbitMqDeadLetterOptions

	// RequeueOnError는 실패 메시지를 재큐잉할지 결정합니다.
	// 기본값 false는 유해 메시지의 무한 재전달을 방지합니다.
	// Deprecated: 대신 FailurePolicy를 사용하세요. true 값은 이전 버전과 호환되며
	// RabbitMqFailureRequeue로 매핑됩니다.
	RequeueOnError bool
}

// EffectivePrefetchCount는 RabbitMQ consumer에 적용할 실제 QoS prefetch 수를 반환합니다.
func (o RabbitMqReadOptions) EffectivePrefetchCount() int {
	if o.PrefetchCount == 0 {
		return 1
	}
	return o.PrefetchCount
}

// PublisherRetryOptions는 RabbitMQ publish 전송 실패 후의 재연결 backoff와
// broker confirm 대기 시간을 제어합니다. 0 값은 각각 100ms, 5s, 2배,
// 20% jitter, 최대 3회 시도, confirm 5s를 사용합니다.
type PublisherRetryOptions struct {
	InitialDelay   time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	Jitter         float64
	MaxAttempts    int
	ConfirmTimeout time.Duration
}

type RabbitMqFailurePolicy string

const (
	RabbitMqFailureReject  RabbitMqFailurePolicy = "reject"
	RabbitMqFailureRequeue RabbitMqFailurePolicy = "requeue"
)

type RabbitMqDeadLetterOptions struct {
	Exchange   string
	RoutingKey string
}

func (o RabbitMqReadOptions) EffectiveFailurePolicy() RabbitMqFailurePolicy {
	if o.FailurePolicy != "" {
		return o.FailurePolicy
	}
	if o.RequeueOnError {
		return RabbitMqFailureRequeue
	}
	return RabbitMqFailureReject
}

// ValidateIssues는 네트워크에 연결하지 않고 RabbitMQ 설정을 검증합니다.
func (o RabbitMqOptions) ValidateIssues(prefix string) []ConfigIssue {
	if prefix == "" {
		prefix = "RabbitMQ"
	}
	if o.Read == nil && o.Write == nil {
		return nil
	}
	var issues []ConfigIssue
	parsedURL, parseErr := url.Parse(o.URL)
	if o.URL == "" {
		issues = append(issues, ConfigIssue{configPath(prefix, "URL"), "RABBITMQ_URL_REQUIRED", "RabbitMQ URL is empty.", "Configure an amqps:// URL."})
	} else if parseErr != nil || parsedURL.Host == "" || (parsedURL.Scheme != "amqp" && parsedURL.Scheme != "amqps") {
		issues = append(issues, ConfigIssue{configPath(prefix, "URL"), "RABBITMQ_URL_INVALID", "RabbitMQ URL is invalid.", "Use an amqp:// or amqps:// URL. Credentials are not included in this error."})
	} else if parsedURL.Scheme == "amqp" && !o.AllowInsecureTransport {
		issues = append(issues, ConfigIssue{configPath(prefix, "URL"), "RABBITMQ_AMQPS_REQUIRED", "Plaintext amqp:// is not allowed by default.", "Use amqps:// or explicitly allow insecure local transport."})
	} else if parsedURL.Scheme == "amqps" && o.AllowInsecureTransport {
		issues = append(issues, ConfigIssue{configPath(prefix, "AllowInsecureTransport"), "RABBITMQ_TLS_INSECURE_CONFLICT", "Secure amqps:// and insecure transport opt-in cannot be enabled together.", "Remove AllowInsecureTransport when using amqps://."})
	}
	if o.Read != nil {
		if o.Read.Exchange == "" {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.Exchange"), "RABBITMQ_EXCHANGE_REQUIRED", "RabbitMQ consumer exchange is empty.", "Set a named topic exchange."})
		}
		if o.Read.PrefetchCount < 0 {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.PrefetchCount"), "RABBITMQ_PREFETCH_COUNT_INVALID", "RabbitMQ prefetch count cannot be negative.", "Use zero for the safe default of 1 or a positive count."})
		}
		policy := o.Read.EffectiveFailurePolicy()
		if policy != RabbitMqFailureReject && policy != RabbitMqFailureRequeue {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.FailurePolicy"), "RABBITMQ_FAILURE_POLICY_INVALID", "RabbitMQ failure policy is not recognized.", "Use RabbitMqFailureReject or RabbitMqFailureRequeue."})
		}
		if o.Read.RequeueOnError && o.Read.FailurePolicy != "" && o.Read.FailurePolicy != RabbitMqFailureRequeue {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.RequeueOnError"), "RABBITMQ_FAILURE_POLICY_CONFLICT", "Deprecated RequeueOnError conflicts with FailurePolicy.", "Remove RequeueOnError and use FailurePolicy only."})
		}
		if o.Read.DeadLetter != nil && o.Read.DeadLetter.Exchange == "" {
			issues = append(issues, ConfigIssue{configPath(prefix, "Read.DeadLetter.Exchange"), "RABBITMQ_DLX_EXCHANGE_REQUIRED", "Dead-letter exchange is empty.", "Set the pre-provisioned dead-letter exchange name."})
		}
		issues = append(issues, o.ConsumerRetry.validateIssues(configPath(prefix, "ConsumerRetry"))...)
	}
	if o.Write != nil && o.Write.Exchange == "" {
		issues = append(issues, ConfigIssue{
			Path:    configPath(prefix, "Write.Exchange"),
			Code:    "RABBITMQ_WRITE_EXCHANGE_REQUIRED",
			Message: "RabbitMQ publisher exchange is empty.",
			Hint:    "Set a named topic exchange for published events.",
		})
	}
	if o.Write != nil {
		issues = append(issues, o.PublisherRetry.validateIssues(configPath(prefix, "PublisherRetry"))...)
	}
	return issues
}

func (o RabbitMqOptions) Validate() error { return ValidationError(o.ValidateIssues("RabbitMQ")...) }

/*
RabbitMqWriteOptions는 RabbitMQ 퍼블리셔 설정입니다.
Exchange 선언과 메시지 발행을 담당합니다.
*/
type RabbitMqWriteOptions struct {
	// Exchange는 이벤트를 발행할 대상 Exchange 이름입니다.
	Exchange string
}

func (o PublisherRetryOptions) validateIssues(prefix string) []ConfigIssue {
	var issues []ConfigIssue
	if o.InitialDelay < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "InitialDelay"), "RABBITMQ_PUBLISHER_RETRY_INITIAL_DELAY_INVALID", "Initial delay cannot be negative.", "Use zero for the default or a positive duration."})
	}
	if o.MaxDelay < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxDelay"), "RABBITMQ_PUBLISHER_RETRY_MAX_DELAY_INVALID", "Maximum delay cannot be negative.", "Use zero for the default or a positive duration."})
	}
	if o.InitialDelay > 0 && o.MaxDelay > 0 && o.MaxDelay < o.InitialDelay {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxDelay"), "RABBITMQ_PUBLISHER_RETRY_DELAY_RANGE_INVALID", "Maximum delay is shorter than the initial delay.", "Set MaxDelay greater than or equal to InitialDelay."})
	}
	if o.Multiplier < 0 || (o.Multiplier > 0 && o.Multiplier < 1) {
		issues = append(issues, ConfigIssue{configPath(prefix, "Multiplier"), "RABBITMQ_PUBLISHER_RETRY_MULTIPLIER_INVALID", "Multiplier must be at least 1.", "Use zero for the default or a value of 1 or greater."})
	}
	if o.Jitter < 0 || o.Jitter > 1 {
		issues = append(issues, ConfigIssue{configPath(prefix, "Jitter"), "RABBITMQ_PUBLISHER_RETRY_JITTER_INVALID", "Jitter must be between 0 and 1.", "Use zero for the default or a fraction such as 0.2."})
	}
	if o.MaxAttempts < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "MaxAttempts"), "RABBITMQ_PUBLISHER_RETRY_MAX_ATTEMPTS_INVALID", "Maximum attempts cannot be negative.", "Use zero for the default of 3 attempts or a positive attempt count."})
	}
	if o.ConfirmTimeout < 0 {
		issues = append(issues, ConfigIssue{configPath(prefix, "ConfirmTimeout"), "RABBITMQ_PUBLISHER_CONFIRM_TIMEOUT_INVALID", "Publisher confirm timeout cannot be negative.", "Use zero for the default or a positive duration."})
	}
	return issues
}
