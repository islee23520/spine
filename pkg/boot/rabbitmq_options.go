package boot

import "net/url"

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
