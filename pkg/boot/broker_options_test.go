package boot

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestConfigErrorFormatting(t *testing.T) {
	err := (&ConfigError{Issues: []ConfigIssue{
		{Path: "Kafka.Brokers", Code: "KAFKA_BROKERS_REQUIRED", Message: "Kafka brokers are not configured.", Hint: "Provide at least one broker address."},
		{Path: "RabbitMQ.URL", Code: "RABBITMQ_URL_REQUIRED", Message: "RabbitMQ URL is empty."},
	}}).Error()
	want := "configuration validation failed with 2 issue(s):\n" +
		"- Kafka.Brokers [KAFKA_BROKERS_REQUIRED]: Kafka brokers are not configured.\n" +
		"  Hint: Provide at least one broker address.\n" +
		"- RabbitMQ.URL [RABBITMQ_URL_REQUIRED]: RabbitMQ URL is empty."
	if err != want {
		t.Fatalf("unexpected deterministic error formatting:\n%s\nwant:\n%s", err, want)
	}
}

func TestKafkaOptionsValidateIssuesSupportsSharedTLS(t *testing.T) {
	opts := KafkaOptions{
		Brokers: []string{"broker.example:9093"},
		TLS:     &tls.Config{MinVersion: tls.VersionTLS12},
		Read:    &KafkaReadOptions{GroupID: "orders"},
		Write:   &KafkaWriteOptions{},
	}
	if issues := opts.ValidateIssues("Kafka"); len(issues) != 0 {
		t.Fatalf("shared TLS should satisfy reader and writer: %+v", issues)
	}
}

func TestKafkaOptionsValidateIssuesAllowsDisabledWrapper(t *testing.T) {
	opts := KafkaOptions{}
	if issues := opts.ValidateIssues("Kafka"); len(issues) != 0 {
		t.Fatalf("Kafka wrapper without an enabled role must remain disabled: %+v", issues)
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("disabled Kafka wrapper must pass validation: %v", err)
	}
}

func TestKafkaOptionsValidateIssuesReportsAllActionableProblems(t *testing.T) {
	opts := KafkaOptions{Read: &KafkaReadOptions{}, Write: &KafkaWriteOptions{}}
	issues := opts.ValidateIssues("Messaging.Kafka")
	if len(issues) != 2 {
		t.Fatalf("expected brokers and group ID issues; got %+v", issues)
	}
	if issues[0].Path != "Messaging.Kafka.Brokers" {
		t.Fatalf("path prefix was not preserved: %+v", issues[0])
	}
}

func TestKafkaOptionsValidateIssuesUsesImplicitSecureTransport(t *testing.T) {
	opts := KafkaOptions{
		Brokers: []string{"broker.example:9093"},
		Read:    &KafkaReadOptions{GroupID: "orders"},
		Write:   &KafkaWriteOptions{},
	}
	if issues := opts.ValidateIssues("Kafka"); len(issues) != 0 {
		t.Fatalf("nil TLS must select Spine's secure default: %+v", issues)
	}
}

func TestKafkaOptionsValidateIssuesRejectsTLSInsecureConflict(t *testing.T) {
	opts := KafkaOptions{
		Brokers:                []string{"broker.example:9093"},
		TLS:                    &tls.Config{MinVersion: tls.VersionTLS12},
		AllowInsecureTransport: true,
		Write:                  &KafkaWriteOptions{},
	}
	if issues := opts.ValidateIssues("Kafka"); len(issues) != 1 || issues[0].Code != "KAFKA_TLS_INSECURE_CONFLICT" {
		t.Fatalf("expected TLS/insecure conflict: %+v", issues)
	}
}

func TestKafkaOptionsValidateIssuesIgnoresSecureOverrideForDisabledRole(t *testing.T) {
	readerOnly := KafkaOptions{
		Brokers:                []string{"localhost:9092"},
		Transport:              &kafka.Transport{TLS: &tls.Config{}},
		AllowInsecureTransport: true,
		Read:                   &KafkaReadOptions{GroupID: "local"},
	}
	if issues := readerOnly.ValidateIssues("Kafka"); len(issues) != 0 {
		t.Fatalf("unused writer transport must not conflict with reader-only config: %+v", issues)
	}

	writerOnly := KafkaOptions{
		Brokers:                []string{"localhost:9092"},
		Dialer:                 &kafka.Dialer{TLS: &tls.Config{}},
		AllowInsecureTransport: true,
		Write:                  &KafkaWriteOptions{},
	}
	if issues := writerOnly.ValidateIssues("Kafka"); len(issues) != 0 {
		t.Fatalf("unused reader dialer must not conflict with writer-only config: %+v", issues)
	}
}

func TestRabbitMqReadOptionsEffectiveFailurePolicyCompatibility(t *testing.T) {
	if got := (RabbitMqReadOptions{}).EffectiveFailurePolicy(); got != RabbitMqFailureReject {
		t.Fatalf("zero value should safely reject, got %q", got)
	}
	if got := (RabbitMqReadOptions{RequeueOnError: true}).EffectiveFailurePolicy(); got != RabbitMqFailureRequeue {
		t.Fatalf("legacy true must remain compatible, got %q", got)
	}
}

func TestRabbitMqOptionsValidateIssuesAllowsDisabledWrapper(t *testing.T) {
	opts := RabbitMqOptions{}
	if issues := opts.ValidateIssues("RabbitMQ"); len(issues) != 0 {
		t.Fatalf("RabbitMQ wrapper without an enabled role must remain disabled: %+v", issues)
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("disabled RabbitMQ wrapper must pass validation: %v", err)
	}
}

func TestRabbitMqOptionsValidateIssuesRejectsConflictingPolicies(t *testing.T) {
	opts := RabbitMqOptions{
		URL: "amqps://broker.example:5671/",
		Read: &RabbitMqReadOptions{
			Exchange:       "events",
			FailurePolicy:  RabbitMqFailureReject,
			RequeueOnError: true,
		},
	}
	issues := opts.ValidateIssues("RabbitMQ")
	if len(issues) != 1 || issues[0].Code != "RABBITMQ_FAILURE_POLICY_CONFLICT" {
		t.Fatalf("expected policy conflict: %+v", issues)
	}
}

func TestRabbitMqOptionsValidateIssuesRequiresWriteExchange(t *testing.T) {
	opts := RabbitMqOptions{
		URL:   "amqps://broker.example:5671/",
		Write: &RabbitMqWriteOptions{},
	}
	issues := opts.ValidateIssues("Messaging.RabbitMQ")
	if len(issues) != 1 {
		t.Fatalf("expected one write exchange issue, got %+v", issues)
	}
	if issues[0].Code != "RABBITMQ_WRITE_EXCHANGE_REQUIRED" || issues[0].Path != "Messaging.RabbitMQ.Write.Exchange" {
		t.Fatalf("unexpected structured issue: %+v", issues[0])
	}
}

func TestRabbitMqReadOptionsEffectivePrefetchCount(t *testing.T) {
	if got := (RabbitMqReadOptions{}).EffectivePrefetchCount(); got != 1 {
		t.Fatalf("zero value must select safe prefetch 1, got %d", got)
	}
	if got := (RabbitMqReadOptions{PrefetchCount: 32}).EffectivePrefetchCount(); got != 32 {
		t.Fatalf("explicit positive prefetch must be preserved, got %d", got)
	}
}

func TestRabbitMqOptionsValidateIssuesRejectsInvalidPrefetch(t *testing.T) {
	opts := RabbitMqOptions{
		URL: "amqps://broker.example:5671/",
		Read: &RabbitMqReadOptions{
			Exchange:      "events",
			PrefetchCount: -1,
		},
	}
	issues := opts.ValidateIssues("RabbitMQ")
	if len(issues) != 1 || issues[0].Code != "RABBITMQ_PREFETCH_COUNT_INVALID" {
		t.Fatalf("expected invalid prefetch issue: %+v", issues)
	}
}

func TestRabbitMqOptionsValidateIssuesRejectsInvalidPublisherRetry(t *testing.T) {
	opts := RabbitMqOptions{
		URL:   "amqps://broker.example:5671/",
		Write: &RabbitMqWriteOptions{Exchange: "events"},
		PublisherRetry: PublisherRetryOptions{
			InitialDelay:   2 * time.Second,
			MaxDelay:       time.Second,
			Multiplier:     0.5,
			Jitter:         1.1,
			MaxAttempts:    -1,
			ConfirmTimeout: -time.Second,
		},
	}
	issues := opts.ValidateIssues("RabbitMQ")
	expectedCodes := map[string]struct{}{
		"RABBITMQ_PUBLISHER_RETRY_DELAY_RANGE_INVALID":  {},
		"RABBITMQ_PUBLISHER_RETRY_MULTIPLIER_INVALID":   {},
		"RABBITMQ_PUBLISHER_RETRY_JITTER_INVALID":       {},
		"RABBITMQ_PUBLISHER_RETRY_MAX_ATTEMPTS_INVALID": {},
		"RABBITMQ_PUBLISHER_CONFIRM_TIMEOUT_INVALID":    {},
	}
	for _, issue := range issues {
		if _, ok := expectedCodes[issue.Code]; !ok {
			t.Fatalf("예상하지 못한 publisher retry issue입니다: %+v", issue)
		}
		delete(expectedCodes, issue.Code)
	}
	if len(expectedCodes) != 0 {
		t.Fatalf("누락된 publisher retry issue code가 있습니다: %v (actual=%+v)", expectedCodes, issues)
	}
}
