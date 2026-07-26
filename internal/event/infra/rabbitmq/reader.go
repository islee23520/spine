package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/rabbitmq/amqp091-go"
)

type Reader struct {
	conn          *amqp091.Connection
	channel       *amqp091.Channel
	msgs          <-chan amqp091.Delivery
	failurePolicy RabbitMqFailurePolicy
}

type RabbitMqOptions struct {
	URL                    string
	Read                   *RabbitMqReadOptions
	AllowInsecureTransport bool
}

type RabbitMqReadOptions struct {
	Queue      string
	Exchange   string
	RoutingKey string
	// RequeueOnError는 전달에 실패한 메시지를 다시 큐에 넣어 재시도합니다.
	// 영구적으로 잘못된 메시지가 컨슈머에서 무한 반복되지 않도록 기본값은 비활성화입니다.
	FailurePolicy  RabbitMqFailurePolicy
	DeadLetter     *RabbitMqDeadLetterOptions
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

func NewRabbitMqReader(opts RabbitMqOptions) (*Reader, error) {
	if opts.Read == nil {
		return nil, errors.New("RabbitMQ read options are not configured")
	}
	if opts.Read.Exchange == "" {
		return nil, errors.New("RabbitMQ default exchange is not supported by Spine")
	}
	if err := validateBrokerURL(opts.URL, opts.AllowInsecureTransport); err != nil {
		return nil, err
	}
	if err := validateReadOptions(opts.Read); err != nil {
		return nil, err
	}

	conn, err := amqp091.Dial(opts.URL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		opts.Read.Exchange,
		"topic",
		true,  // 영속 교환기
		false, // 자동 삭제 안 함
		false, // 내부용 아님
		false, // 서버 응답을 기다림
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	var queueArgs amqp091.Table
	if opts.Read.DeadLetter != nil {
		queueArgs = amqp091.Table{"x-dead-letter-exchange": opts.Read.DeadLetter.Exchange}
		if opts.Read.DeadLetter.RoutingKey != "" {
			queueArgs["x-dead-letter-routing-key"] = opts.Read.DeadLetter.RoutingKey
		}
	}

	_, err = ch.QueueDeclare(
		opts.Read.Queue,
		true,  // 영속 큐
		false, // 자동 삭제 안 함
		false, // 전용 큐 아님
		false, // 서버 응답을 기다림
		queueArgs,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		opts.Read.Queue,
		opts.Read.RoutingKey,
		opts.Read.Exchange,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	msgs, err := ch.Consume(
		opts.Read.Queue,
		"",
		false, // 자동 ACK를 사용하지 않음
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Reader{
		conn:          conn,
		channel:       ch,
		msgs:          msgs,
		failurePolicy: effectiveFailurePolicy(opts.Read),
	}, nil
}

func validateReadOptions(opts *RabbitMqReadOptions) error {
	if opts.Queue == "" {
		return errors.New("RabbitMQ read queue cannot be empty")
	}
	if opts.RoutingKey == "" {
		return errors.New("RabbitMQ read routing key cannot be empty")
	}
	policy := effectiveFailurePolicy(opts)
	if policy != RabbitMqFailureReject && policy != RabbitMqFailureRequeue {
		return fmt.Errorf("RabbitMQ failure policy %q is invalid", policy)
	}
	if opts.RequeueOnError && opts.FailurePolicy != "" && opts.FailurePolicy != RabbitMqFailureRequeue {
		return errors.New("RabbitMQ RequeueOnError conflicts with FailurePolicy")
	}
	if opts.DeadLetter != nil && opts.DeadLetter.Exchange == "" {
		return errors.New("RabbitMQ dead-letter exchange cannot be empty")
	}
	return nil
}

func effectiveFailurePolicy(opts *RabbitMqReadOptions) RabbitMqFailurePolicy {
	if opts.FailurePolicy != "" {
		return opts.FailurePolicy
	}
	if opts.RequeueOnError {
		return RabbitMqFailureRequeue
	}
	return RabbitMqFailureReject
}

func validateBrokerURL(rawURL string, allowInsecure bool) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("RabbitMQ URL is invalid")
	}
	if parsed.Scheme != "amqps" && !allowInsecure {
		return errors.New("RabbitMQ requires an amqps URL; set AllowInsecureTransport only for isolated development")
	}
	if parsed.Scheme != "amqp" && parsed.Scheme != "amqps" {
		return errors.New("RabbitMQ URL must use amqp or amqps")
	}
	if parsed.Scheme == "amqps" && allowInsecure {
		return errors.New("RabbitMQ secure URL conflicts with AllowInsecureTransport")
	}
	return nil
}

func (r *Reader) Read(ctx context.Context) (*consumer.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case msg, ok := <-r.msgs:
		if !ok {
			return nil, errors.New("RabbitMQ channel is closed")
		}

		// RoutingKey는 큐 바인딩을 선택하는 신뢰할 수 있는 디스패치 식별자입니다.
		// AMQP Type은 생산자가 제어하는 메타데이터이므로 등록된 다른 컨슈머를
		// 선택하는 데 사용해서는 안 됩니다.
		eventName := msg.RoutingKey
		if msg.Type != "" && msg.Type != msg.RoutingKey {
			log.Printf("[RabbitMQ][Read] Message type differs from routing key (type=%q routing_key=%q dispatch_key=%q)", msg.Type, msg.RoutingKey, eventName)
		}

		consumerMsg := &consumer.Message{
			EventName: eventName,
			Payload:   msg.Body,
			Metadata: map[string]string{
				"routing_key":  msg.RoutingKey,
				"amqp_type":    msg.Type,
				"dispatch_key": eventName,
			},
		}

		// ACK 콜백 설정: 핸들러 성공 시 ACK
		consumerMsg.SetAckHandler(func() error {
			return msg.Ack(false)
		})

		// NACK 콜백 설정: 실패 메시지는 기본적으로 재큐잉하지 않습니다.
		consumerMsg.SetNackHandler(func() error {
			return msg.Nack(false, r.failurePolicy == RabbitMqFailureRequeue) // 여러 메시지를 한꺼번에 처리하지 않음
		})

		return consumerMsg, nil
	}
}

func (r *Reader) Close() error {
	if r.channel != nil {
		_ = r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
