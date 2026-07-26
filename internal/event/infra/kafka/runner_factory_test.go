package kafka

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
	segmentio "github.com/segmentio/kafka-go"
)

func TestRunnerFactoryValidateStartupRejectsTCPServiceWithoutKafkaProtocol(t *testing.T) {
	client, server := net.Pipe()
	serverClosed := make(chan struct{})
	go func() {
		defer close(serverClosed)
		defer server.Close()
		buffer := make([]byte, 64)
		for {
			if _, err := server.Read(buffer); err != nil {
				return
			}
		}
	}()

	dialer := &segmentio.Dialer{
		Timeout: 100 * time.Millisecond,
		DialFunc: func(context.Context, string, string) (net.Conn, error) {
			return client, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := NewRunnerFactory(boot.KafkaOptions{
		Brokers:                []string{"not-kafka:9092"},
		Dialer:                 dialer,
		AllowInsecureTransport: true,
		Read:                   &boot.KafkaReadOptions{GroupID: "startup-validation"},
	}).ValidateStartup(ctx, consumer.Registration{Topic: "orders"})
	if err == nil || !strings.Contains(err.Error(), "Kafka startup handshake failed") {
		t.Fatalf("Kafka 프로토콜에 응답하지 않는 TCP 서비스는 거부해야 합니다: %v", err)
	}
	select {
	case <-serverClosed:
	case <-time.After(time.Second):
		t.Fatal("startup 검증이 실패한 Kafka 연결을 닫지 않았습니다")
	}
}
