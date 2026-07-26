package rabbitmq

import (
	"strings"
	"testing"

	"github.com/NARUBROWN/spine/pkg/boot"
)

func TestNewRabbitMqWriter_RequiresWriteOptions(t *testing.T) {
	_, err := NewRabbitMqWriter(boot.RabbitMqOptions{
		URL: "amqp://guest:guest@localhost:5672/",
	})
	if err == nil {
		t.Fatal("Write 옵션 누락 시 에러가 발생해야 합니다")
	}
}

func TestNewRabbitMqWriter_RequiresExchangeBeforeDial(t *testing.T) {
	_, err := NewRabbitMqWriter(boot.RabbitMqOptions{
		URL:   "amqps://unreachable.invalid:5671/",
		Write: &boot.RabbitMqWriteOptions{},
	})
	if err == nil || !strings.Contains(err.Error(), "exchange cannot be empty") {
		t.Fatalf("empty exchange should fail before dialing: %v", err)
	}
}
