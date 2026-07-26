package kafka

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/pkg/boot"
	segmentio "github.com/segmentio/kafka-go"
)

func TestEffectiveDialerUsesSharedTLSWithoutMutatingOverride(t *testing.T) {
	shared := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "broker.example"}
	override := &segmentio.Dialer{}
	dialer := effectiveDialer(boot.KafkaOptions{TLS: shared, Dialer: override})
	if dialer == override || dialer.TLS == shared || dialer.TLS.ServerName != "broker.example" {
		t.Fatalf("shared TLS should be cloned into a cloned override: %#v", dialer)
	}
	if override.TLS != nil {
		t.Fatal("advanced override must not be mutated")
	}
}

func TestEffectiveDialerSharedTLSPreservesKafkaDefaultsWithoutMutation(t *testing.T) {
	defaultTimeout := segmentio.DefaultDialer.Timeout
	defaultDualStack := segmentio.DefaultDialer.DualStack
	defaultTLS := segmentio.DefaultDialer.TLS
	shared := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "broker.example"}

	dialer := effectiveDialer(boot.KafkaOptions{TLS: shared})

	if dialer == segmentio.DefaultDialer {
		t.Fatal("effective dialer must clone kafka.DefaultDialer")
	}
	if dialer.Timeout != 10*time.Second {
		t.Fatalf("default timeout must remain 10s, got %s", dialer.Timeout)
	}
	if !dialer.DualStack {
		t.Fatal("default DualStack must remain enabled")
	}
	if dialer.TLS == shared || dialer.TLS.ServerName != "broker.example" {
		t.Fatal("shared TLS must be cloned into the effective dialer")
	}
	if segmentio.DefaultDialer.Timeout != defaultTimeout || segmentio.DefaultDialer.DualStack != defaultDualStack || segmentio.DefaultDialer.TLS != defaultTLS {
		t.Fatal("effectiveDialer must not mutate kafka.DefaultDialer")
	}
}

func TestEffectiveDialerUsesImplicitTLS12Default(t *testing.T) {
	dialer := effectiveDialer(boot.KafkaOptions{})
	if dialer == nil || dialer.TLS == nil {
		t.Fatal("secure Kafka dialer must be created implicitly")
	}
	if dialer.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %d, want TLS 1.2", dialer.TLS.MinVersion)
	}
	if dialer.Timeout != 10*time.Second || !dialer.DualStack {
		t.Fatalf("Kafka defaults must remain intact: %+v", dialer)
	}
}

func TestEffectiveDialerPreservesAdvancedTLSOverride(t *testing.T) {
	advancedTLS := &tls.Config{ServerName: "advanced.example"}
	dialer := effectiveDialer(boot.KafkaOptions{
		TLS:    &tls.Config{ServerName: "shared.example"},
		Dialer: &segmentio.Dialer{TLS: advancedTLS},
	})
	if dialer.TLS != advancedTLS {
		t.Fatal("advanced TLS override should take precedence")
	}
}
