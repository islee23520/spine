package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/NARUBROWN/spine/pkg/boot"
	"github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/segmentio/kafka-go"
)

type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	Writer      *kafka.Writer
	writer      kafkaMessageWriter
	topicPrefix string
}

func NewKafkaPublisher(opts *boot.KafkaOptions) (*KafkaPublisher, error) {
	if opts == nil {
		return nil, errors.New("Kafka options cannot be nil")
	}
	if opts.Write == nil {
		return nil, errors.New("Kafka write options are not configured")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	log.Println("[Kafka][Write] Event publisher initialized")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(opts.Brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	if transport := effectiveTransport(*opts); transport != nil {
		writer.Transport = transport
	}

	return &KafkaPublisher{
		Writer:      writer,
		writer:      writer,
		topicPrefix: opts.Write.TopicPrefix,
	}, nil
}

func effectiveTransport(opts boot.KafkaOptions) *kafka.Transport {
	tlsConfig := effectiveTLSConfig(opts)
	if opts.Transport != nil {
		transport := kafka.Transport{
			Dial:           opts.Transport.Dial,
			DialTimeout:    opts.Transport.DialTimeout,
			IdleTimeout:    opts.Transport.IdleTimeout,
			MetadataTTL:    opts.Transport.MetadataTTL,
			MetadataTopics: append([]string(nil), opts.Transport.MetadataTopics...),
			ClientID:       opts.Transport.ClientID,
			TLS:            opts.Transport.TLS,
			SASL:           opts.Transport.SASL,
			Resolver:       opts.Transport.Resolver,
			Context:        opts.Transport.Context,
		}
		if transport.TLS == nil && tlsConfig != nil {
			transport.TLS = tlsConfig
		}
		return &transport
	}
	if tlsConfig == nil {
		return nil
	}
	return &kafka.Transport{TLS: tlsConfig}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event publish.DomainEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("KafkaPublisher serialization failed: %w", err)
	}

	return p.client().WriteMessages(ctx, kafka.Message{
		Topic: p.topicName(event.Name()),
		Value: payload,
		Time:  event.OccurredAt(),
	})
}

func (p *KafkaPublisher) Close() error {
	client := p.client()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (p *KafkaPublisher) client() kafkaMessageWriter {
	if p.writer != nil {
		return p.writer
	}
	if p.Writer != nil {
		return p.Writer
	}
	return nil
}

func (p *KafkaPublisher) topicName(eventName string) string {
	if p.topicPrefix == "" {
		return eventName
	}
	return p.topicPrefix + eventName
}
