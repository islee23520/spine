package consumer

import (
	"fmt"
	"sync"

	"github.com/NARUBROWN/spine/core"
	"github.com/NARUBROWN/spine/internal/router"
)

type Registration struct {
	Topic string
	Meta  core.HandlerMeta
}

type Registry struct {
	mu            sync.RWMutex
	registrations []Registration
	topics        map[string]struct{}
}

func NewRegistry() *Registry {
	return &Registry{
		registrations: make([]Registration, 0),
		topics:        make(map[string]struct{}),
	}
}

func (r *Registry) Register(topic string, target any) error {
	if topic == "" {
		return fmt.Errorf("consumer: topic cannot be empty")
	}
	if target == nil {
		return fmt.Errorf("consumer: target cannot be nil")
	}

	meta, err := router.NewHandlerMeta(target)

	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topics == nil {
		r.topics = make(map[string]struct{})
	}
	if _, exists := r.topics[topic]; exists {
		return fmt.Errorf("consumer: topic already registered: %s", topic)
	}
	r.registrations = append(r.registrations, Registration{
		Topic: topic,
		Meta:  meta,
	})
	r.topics[topic] = struct{}{}
	return nil
}

func (r *Registry) Registrations() []Registration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cpy := make([]Registration, len(r.registrations))
	copy(cpy, r.registrations)
	return cpy
}
