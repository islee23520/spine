# Security configuration reference

Spine uses secure transport and bounded runtime defaults. Call `App.Validate` in deployment checks; `App.Run` performs the same network-free preflight automatically.

## Defaults

| Area | Default | Explicit development or compatibility option |
|---|---|---|
| Kafka | implicit TLS 1.2+ for each enabled reader/writer | custom `TLS`/`Dialer`/`Transport`, or local-only `AllowInsecureTransport: true` |
| RabbitMQ | `amqps://` required | `AllowInsecureTransport: true` with `amqp://` |
| RabbitMQ handler failure | reject without requeue | `FailurePolicy: boot.RabbitMqFailureRequeue` |
| Consumer transport failure | rebuild reader with exponential backoff and jitter | configure `ConsumerRetry` |
| WebSocket origin | scheme and host must match | exact `AllowedOrigins` |
| WebSocket capacity | 1024 active and pending connections | positive limit or `UnlimitedWebSocketConnections` |
| Global interceptors | HTTP and WebSocket | `InterceptorFor` with a narrower scope |
| Credentialed CORS | explicit origins required | none; wildcard credentials are invalid |
| Cookie serialization | reject invalid fields before writing a response | `EncodeCookieValue` for arbitrary values |

## Operational requirements

- Only enable insecure broker transport in an isolated local environment.
- Provision a RabbitMQ dead-letter exchange before configuring it on a Spine consumer queue.
- When enabling trusted proxy CIDRs, configure the proxy to remove or overwrite client-supplied forwarding headers.
- Alert on `WEBSOCKET_CAPACITY_EXCEEDED`, consumer reconnect exhaustion, and RabbitMQ `Type`/`RoutingKey` mismatch logs.
- Treat `boot.ConfigError.Issues` codes as stable machine-readable deployment diagnostics; do not parse the human message.

See the [v0.5 migration guide](migration/v0.5.en.md) and the [compilable example](../examples/security-config/main.go).
