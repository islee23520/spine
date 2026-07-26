# Spine v0.5.1

v0.5.1 is a corrective release for delivery reliability, lifecycle ordering, response transaction safety, and WebSocket handshake authentication. The existing v0.5.0 tag remains immutable.

## Fixed

- Required acknowledgements from all in-sync replicas for framework-managed Kafka publishes instead of using the writer's fire-and-forget zero value.
- Prevented Kafka NACK or commit failures from being skipped by a later offset commit.
- Applied consumer retry policy to initial reader creation and reader reconstruction after failed ACK/NACK, and made consumer shutdown fully drain handlers, ACK/NACK, and readers.
- Prepared HTTP responses before `BeforeResponse`, including JSON serialization, cookie validation, and status validation.
- Delayed HTTP listener startup until dependency warm-up and broker validation complete.
- Cleaned up partially initialized custom transports and rejected negative shutdown timeouts.
- Added capacity-bounded pre-upgrade WebSocket authentication, preserved immutable handshake request data in message contexts, and drained active handlers during shutdown.
- Added RabbitMQ persistent publishing, mandatory routing, publisher confirms, bounded initial/reconnect retry, and consumer QoS.
- Recovered publisher goroutine panics as dispatch errors.
- Rejected unsafe nil DI constructors/results while preserving nil collection providers, unified singleton identity across interface/concrete resolution, restored router backtracking, and removed resolver reflection panics.
- Preserved error-response write failures and safely handled invalid HTTP status values.
- Documented duplicate interceptor behavior and secure-default Kafka examples.
- Added tag-triggered CI and macOS/Windows test coverage.

## New configuration

- `boot.PublisherRetryOptions` through `RabbitMqOptions.PublisherRetry`
- `RabbitMqReadOptions.PrefetchCount` (zero defaults to 1)
- optional `core.WebSocketHandshakeInterceptor`
- `core.WebSocketHandshakeContext` and `core.WebSocketMessageContext`

## Delivery semantics

Kafka and RabbitMQ delivery is at least once. Handlers must be idempotent. RabbitMQ retry after a lost confirm can duplicate a message, and a permanently failing Kafka record can block its partition because v0.5.1 does not choose a skip/DLQ policy automatically. A physical HTTP socket write or broker publish still cannot be atomic with a database commit; use an outbox for durable external side effects.

See [the Korean migration guide](docs/migration/v0.5.1.md) or [the English migration guide](docs/migration/v0.5.1.en.md).
