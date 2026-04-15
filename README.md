# Comhub

Comhub is a Kafka-native event operations platform that demonstrates production-grade event-driven architecture patterns. It focuses on the mechanics that matter in real systems: canonical event envelopes, Kafka headers, topic-based processing, retries, DLQ handling, replay, observability, Kubernetes deployment, and operational dashboards.

The project is intentionally explicit and small-surface-area. Cross-service communication is planned through Kafka topics, not service-to-service business HTTP calls. Shared contracts live in the `common` module so every service uses the same canonical event model and low-risk logging conventions.

## Modules

| Module | Purpose |
| --- | --- |
| `common` | Shared canonical event model, Kafka header constants, Jackson Kafka JSON serializer/deserializer, and MDC logging helper. |
| `control-plane` | Planned browser-facing control plane and API for configuration, DLQ, replay, and dashboard handoff. |
| `service-mapper` | Planned processing service that maps source events into the canonical event model. |
| `service-router` | Planned processing service that classifies canonical events and routes them by policy. |
| `service-delivery` | Planned processing service that delivers routed events to configured destinations. |
| `loadgen` | Planned load generator for steady, spike, malformed, and failure demonstration scenarios. |

## Technology Stack

- Java 21
- Spring Boot 4.0.5
- Spring for Apache Kafka 4.0.4
- Apache Kafka 4.x
- Maven multi-module build
- JUnit 5
- Jackson JSON serialization

Planned platform components include Strimzi-managed Kafka, KEDA autoscaling, Prometheus metrics, Grafana dashboards, and Mailpit-backed local email delivery. Local development runs these pieces on a `kind` Kubernetes cluster where possible, while the architecture keeps them aligned with production Kubernetes deployment patterns.

## Build And Test

Run all tests:

```powershell
mvn test
```

Build all modules without running tests:

```powershell
mvn -q -DskipTests package
```

After packaging, the shared module jar is produced under:

```text
common/target/common-0.0.1-SNAPSHOT.jar
```

## Design Principles

- Keep Kafka as the integration backbone.
- Keep shared contracts in `common`.
- Prefer explicit, explainable code over hidden framework magic.
- Do not log raw payloads, secrets, or unbounded labels.
- Keep metric and log fields low-cardinality.
- Make failures visible; do not silently drop events.
- Keep processing services narrow and focused on one pipeline concern.

## Repository Layout

```text
.
|-- common/
|-- control-plane/
|-- service-mapper/
|-- service-router/
|-- service-delivery/
|-- loadgen/
|-- docs/
`-- pom.xml
```

## License

No license has been selected yet.
