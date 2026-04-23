# Comhub

Comhub is a Kafka-native intermediate operational processing layer for existing event streams. It lets operators map heterogeneous Kafka events into a canonical operational model, classify them, apply configurable operational policy, route meaningful events into downstream actions, and observe those outcomes through purpose-built operational visibility.

The project is intentionally explicit and small-surface-area. Cross-service communication is planned through Kafka topics, not service-to-service business HTTP calls. Shared contracts live in the `common` module so every service uses the same canonical event model and low-risk logging conventions. Comhub does not try to own upstream producer reliability. Its reliability workflows apply within Comhub's own processing boundary after events have entered the Comhub pipeline.

## Modules

| Module | Purpose |
| --- | --- |
| `common` | Shared canonical event model, Kafka header constants, Jackson Kafka JSON serializer/deserializer, and MDC logging helper. |
| `control-plane` | Browser-facing control plane and API for source configuration, DLQ inspection, replay, and dashboard handoff. |
| `service-mapper` | Processing service that interprets source events and maps them into the canonical operational event model. |
| `service-router` | Processing service that classifies canonical events and applies operational-policy routing such as notify, suppress, or visibility-only handling. |
| `service-delivery` | Processing service that executes downstream notification delivery for events that require it. |
| `loadgen` | Planned load generator for steady, spike, malformed, and failure demonstration scenarios. |

## Technology Stack

- Java 21
- Spring Boot 4.0.5
- Spring for Apache Kafka 4.0.4
- Apache Kafka 4.x
- Maven multi-module build
- JUnit 5
- Jackson JSON serialization

Planned platform components include Strimzi-managed Kafka, KEDA autoscaling, Prometheus metrics, Grafana dashboards, and Mailpit-backed local email delivery. Local development runs these pieces on a `kind` Kubernetes cluster where possible, while the architecture keeps them aligned with production Kubernetes deployment patterns. Delivery retry, DLQ, and replay remain important, but they are scoped to Comhub-managed processing outcomes rather than upstream source-system ownership.

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
- Treat Comhub as an operational interpretation and policy layer over existing streams.
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
