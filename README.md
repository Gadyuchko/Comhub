# Comhub

Comhub watches your Kafka event streams and turns them into alerts, metrics, and audit trails. You point it at a topic, paste a sample of the JSON your service publishes, and tell it — through a web UI, no code — which fields matter, how to classify each event, and who should be notified when something interesting happens. Comhub sits between your existing services and your operations team: it reads events from Kafka, processes them based on rules you configure at runtime, sends email alerts when those rules match, and surfaces everything on dashboards so you can see what's going on.

## What you can do with it

- **Onboard a new event source in minutes.** Open the Config page, drop in a sample of the JSON your service publishes, click through to map fields like severity, subject, and message, and save. Comhub starts processing live events from that topic immediately — no rebuild, no redeploy.
- **Define classification and routing rules.** Say "if severity is high and category is payment, email the on-call address" — without writing code or editing YAML. Rules are configuration; they take effect at runtime.
- **Get email alerts.** Rule matches send mail through standard SMTP. Local development uses Mailpit so you can see what would have gone out without sending real mail.
- **Watch the pipeline.** Grafana dashboards show event throughput, classification rates, delivery outcomes, retry behavior, and dead-letter volume.
- **Recover from failures.** Failed events move through retry tiers and then land in a dead-letter queue you can browse and replay one event at a time. Nothing gets silently dropped.

## How you use it

1. **Bring up a local cluster.** Follow [`runbooks/local-setup.md`](runbooks/local-setup.md) — one script bootstraps a `kind` Kubernetes cluster with a 3-broker Kafka and every topic Comhub needs.
2. **Build and deploy Comhub.** `./scripts/build-images.sh` builds the service images and loads them into the cluster; `kubectl apply` brings the services up.
3. **Open the UI.** Port-forward the control-plane (`kubectl port-forward -n comhub svc/control-plane 8080:8080`) and visit `http://localhost:8080/config`.
4. **Configure your first source.** Click `New Source`, fill in the topic name, paste a sample event, map the fields, add classification and routing rules, click `Save and Enable`.
5. **Send events.** Anything published to that topic flows through Comhub, gets classified, routes to email if your rules match, and shows up on the dashboards.

The end-to-end walkthrough lives in [`runbooks/onboarding-demo.md`](runbooks/onboarding-demo.md).

## Where this is going

Comhub is planned to evolve into a production-grade pluggable layer for event-driven systems. The current architecture is intentionally small and explicit so the surfaces that matter — delivery channels, classification rules, source codecs, and policy isolation — can extend cleanly without redesigning the core. Future directions include additional notification channels beyond email (Slack, webhooks, PagerDuty), Avro and Protobuf alongside JSON, multi-tenant deployments, and richer rule engines, all behind the same Kafka-native pipeline.

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
