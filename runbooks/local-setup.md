# Local Setup

How to bootstrap the Comhub local development environment from scratch.

## Host Requirements

- **RAM:** 32 GB minimum (kind cluster + 3 Kafka brokers + Strimzi operator is memory-heavy)
- **CPU:** 8 cores minimum
- **Disk:** ~10 GB free for Docker images and container volumes

## Prerequisites

Install these before running the bootstrap script:

- **Docker (Docker Desktop for Win)** -- must be running before anything else. kind uses Docker to create Kubernetes nodes as containers.
  Download: https://www.docker.com/products/docker-desktop/

- **kind v0.31.0** -- creates the local Kubernetes cluster.
  Download: https://kind.sigs.k8s.io/docs/user/quick-start/#installation

- **kubectl** -- the Kubernetes CLI. Used to apply manifests and verify cluster state.
  Download: https://kubernetes.io/docs/tasks/tools/

- **curl** -- downloads the pinned Strimzi operator manifest during bootstrap.

- **sed** -- rewrites the Strimzi install manifest namespace to `kafka` during bootstrap.

- **Java 21** -- required for building and running Comhub services (later stories).
  Download: https://adoptium.net/

- **Maven** -- builds the Java project.
  Download: https://maven.apache.org/install.html

- **Git Bash (Windows only)** -- the bootstrap script is a bash script. On Windows, run it from Git Bash. Comes with Git for Windows.

## Bootstrap

1. Make sure Docker is running.

2. Open a terminal (Git Bash on Windows) and navigate to the project root:

   ```bash
   cd /c/Users/YourName/IdeaProjects/Comhub
   ```

3. Run the bootstrap script:

   ```bash
   ./scripts/bootstrap-cluster.sh
   ```

   The script will:
   - Check that `docker`, `kind`, `kubectl`, `curl`, and `sed` are installed
   - Create a kind cluster named `comhub` (1 control-plane + 3 worker nodes), or reuse it if it already exists
   - Create the `kafka` namespace
   - Install the Strimzi operator (v0.51.0) and wait for it to be ready
   - Apply the Kafka cluster CR (3 KRaft brokers) and wait for it to be ready
   - Apply all 9 topic CRs and wait for them to be ready

   This takes several minutes on first run. Subsequent runs are faster because the script reuses the existing cluster.

## Verification

After the script completes, verify each layer:

### Cluster nodes

```bash
kubectl get nodes
```

Expected: 1 control-plane node and 3 worker nodes, all `Ready`.

### Strimzi operator

```bash
kubectl get deployment/strimzi-cluster-operator -n kafka
```

Expected: `AVAILABLE` = 1.

### Kafka cluster

```bash
kubectl get kafka -n kafka
```

Expected: `comhub` cluster with `Ready` = `True`.

### Topics

```bash
kubectl get kafkatopic -n kafka
```

Expected: all 9 topics listed with `Ready` = `True`.

## Smoke Test

Verify that Kafka can produce and consume messages end-to-end.

### Start a temporary pod with Kafka tools

```bash
kubectl run kafka-smoke-test -n kafka --rm -it \
  --image=quay.io/strimzi/kafka:0.51.0-kafka-4.2.0 \
  --restart=Never -- bash
```

This drops you into a shell inside a pod that has the Kafka CLI tools.

### Produce a test message

From inside the pod:

```bash
echo '{"source":"smoke-test","payload":"hello"}' | \
  bin/kafka-console-producer.sh \
    --bootstrap-server comhub-kafka-bootstrap:9092 \
    --topic comhub.canonical.v1
```

### Consume and verify

From inside the pod:

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server comhub-kafka-bootstrap:9092 \
  --topic comhub.canonical.v1 \
  --from-beginning \
  --max-messages 1
```

Expected: the JSON message you just produced is printed to the console.

Type `exit` to leave the pod. It cleans itself up automatically.

## What This Does NOT Include

The following are handled by later stories and are not part of this bootstrap:

- Monitoring stack (Prometheus + Grafana)
- KEDA autoscaling
- Mailpit email delivery
- Comhub application deployments (control-plane, mapper, router, delivery)
- Source configuration and event processing
