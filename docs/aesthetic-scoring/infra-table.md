# Infrastructure Requirements Table

## Platform & Application Services

| Service | Namespace | CPU Request | CPU Limit | Memory Request | Memory Limit | GPU | Storage | Right-sizing rationale |
|---|---|---:|---:|---:|---:|---:|---|---|
| MinIO | platform | 250m | 500m | 256Mi | 512Mi | 0 | 5Gi PVC | Lightweight object storage for demo workload. |
| MLflow Postgres | platform | 100m | 500m | 256Mi | 512Mi | 0 | 5Gi PVC | MLflow experiment tracking DB; low QPS. |
| MLflow Server | platform | 100m | 500m | 256Mi | 512Mi | 0 | none | Stateless; artifacts on MinIO. |
| Immich Postgres | aesthetic-hub | 250m | 500m | 512Mi | 1Gi | 0 | 5Gi PVC | Small single-user demo database. |
| Immich Redis | aesthetic-hub | 100m | 250m | 128Mi | 256Mi | 0 | none | Cache/message service; low load. |
| Immich Server | aesthetic-hub | 250m | 500m | 512Mi | 1Gi | 0 | 5Gi PVC (upload) | Single replica; HPA scales to 3 on CPU >70%. |
| Immich Web | aesthetic-hub | 100m | 250m | 128Mi | 256Mi | 0 | none | Lightweight SSR frontend; HPA scales to 2. |
| Triton Server (staging) | aesthetic-hub-staging | 500m | 2000m | 1Gi | 4Gi | 0* | none | Model inference; CPU-only for demo. GPU added when available. |
| Triton Server (canary) | aesthetic-hub-canary | 500m | 2000m | 1Gi | 4Gi | 0* | none | As staging; 10% traffic weight. |
| Triton Server (production) | aesthetic-hub | 500m | 2000m | 1Gi | 4Gi | 0* | none | HPA-governed; scales with serving team's HPA. |
| Scoring Service (staging) | aesthetic-hub-staging | 200m | 500m | 256Mi | 512Mi | 0 | none | Thin REST wrapper around Triton. |
| Scoring Service (canary) | aesthetic-hub-canary | 200m | 500m | 256Mi | 512Mi | 0 | none | As staging. |
| Scoring Service (production) | aesthetic-hub | 200m | 500m | 256Mi | 512Mi | 0 | none | HPA target CPU 70%, max 3 replicas. |

\* GPU will be attached to the Triton nodes when a GPU instance is provisioned. CPU-mode Triton is used for integration testing.

## Monitoring Stack

| Service | Namespace | CPU Request | CPU Limit | Memory Request | Memory Limit | Storage | Right-sizing rationale |
|---|---|---:|---:|---:|---:|---|---|
| Prometheus | monitoring | 200m | 500m | 512Mi | 1Gi | 5Gi PVC | 15-day retention; ~30s scrape interval on ~10 targets. |
| AlertManager | monitoring | 50m | 200m | 64Mi | 256Mi | 1Gi PVC | Low resource; stateful only for silences/inhibitions. |
| Grafana | monitoring | 100m | 300m | 256Mi | 512Mi | 2Gi PVC | Dashboard renders; one dashboard pre-provisioned. |
| node-exporter | monitoring | 50m | 200m | 64Mi | 128Mi | none | DaemonSet; one pod per node; minimal overhead. |
| kube-state-metrics | monitoring | 100m | 250m | 128Mi | 256Mi | none | Single replica; read-only K8s API queries. |

**Right-sizing evidence:**
Values were derived from documented typical resource usage from upstream projects (Prometheus ~200–400 MiB RSS at 10k active series; Grafana ~100–200 MiB; node-exporter <64 MiB per node). Actual values will be validated on Chameleon using `kubectl top pods -n monitoring` and tightened for the final submission.

**GPU justification (updated):**
GPU is not provisioned at this stage. The Triton Inference Server uses ONNX models on CPU, which is adequate for demo/course traffic (~1–5 requests/second). A GPU reservation will be made for the "ongoing operation" phase when sustained throughput requires it.

---

# DevOps/Platform Container & Deployment Mapping (Phase 2)

| Role | Service / Container | Image / Dockerfile | K8S Manifest |
|---|---|---|---|
| DevOps | MinIO | `minio/minio` | `k8s/platform/minio-deployment.yaml` |
| DevOps | MinIO Service | `minio/minio` | `k8s/platform/minio-service.yaml` |
| DevOps | MLflow Postgres | `postgres:15-alpine` | `k8s/platform/mlflow.yaml` |
| DevOps | MLflow Server | `ghcr.io/mlflow/mlflow:v2.13.2` | `k8s/platform/mlflow.yaml` |
| DevOps | Prometheus | `prom/prometheus:v2.52.0` | `k8s/monitoring/prometheus-deployment.yaml` |
| DevOps | AlertManager | `prom/alertmanager:v0.27.0` | `k8s/monitoring/alertmanager-deployment.yaml` |
| DevOps | Grafana | `grafana/grafana:10.4.2` | `k8s/monitoring/grafana-deployment.yaml` |
| DevOps | node-exporter | `prom/node-exporter:v1.8.1` | `k8s/monitoring/node-exporter-daemonset.yaml` |
| DevOps | kube-state-metrics | `registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.12.0` | `k8s/monitoring/kube-state-metrics.yaml` |
| DevOps | HPA (Immich Server) | n/a | `k8s/autoscaling/hpa-immich-server.yaml` |
| DevOps | HPA (Immich Web) | n/a | `k8s/autoscaling/hpa-immich-web.yaml` |
| DevOps | Namespace (App) | n/a | `k8s/app/namespace.yaml` |
| DevOps | Namespace (Platform) | n/a | `k8s/platform/namespace.yaml` |
| DevOps | Namespace (Monitoring) | n/a | `k8s/monitoring/namespace.yaml` |
| DevOps | Argo WorkflowTemplate (train) | Argo Workflows | `k8s/workflows/train-and-evaluate.yaml` |
| DevOps | Argo WorkflowTemplate (promote) | Argo Workflows | `k8s/workflows/promote-model.yaml` |
| DevOps | Argo CronWorkflow | Argo Workflows | `k8s/workflows/cron-train.yaml` |
| DevOps | Argo EventSource + Sensor | Argo Events | `k8s/workflows/event-trigger.yaml` |
| Serving | Immich Server | `ghcr.io/immich-app/immich-server:release` | `k8s/immich-server.yaml` |
| Serving | Immich Web | `ghcr.io/immich-app/immich-web:release` | `k8s/app/immich-web.yaml` |
| Serving | Triton Server (staging) | `nvcr.io/nvidia/tritonserver:24.10-py3` | `k8s/staging/templates/deployment.yaml` |
| Serving | Triton Server (canary) | `nvcr.io/nvidia/tritonserver:24.10-py3` | `k8s/canary/templates/deployment.yaml` |
| Serving | Triton Server (production) | `nvcr.io/nvidia/tritonserver:24.10-py3` | `k8s/production/templates/deployment.yaml` |
| Serving | Scoring Service (staging) | `localhost:5000/aesthetic-scoring:latest` | `k8s/staging/templates/deployment.yaml` |
| Serving | Scoring Service (canary) | `localhost:5000/aesthetic-scoring:latest` | `k8s/canary/templates/deployment.yaml` |
| Serving | Scoring Service (production) | `localhost:5000/aesthetic-scoring:stable` | `k8s/production/templates/deployment.yaml` |
| Data | Immich Postgres | `tensorchord/pgvecto-rs:pg14-v0.2.0` | `k8s/app/postgres.yaml` |
| Data | Immich Redis | `redis:6.2-alpine` | `k8s/app/redis.yaml` |
| Data | Data API | [binti-p/aesthetic-hub-data](https://github.com/binti-p/aesthetic-hub-data) | (Data team repo) |
| Data | Feature Service | [binti-p/aesthetic-hub-data](https://github.com/binti-p/aesthetic-hub-data) | (Data team repo) |
| Data | Batch Pipeline | [binti-p/aesthetic-hub-data](https://github.com/binti-p/aesthetic-hub-data) | (Data team repo) |
| Training | Training Container | Team Dockerfile (PyTorch + CUDA + MLflow + CLIP) | (Training team repo) |
