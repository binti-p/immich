# Aesthetic Hub — System Architecture (Phase 2)

## Overview

Aesthetic Hub is deployed on a Kubernetes cluster provisioned on Chameleon Cloud. The system integrates an ML-based photo aesthetic scoring feature into Immich, a self-hosted photo management platform.

The system consists of:
- A **3-node Kubernetes cluster** (2 control-plane + 1 worker) on KVM@TACC
- **Shared platform services** (MinIO, MLflow) for artifact and experiment storage
- An **open-source application** (Immich) with the aesthetic scoring ML feature integrated
- A **monitoring stack** (Prometheus, AlertManager, Grafana) for infrastructure observability
- **CI/CD and ML automation** (ArgoCD, Argo Workflows, Argo Events) for model lifecycle management
- **Three deployment environments** (staging, canary, production) for safe model promotion

---

## 1. Infrastructure Layer

Provisioned with **Terraform** on Chameleon Cloud (KVM@TACC):
- 3 Ubuntu 24.04 KVM instances
- Private network `192.168.1.0/24` + shared public network
- Single floating IP assigned to node1 (jump host)
- Security groups expose ports: 22, 80, 8000–8082, 9001, 9090

Configured with **Ansible + Kubespray**:
- Kubernetes 3-node cluster (Calico CNI, local-path storage provisioner)
- Post-install: CoreDNS patched, Argo Workflows + Argo Events installed, ArgoCD installed

---

## 2. Namespace Structure

| Namespace | Purpose |
|---|---|
| `platform` | Shared services: MinIO (object storage), MLflow (experiment tracking) |
| `aesthetic-hub` | Production: Immich app + production aesthetic scoring service |
| `aesthetic-hub-staging` | Staging: new model versions deployed here first |
| `aesthetic-hub-canary` | Canary: validated staging models receive 10% of traffic |
| `monitoring` | Prometheus, AlertManager, Grafana, node-exporter, kube-state-metrics |
| `argo` | Argo Workflows (ML training + promotion pipelines) |
| `argo-events` | Argo Events (webhook trigger for on-demand training) |
| `argocd` | ArgoCD (GitOps continuous delivery) |
| `kube-system` | Kubernetes system components |

---

## 3. Platform Layer (`platform` namespace)

### MinIO (Object Storage)
- Stores: training datasets, model artifacts, Triton model repository
- Buckets: `aesthetic-hub-data`, `mlflow-artifacts`, `triton-models`
- Persistent: 5Gi PVC backed by local-path provisioner
- Exposed via NodePort for CLI access

### MLflow (Experiment Tracking)
- Backend store: dedicated PostgreSQL instance (`mlflow-postgres`) with 5Gi PVC
- Artifact store: MinIO bucket `mlflow-artifacts`
- Used by: training team for logging runs; DevOps for model registration/promotion
- Exposed via NodePort (port 30500) for browser access
- Scraped by Prometheus for availability monitoring

---

## 4. Application Layer (`aesthetic-hub` namespace)

### Immich (Photo Management)
- **Immich Web** (frontend): Node.js SSR, NodePort for browser access; HPA min=1 max=2
- **Immich Server** (backend API): connects to Postgres + Redis; HPA min=1 max=3, CPU target 70%
- **Postgres**: stores Immich metadata (photo records, albums, users); 5Gi PVC
- **Redis**: caching + background job queue

### Aesthetic Scoring (Production)
- **Scoring Service**: thin REST API that accepts an image, calls Triton, returns `{score, confidence, explanations}`
- **Triton Inference Server**: serves the CLIP-based aesthetic regression model; polls MinIO `triton-models/production` for model updates every 60s
- HPA on Scoring Service: min=1 max=3, CPU target 70%
- Integrated into Immich: scoring is triggered on photo upload via Immich's webhook/plugin mechanism

---

## 5. ML Pipeline (Argo Workflows)

### train-and-evaluate (WorkflowTemplate)
```
train → evaluate → [gate check] → register-to-staging
```
- `train`: runs the training container on GPU/CPU, logs run to MLflow
- `evaluate`: checks Spearman-r and NDCG@10 against thresholds; fails if below gates
- `register-to-staging`: promotes run to `Staging` model stage in MLflow registry

### promote-model (WorkflowTemplate)
```
transition-model-stage → argocd-sync
```
- Moves a model between `Staging → Canary → Production` (or rolls back)
- After stage transition, syncs the relevant ArgoCD app so Triton picks up the new model
- Rollback path: archives bad production model, restores previous version

### Automated triggers
- **CronWorkflow**: runs `train-and-evaluate` every Sunday at 02:00 UTC
- **Argo EventSource + Sensor**: POST to `<node>:31234/aesthetic-hub/train` triggers training on demand (used by Data team's batch pipeline when new feedback data is ready)

---

## 6. Environment Promotion Flow

```
New training data arrives
        ↓
[cron or webhook] train-and-evaluate
        ↓
  Quality gates pass?
  No → workflow fails (production unchanged)
  Yes → model registered to MLflow "Staging"
        ↓
  ArgoCD syncs aesthetic-hub-staging
  Triton staging picks up new model
        ↓
  Automated smoke test (serving team)
  Pass → promote Staging → Canary (10% traffic)
        ↓
  Canary observationWindow (30 min)
  Metrics pass? (error rate < 2%, accuracy > 80%)
  No → rollback workflow triggered by AlertManager
  Yes → promote Canary → Production
        ↓
  ArgoCD syncs aesthetic-hub (production)
  Triton production updates via model poll
```

All promotion steps are executed by Argo Workflows (no manual SSH required). ArgoCD enforces GitOps: any manual changes to K8s objects are reverted within 3 minutes.

---

## 7. Monitoring Stack (`monitoring` namespace)

### Data collection
- **node-exporter** (DaemonSet): host-level CPU, memory, disk, network per node
- **kube-state-metrics**: K8s object state (pod readiness, deployment replicas, HPA status)
- **Prometheus** (scrape interval 30s): aggregates metrics from all the above + cAdvisor + MLflow + Triton + Argo Workflows

### Alerting
- **AlertManager**: routes alerts by severity and type
  - `critical` alerts: immediate notification (1h repeat)
  - ML pipeline alerts: separate routing (4h repeat)
  - Inhibition rules: suppress pod-level alerts when a node is down
- Key alert rules (see `k8s/monitoring/prometheus-configmap.yaml`):
  - `NodeHighCPU` / `NodeCriticalCPU` (85% / 95% thresholds)
  - `NodeHighMemory` (85%)
  - `NodeDiskSpaceLow` / `NodeDiskSpaceCritical` (80% / 90%)
  - `NodeNotReady` (1 min)
  - `PodCrashLooping` (>3 restarts in 10 min)
  - `MLflowServiceDown` / `TritonServiceDown` (2 min)
  - `ArgoWorkflowFailed`
  - `DeploymentReplicasMismatch` (10 min)
  - Automated rollback: `TritonServiceDown` → AlertManager webhook → Argo promote-model (rollback=true)

### Visualization
- **Grafana** (pre-provisioned datasource + dashboard):
  - Cluster overview: node CPU/memory/disk, pod counts, restart rates
  - HPA replica tracking
  - Active alert panel

---

## 8. Autoscaling

| Resource | Min | Max | Trigger |
|---|---|---|---|
| Immich Server | 1 | 3 | CPU > 70% or Memory > 80% |
| Immich Web | 1 | 2 | CPU > 70% |
| Scoring Service (production) | 1 | 3 | CPU > 70% |

Scale-up: stabilization 60s, 1 pod/60s. Scale-down: stabilization 300s, 1 pod/120s. These conservative behaviors prevent thrashing under bursty demo traffic.

---

## 9. Secrets Management

No secrets are committed to Git. All credentials are injected at cluster setup time by Ansible:
- `minio-credentials` (platform namespace): generated once, re-used by MLflow + Triton + Argo Workflows
- `mlflow-postgres-credentials` (platform namespace): generated by deploy_monitoring.yml
- `grafana-secret` (monitoring namespace): generated by deploy_monitoring.yml
- `argocd-initial-admin-secret` (argocd namespace): generated by ArgoCD itself

The file `k8s/monitoring/grafana-secret.yaml` in Git is a **documentation placeholder** only.

---

## 10. Storage Summary

| PVC | Namespace | Size | Contents |
|---|---|---|---|
| `minio-pvc` | platform | 5Gi | ML datasets, model artifacts, Triton model repo |
| `mlflow-postgres-pvc` | platform | 5Gi | MLflow experiment + run metadata |
| `prometheus-pvc` | monitoring | 5Gi | 15 days of metrics time-series |
| `alertmanager-pvc` | monitoring | 1Gi | Silence + inhibition state |
| `grafana-pvc` | monitoring | 2Gi | Grafana state + custom dashboards |
| `immich-postgres-pvc` | aesthetic-hub | 5Gi | Immich photo metadata |
| `immich-upload-pvc` | aesthetic-hub | 5Gi | Uploaded photos + thumbnails |

All PVCs use the `local-path` StorageClass (Rancher Local Path Provisioner), writing to `/opt/local-path-provisioner/` on the worker node. Data persists across pod restarts.
