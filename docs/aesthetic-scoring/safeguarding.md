# Aesthetic Hub — Safeguarding Plan

## 1. Overview

Aesthetic Hub integrates an ML-based aesthetic scoring feature into Immich, a self-hosted photo management system. The model ranks uploaded photos by aesthetic quality using a CLIP-based regression model trained on the AVA dataset. This plan describes the active mechanisms used to support each safeguarding principle across the system.

---

## 2. Fairness

### Risk
Aesthetic perception is culturally and contextually relative. A model trained on AVA (crowdsourced ratings from DPChallenge.com, predominantly English-speaking, hobbyist photography community) may systematically score certain types of photos — including photos from non-Western traditions, subjects, or styles — lower than others. This could cause minority group photos to be buried in sorted album views.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **Stratified evaluation by photo category** | Offline evaluation script (`evaluate.py`) tags AVA photos by category and reports per-category Spearman-r. A model fails the quality gate if the worst-performing category drops more than 0.15 below average. | Training |
| **Score distribution monitoring** | Grafana dashboard plots the live distribution of aesthetic scores emitted per day (histogram). A Prometheus alert fires if the p10 score drops by more than 0.15 vs. the 7-day baseline, indicating the model may be systematically downgrading a new photo type. | Serving / DevOps |
| **Score transparency in Immich UI** | The aesthetic score is shown alongside each photo in the UI (as a numerical badge). Users can explicitly exclude scores from sorting. This prevents silent discrimination. | Serving (UI integration) |
| **Opt-out** | Users can disable aesthetic sorting globally or per album via Immich settings. No photos are hidden or deleted based on score alone. | Serving |

---

## 3. Explainability

### Risk
Users who see their photo ranked low with no explanation may lose trust in the system.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **CLIP text-similarity explanation** | The scoring service returns the top-3 aesthetic descriptors most aligned with the image (e.g., "shallow depth of field", "golden hour lighting") using cosine similarity between the image embedding and a fixed set of aesthetic concept embeddings. These are shown in the Immich photo detail panel. | Serving |
| **Confidence intervals** | The model outputs a mean score and an uncertainty estimate (dropout inference). When uncertainty is high (σ > 0.15), the UI shows a "low confidence" badge instead of a precise score. | Serving |
| **MLflow model card** | Each registered model version includes a model card with: training data summary, per-category evaluation metrics, known limitations, and intended use. | Training |

---

## 4. Transparency

### Risk
Users and administrators may not know that ML is being used to rank their photos, or that their ratings are used for retraining.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **In-app disclosure** | The Immich settings page includes a section "Aesthetic Scoring (ML Feature)" explaining: what the model does, what data it was trained on, and that user feedback may be used for retraining. | Serving (UI) |
| **Feedback consent** | Explicit thumb-up/thumb-down buttons are the only feedback mechanism. No implicit behavioral data (e.g., view time) is used for retraining without disclosure. | Data |
| **Audit log** | Each model promotion (Staging → Canary → Production) is logged in MLflow with: who triggered it (Argo Workflow run ID), what metrics passed the gate, and the git SHA of the training code used. | DevOps (Argo Workflows) |
| **Public MLflow UI** | The MLflow UI is accessible to all cluster admins, showing all training runs, metrics, and registered model versions. | DevOps |

---

## 5. Privacy

### Risk
Photo metadata (location, timestamp, subjects) could be logged during inference and used in ways users did not consent to.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **Inference-time data minimization** | The scoring service sends only the raw image bytes to Triton. No EXIF data, filename, or album membership is forwarded to the model. | Serving |
| **No user-identifiable data in training** | The feedback pipeline (Data team) logs only `(image_hash, score, feedback)` tuples. No user IDs, IP addresses, or timestamps are stored in the training dataset. | Data |
| **Training data on private MinIO** | Training datasets in MinIO are not publicly accessible; they require credentials. The bucket `aesthetic-hub-data` has `private` ACL. | DevOps |
| **Secrets never in Git** | All credentials (MinIO, PostgreSQL, Grafana) are injected at deploy time via Ansible-generated Kubernetes secrets. The `grafana-secret.yaml` in the repo is a documentation placeholder only. | DevOps |

---

## 6. Accountability

### Risk
If the model produces harmful outputs (e.g., systematically low scores for a class of photos), it should be traceable and reversible.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **Automated rollback** | The `promote-model` Argo Workflow has a `rollback=true` parameter. If Prometheus triggers a `ModelScoreDriftHigh` or `TritonErrorRateHigh` alert, the AlertManager webhook calls the Argo Workflow API to submit a rollback workflow, reverting to the previously archived production version. | DevOps + Serving |
| **Model versioning** | All model versions are retained in MLflow (never deleted). "Archived" stage is used for superseded versions, preserving the full audit trail. | Training |
| **Promotion gate enforcement** | No model can reach production without passing offline quality gates and an automated canary validation period. There is no path to skip this (the Argo Workflow enforces it). | DevOps |
| **Git as source of truth** | All Argo Workflow templates, K8s manifests, and IaC code live in this repository. No production changes can be made without a git commit. ArgoCD self-heals any manual `kubectl` changes within 3 minutes. | DevOps |

---

## 7. Robustness

### Risk
Malformed inputs, adversarial images, or high traffic could cause the inference pipeline to fail in ways that degrade Immich for all users.

### Mechanisms

| Mechanism | Where implemented | Who owns it |
|---|---|---|
| **Input validation** | The scoring service validates: image format (JPEG/PNG/WEBP only), image dimensions (min 64×64, max 8192×8192), and file size (max 50 MB) before forwarding to Triton. Invalid inputs return HTTP 400 with no model call. | Serving |
| **Rate limiting** | The Immich nginx reverse proxy limits scoring requests to 10/s per user IP using `limit_req_zone`. This prevents a single client from saturating Triton. | Serving (nginx config) |
| **Graceful degradation** | If Triton is unreachable or returns an error, the scoring service returns a default score of `null` and Immich displays photos in upload-time order. The feature degrades silently rather than blocking the core photo management workflow. | Serving |
| **HPA autoscaling** | The scoring service and Immich server are both covered by HorizontalPodAutoscalers (CPU target: 70%). See `k8s/autoscaling/`. | DevOps |
| **Triton model health check** | Triton exposes a `/v2/health/ready` endpoint. The scoring service checks it on startup and every 60 seconds; if unhealthy, inference calls are short-circuited. | Serving |
| **Data quality gates at ingestion** | The Data team's pipeline validates incoming feedback data for out-of-range scores (must be 1–10) and duplicate `image_hash` values before writing to the training store. | Data |
| **Prometheus alerting** | Infrastructure degradation alerts (CPU, memory, disk, pod restarts, node not ready) are configured in `k8s/monitoring/prometheus-configmap.yaml`. AlertManager routes critical alerts immediately and suppresses cascading alerts when a node is down. | DevOps |

---

## 8. Summary Table

| Principle | Active mechanisms | Phase implemented |
|---|---|---|
| Fairness | Stratified evaluation gates; score distribution monitoring; opt-out | Phase 2 |
| Explainability | CLIP concept similarity; confidence badges; model card | Phase 2 |
| Transparency | In-app disclosure; feedback consent; promotion audit log | Phase 2 |
| Privacy | Data minimization at inference; no user IDs in training; private buckets; secrets hygiene | Phases 1–2 |
| Accountability | Automated rollback; model versioning; promotion gates; Git as source of truth | Phase 2 |
| Robustness | Input validation; rate limiting; graceful degradation; HPA; alerting | Phase 2 |
