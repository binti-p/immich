# Immich Logging Configuration

## Overview

Immich Server uses structured JSON logging for the aesthetic integration, enabling centralized log aggregation and analysis. This document describes the logging configuration for Requirements 20.5 and 20.6.

## Environment Variables

### IMMICH_LOG_LEVEL

Controls the minimum log level for output. Messages below this level are filtered out.

**Options:**
- `verbose` - Most detailed, includes all messages
- `debug` - Debug information for development
- `log` - Standard informational messages (equivalent to INFO, **default**)
- `warn` - Warning messages only
- `error` - Error messages only
- `fatal` - Fatal errors only

**Default:** `log` (INFO level)

**Example:**
```yaml
env:
  - name: IMMICH_LOG_LEVEL
    value: "log"
```

### IMMICH_LOG_FORMAT

Controls the output format of log messages.

**Options:**
- `console` - Human-readable colored output (**default**)
- `json` - Structured JSON format for log aggregation

**Default:** `console`

**Example:**
```yaml
env:
  - name: IMMICH_LOG_FORMAT
    value: "json"
```

## JSON Log Format

When `IMMICH_LOG_FORMAT=json`, logs are output as structured JSON with the following fields:

### Required Fields (Requirement 20.5)

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `timestamp` | string | ISO 8601 timestamp | `"2024-01-15T10:30:00.123Z"` |
| `level` | string | Log level | `"log"`, `"warn"`, `"error"` |
| `context` | string | Service/module name | `"AestheticIntegrationService"` |
| `message` | string | Log message | `"Webhook sent successfully"` |
| `...context` | object | Additional structured data | `{"assetId": "123", "userId": "456"}` |

### Example JSON Log Output

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "log",
  "context": "AestheticIntegrationService",
  "message": "Webhook sent successfully for asset 550e8400-e29b-41d4-a716-446655440000",
  "assetId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

## Aesthetic Integration Logging

The aesthetic integration module logs the following events:

### Upload Events (Requirement 20.1)

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "log",
  "context": "WebhookService",
  "message": "Webhook sent successfully for asset 550e8400-e29b-41d4-a716-446655440000",
  "assetId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

### Feature Service Errors (Requirement 20.2)

```json
{
  "timestamp": "2024-01-15T10:30:05.456Z",
  "level": "error",
  "context": "WebhookService",
  "message": "Webhook failed for asset 550e8400-e29b-41d4-a716-446655440000: HTTP 500: Internal Server Error",
  "assetId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "error": "HTTP 500: Internal Server Error"
}
```

### Data Pipeline Database Errors (Requirement 20.4)

```json
{
  "timestamp": "2024-01-15T10:31:00.789Z",
  "level": "error",
  "context": "AestheticIntegrationService",
  "message": "Failed to retrieve scores for 100 assets: Connection timeout",
  "assetIds": ["550e8400-...", "7c9e6679-..."]
}
```

## Kubernetes Deployment Configuration

The Immich Server deployment in `k8s/app/immich-server.yaml` is configured with:

```yaml
env:
  # Logging Configuration (Requirements 20.5, 20.6)
  - name: IMMICH_LOG_LEVEL
    value: "log"  # Options: verbose, debug, log, warn, error, fatal (default: log = INFO)
  - name: IMMICH_LOG_FORMAT
    value: "json"  # Options: console, json (default: console)
```

## Log Aggregation

JSON logs can be collected and analyzed using standard log aggregation tools:

### Kubernetes Log Collection

```bash
# View logs from all Immich Server pods
kubectl logs -n aesthetic-hub -l app=immich-server --tail=100

# Stream logs in real-time
kubectl logs -n aesthetic-hub -l app=immich-server -f

# Filter logs by level using jq
kubectl logs -n aesthetic-hub -l app=immich-server | jq 'select(.level == "error")'

# Extract aesthetic integration logs
kubectl logs -n aesthetic-hub -l app=immich-server | jq 'select(.context | contains("Aesthetic"))'
```

### Prometheus Integration

The Immich Server exposes metrics at `/metrics` endpoint:

- `immich_upload_events_total{status="success|failure"}` - Upload event counter
- `immich_feature_service_request_duration_seconds` - Feature Service latency histogram
- `immich_gallery_queries_total{has_scores="true|false"}` - Gallery query counter

These metrics complement the structured logs for comprehensive observability.

## Troubleshooting

### Logs Not Appearing

1. Check log level configuration:
   ```bash
   kubectl get deployment immich-server -n aesthetic-hub -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="IMMICH_LOG_LEVEL")].value}'
   ```

2. Verify log format:
   ```bash
   kubectl get deployment immich-server -n aesthetic-hub -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="IMMICH_LOG_FORMAT")].value}'
   ```

3. Check pod logs:
   ```bash
   kubectl logs -n aesthetic-hub deployment/immich-server --tail=50
   ```

### Invalid JSON Output

If logs are not valid JSON, check:
1. `IMMICH_LOG_FORMAT` is set to `"json"`
2. No other services are writing to stdout in non-JSON format
3. NestJS ConsoleLogger is properly initialized

### Changing Log Level at Runtime

The log level can be changed by updating the deployment:

```bash
kubectl set env deployment/immich-server -n aesthetic-hub IMMICH_LOG_LEVEL=debug
```

This will trigger a rolling restart of the pods with the new log level.

## References

- **Requirements:** 20.5, 20.6 in `.kiro/specs/immich-aesthetic-integration/requirements.md`
- **Design:** Logging section in `.kiro/specs/immich-aesthetic-integration/design.md`
- **Implementation:** `immich/server/src/repositories/logging.repository.ts`
- **Deployment:** `aesthetic_hub/k8s/app/immich-server.yaml`
