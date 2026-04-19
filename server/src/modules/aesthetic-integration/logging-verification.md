# JSON Logging Verification

## Overview

This document verifies that Immich's `LoggingRepository` with `IMMICH_LOG_FORMAT=json` produces structured JSON logs that meet Requirements 20.5 and 20.6.

## Required Fields (Requirement 20.5)

The requirements specify that logs must include:
1. **timestamp** - ISO 8601 timestamp
2. **level** - Log level (verbose, debug, log, warn, error, fatal)
3. **service** - Service/module name (context)
4. **message** - Log message
5. **context** - Additional structured data

## NestJS ConsoleLogger JSON Format

Immich uses NestJS's built-in `ConsoleLogger` with `json: true` option. When JSON mode is enabled, NestJS ConsoleLogger outputs logs in the following format:

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

### Field Mapping

| Requirement | NestJS Field | Implementation |
|-------------|--------------|----------------|
| timestamp | `timestamp` | ✅ Automatically added by NestJS ConsoleLogger |
| level | `level` | ✅ Automatically added (verbose, debug, log, warn, error, fatal) |
| service | `context` | ✅ Set via `logger.setContext(ServiceName)` |
| message | `message` | ✅ First parameter to log methods |
| context | `...spread` | ✅ Additional parameters spread into JSON |

## Implementation in Aesthetic Integration

### AestheticIntegrationService

```typescript
export class AestheticIntegrationService {
  private readonly logger = new LoggingRepository(undefined, undefined);

  constructor(...) {
    this.logger.setContext(AestheticIntegrationService.name); // Sets "service" field
  }

  async getScoresForAssets(assetIds: string[]): Promise<Map<string, AestheticScoreDto>> {
    try {
      // ... implementation
    } catch (error) {
      // Structured logging with context
      this.logger.error(
        `Failed to retrieve scores for ${assetIds.length} assets: ${error.message}`,
        { assetIds } // Additional context data
      );
      return new Map();
    }
  }
}
```

**Output:**
```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "error",
  "context": "AestheticIntegrationService",
  "message": "Failed to retrieve scores for 100 assets: Connection timeout",
  "assetIds": ["550e8400-...", "7c9e6679-..."]
}
```

### WebhookService

```typescript
export class WebhookService {
  private readonly logger = new LoggingRepository(undefined, undefined);

  constructor(...) {
    this.logger.setContext(WebhookService.name); // Sets "service" field
  }

  async sendAsync(payload: UploadWebhookPayload): Promise<void> {
    try {
      // ... webhook call
      this.logger.log(
        `Webhook sent successfully for asset ${payload.asset_id}`,
        {
          assetId: payload.asset_id,
          userId: payload.user_id,
          requestId,
        }
      );
    } catch (error) {
      this.logger.error(
        `Webhook failed for asset ${payload.asset_id}: ${errorMessage}`,
        {
          assetId: payload.asset_id,
          userId: payload.user_id,
          requestId,
          error: errorMessage,
        }
      );
    }
  }
}
```

**Success Output:**
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

**Error Output:**
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

## Log Level Configuration (Requirement 20.6)

### Environment Variable

The log level is configurable via `IMMICH_LOG_LEVEL` environment variable:

```yaml
env:
  - name: IMMICH_LOG_LEVEL
    value: "log"  # Default: INFO level
```

### Supported Values

- `verbose` - Most detailed
- `debug` - Debug information
- `log` - Standard informational messages (default, equivalent to INFO)
- `warn` - Warnings only
- `error` - Errors only
- `fatal` - Fatal errors only

### Implementation

The log level is set during application bootstrap in `SystemConfigService`:

```typescript
@OnEvent({ name: 'ConfigInit', priority: -100 })
onConfigInit({ newConfig: { logging, machineLearning } }: ArgOf<'ConfigInit'>) {
  const { logLevel: envLevel } = this.configRepository.getEnv();
  const configLevel = logging.enabled ? logging.level : false;
  const level = envLevel ?? configLevel;
  this.logger.setLogLevel(level);
  this.logger.log(`LogLevel=${level} ${envLevel ? '(set via IMMICH_LOG_LEVEL)' : '(set via system config)'}`);
}
```

Priority order:
1. `IMMICH_LOG_LEVEL` environment variable (highest priority)
2. System config `logging.level` setting
3. Default: `log` (INFO)

## Verification Checklist

- ✅ **timestamp**: Automatically added by NestJS ConsoleLogger in ISO 8601 format
- ✅ **level**: Automatically added by NestJS ConsoleLogger (verbose, debug, log, warn, error, fatal)
- ✅ **service**: Set via `logger.setContext(ServiceName)` in each service constructor
- ✅ **message**: First parameter to all log method calls
- ✅ **context**: Additional parameters spread into JSON output
- ✅ **LOG_LEVEL configurable**: Via `IMMICH_LOG_LEVEL` environment variable (default: `log` = INFO)
- ✅ **JSON format**: Via `IMMICH_LOG_FORMAT=json` environment variable

## Conclusion

Immich's `LoggingRepository` with `IMMICH_LOG_FORMAT=json` fully satisfies Requirements 20.5 and 20.6:

1. **Structured JSON logging** with all required fields (timestamp, level, service, message, context)
2. **Configurable log level** via `IMMICH_LOG_LEVEL` environment variable with default INFO level
3. **Aesthetic integration logging** already implemented in `AestheticIntegrationService` and `WebhookService`

No code changes are required. The configuration is complete with the Kubernetes deployment update to set `IMMICH_LOG_FORMAT=json`.
