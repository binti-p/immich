import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { ConfigRepository } from 'src/repositories/config.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { TelemetryRepository } from 'src/repositories/telemetry.repository';
import { UploadWebhookPayload } from './dto/aesthetic-score.dto';

@Injectable()
export class WebhookService {
  private readonly logger = new LoggingRepository(undefined, undefined);

  constructor(
    private readonly configRepository: ConfigRepository,
    private readonly telemetryRepository: TelemetryRepository,
  ) {
    this.logger.setContext(WebhookService.name);
  }

  async sendAsync(payload: UploadWebhookPayload): Promise<void> {
    const featureServiceUrl = process.env.FEATURE_SERVICE_URL;

    if (!featureServiceUrl) {
      this.logger.warn('FEATURE_SERVICE_URL not configured, skipping webhook');
      return;
    }

    const requestId = randomUUID();
    const url = `${featureServiceUrl}/process`;
    const startTime = performance.now();
    let status: 'success' | 'failure' = 'failure';

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Request-ID': requestId,
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      status = 'success';
      this.logger.log(`Webhook sent successfully for asset ${payload.asset_id}`, {
        assetId: payload.asset_id,
        userId: payload.user_id,
        requestId,
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Webhook failed for asset ${payload.asset_id}: ${errorMessage}`, {
        assetId: payload.asset_id,
        userId: payload.user_id,
        requestId,
        error: errorMessage,
      });
      // Fire-and-forget: log error but don't throw to ensure non-blocking behavior
    } finally {
      // Track metrics: upload event counter and Feature Service request duration
      // Requirements: 14.2, 14.3
      const duration = (performance.now() - startTime) / 1000; // Convert to seconds

      this.telemetryRepository.api.addToCounter(`immich_upload_events_total.${status}`, 1);
      this.telemetryRepository.api.addToHistogram('immich_feature_service_request_duration_seconds', duration);
    }
  }

  /**
   * Send batch rescore request to Feature Service
   * @param assetIds Array of asset IDs to rescore
   * @param userId Optional user ID for filtering
   */
  async sendBatchRescore(assetIds: string[], userId?: string): Promise<void> {
    const featureServiceUrl = process.env.FEATURE_SERVICE_URL;

    if (!featureServiceUrl) {
      this.logger.warn('FEATURE_SERVICE_URL not configured, skipping batch rescore webhook');
      return;
    }

    const requestId = randomUUID();
    const url = `${featureServiceUrl}/batch-rescore`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout for batch

      const payload = {
        asset_ids: assetIds,
        user_id: userId,
        request_id: requestId,
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Request-ID': requestId,
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      this.logger.log(`Batch rescore webhook sent successfully for ${assetIds.length} assets`, {
        assetCount: assetIds.length,
        userId,
        requestId,
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Batch rescore webhook failed for ${assetIds.length} assets: ${errorMessage}`, {
        assetCount: assetIds.length,
        userId,
        requestId,
        error: errorMessage,
      });
      throw error; // Throw error so caller can handle and log
    }
  }
}
