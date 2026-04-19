import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { AssetRepository } from 'src/repositories/asset.repository';
import { AestheticService } from 'src/services/aesthetic.service';
import { AestheticScoreDto, RescoreAllResponseDto, ScoreCallbackPayload, UploadWebhookPayload } from './dto/aesthetic-score.dto';
import { DataPipelineRepository } from './data-pipeline.repository';
import { WebhookService } from './webhook.service';

@Injectable()
export class AestheticIntegrationService {
  private readonly logger = new Logger(AestheticIntegrationService.name);

  constructor(
    private readonly webhookService: WebhookService,
    private readonly dataPipelineRepo: DataPipelineRepository,
    private readonly assetRepository: AssetRepository,
  ) {}

  async getScoresForAssets(assetIds: string[]): Promise<Map<string, AestheticScoreDto>> {
    // Handle empty input gracefully
    if (assetIds.length === 0) {
      return new Map();
    }

    try {
      // Call DataPipelineRepository to batch query scores
      const scores = await this.dataPipelineRepo.getScoresByAssetIds(assetIds);

      // Convert array result to Map for O(1) lookup by asset ID
      return new Map(scores.map((s) => [s.assetId, s]));
    } catch (error) {
      // Log error but return empty map for graceful degradation
      this.logger.error(`Failed to retrieve scores for ${assetIds.length} assets: ${error instanceof Error ? error.message : 'Unknown error'}`, {
        assetIds,
      });
      return new Map();
    }
  }

  async notifyFeatureService(assetId: string, userId: string, storagePath: string): Promise<void> {
    // Check if aesthetic scoring is enabled (default: true)
    // This is a custom environment variable not part of Immich's standard config
    const enableAestheticScoring = process.env.ENABLE_AESTHETIC_SCORING !== 'false';
    if (!enableAestheticScoring) {
      this.logger.debug('Aesthetic scoring disabled, skipping webhook');
      return;
    }

    const payload: UploadWebhookPayload = {
      asset_id: assetId,
      user_id: userId,
      storage_path: storagePath,
      uploaded_at: new Date().toISOString(),
    };

    // Fire-and-forget: call webhook asynchronously without awaiting
    // Errors are logged by WebhookService but don't block the upload flow
    this.webhookService.sendAsync(payload).catch((err) => {
      // This catch is a safety net, but WebhookService already handles errors
      this.logger.error(`Unexpected error in webhook fire-and-forget: ${err.message}`, {
        assetId,
        userId,
      });
    });
  }

  async receiveScoreCallback(payload: ScoreCallbackPayload): Promise<void> {
    const { asset_id, score } = payload;

    if (score < 0 || score > 1) {
      this.logger.warn(`Received out-of-range score ${score} for asset ${asset_id}, clamping`);
    }

    const clampedScore = Math.min(1, Math.max(0, score));

    try {
      await this.assetRepository.updateAestheticScore(asset_id, clampedScore);
      this.logger.debug(`Updated aestheticScore=${clampedScore} for asset ${asset_id}`);
    } catch (error) {
      this.logger.error(`Failed to update aestheticScore for asset ${asset_id}: ${error instanceof Error ? error.message : error}`);
      throw error;
    }
  }

  async rescoreAll(userId?: string): Promise<RescoreAllResponseDto> {
    // Generate a unique job ID for tracking
    const jobId = randomUUID();

    const userScope = userId ? `for user ${userId}` : 'for all users';
    this.logger.log(`Starting batch rescore job ${jobId} ${userScope}`);

    // Queue the rescoring job asynchronously (fire-and-forget)
    // The actual implementation will be in Task 13.2
    this.queueRescoreJob(jobId, userId).catch((err) => {
      this.logger.error(`Failed to queue rescore job ${jobId}: ${err.message}`, {
        jobId,
        userId,
      });
    });

    // Return 202 Accepted with job ID immediately
    return { jobId };
  }

  private async queueRescoreJob(jobId: string, userId?: string): Promise<void> {
    try {
      const assetIds = await this.assetRepository.getAllAssetIds(userId);

      if (assetIds.length === 0) {
        this.logger.log(`Rescore job ${jobId} completed: No assets found`);
        return;
      }

      this.logger.log(`Rescore job ${jobId}: ${assetIds.length} assets to score`);

      const aesthetic = AestheticService.instance;
      if (!aesthetic) {
        this.logger.warn(`Rescore job ${jobId}: AestheticService not available`);
        return;
      }

      let processed = 0;
      for (const assetId of assetIds) {
        // Find the owner of this asset
        const asset = await this.assetRepository.getById(assetId);
        if (!asset) continue;
        aesthetic.scoreImage(assetId, asset.ownerId);
        processed++;
        // Small delay to avoid overwhelming aesthetic-service
        if (processed % 10 === 0) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      this.logger.log(`Rescore job ${jobId} queued ${processed} score-image calls`);
    } catch (error) {
      this.logger.error(`Rescore job ${jobId} failed: ${error instanceof Error ? error.message : error}`);
      throw error;
    }
  }
}
