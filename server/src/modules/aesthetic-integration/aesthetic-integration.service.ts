import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { AssetRepository } from 'src/repositories/asset.repository';
import { DB } from 'src/schema';
import { AestheticService } from 'src/services/aesthetic.service';
import { AestheticScoreDto, RescoreAllResponseDto, ScoreCallbackPayload } from './dto/aesthetic-score.dto';

@Injectable()
export class AestheticIntegrationService {
  private readonly logger = new Logger(AestheticIntegrationService.name);

  constructor(
    @InjectKysely() private readonly db: Kysely<DB>,
    private readonly assetRepository: AssetRepository,
  ) {}

  async getScoresForAssets(assetIds: string[]): Promise<Map<string, AestheticScoreDto>> {
    if (assetIds.length === 0) {
      return new Map();
    }

    try {
      const rows = await this.db
        .selectFrom('aesthetic_scores')
        .select(['assetId', 'userId', 'score', 'alpha', 'modelVersion', 'scoredAt'])
        .where('assetId', 'in', assetIds as any)
        .execute();

      return new Map(
        rows.map((row) => [
          row.assetId as string,
          {
            assetId:           row.assetId as string,
            userId:            row.userId as string,
            score:             row.score as number,
            globalScore:       row.score as number,
            personalizedScore: null,
            alpha:             (row.alpha as number) ?? 0,
            modelVersion:      (row.modelVersion as string) ?? '',
            scoredAt:          row.scoredAt as Date,
          } satisfies AestheticScoreDto,
        ]),
      );
    } catch (error) {
      this.logger.error(
        `Failed to retrieve scores for ${assetIds.length} assets: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
      return new Map();
    }
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
      this.logger.error(
        `Failed to update aestheticScore for asset ${asset_id}: ${error instanceof Error ? error.message : error}`,
      );
      throw error;
    }
  }

  async rescoreAll(userId?: string): Promise<RescoreAllResponseDto> {
    const jobId = randomUUID();
    this.logger.log(`Starting rescore job ${jobId} ${userId ? `for user ${userId}` : 'for all users'}`);

    this.queueRescoreJob(jobId, userId).catch((err) => {
      this.logger.error(`Failed to queue rescore job ${jobId}: ${err.message}`);
    });

    return { jobId };
  }

  private async queueRescoreJob(jobId: string, userId?: string): Promise<void> {
    try {
      const assetIds = await this.assetRepository.getAllAssetIds(userId);

      if (assetIds.length === 0) {
        this.logger.log(`Rescore job ${jobId}: no assets found`);
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
        const asset = await this.assetRepository.getById(assetId);
        if (!asset) continue;
        aesthetic.scoreImage(assetId, asset.ownerId);
        processed++;
        if (processed % 10 === 0) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      this.logger.log(`Rescore job ${jobId}: queued ${processed} score-image calls`);
    } catch (error) {
      this.logger.error(`Rescore job ${jobId} failed: ${error instanceof Error ? error.message : error}`);
      throw error;
    }
  }
}
