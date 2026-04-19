import { Injectable, Logger } from '@nestjs/common';
import { Kysely } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DB } from 'src/schema';
import { AestheticScoreDto } from './dto/aesthetic-score.dto';

/**
 * Repository for querying aesthetic scores from the Immich PostgreSQL database.
 * The aesthetic_scores table lives in the same DB as the rest of Immich.
 */
@Injectable()
export class DataPipelineRepository {
  private readonly logger = new Logger(DataPipelineRepository.name);

  constructor(@InjectKysely() private readonly db: Kysely<DB>) {}

  /**
   * Retrieve aesthetic scores for a batch of asset IDs.
   */
  async getScoresByAssetIds(assetIds: string[]): Promise<AestheticScoreDto[]> {
    if (assetIds.length === 0) {
      return [];
    }

    try {
      const rows = await this.db
        .selectFrom('aesthetic_scores as s')
        .select([
          's.asset_id',
          's.user_id',
          's.score',
          's.model_version',
          's.is_cold_start',
          's.alpha',
          's.inference_request_id',
          's.scored_at',
        ])
        .where('s.asset_id', 'in', assetIds as any)
        .execute();

      return rows.map((row: any) => ({
        assetId: row.asset_id,
        userId: row.user_id,
        score: row.score,
        globalScore: row.score,       // single score column — use as both
        personalizedScore: null,
        alpha: row.alpha ?? 0,
        modelVersion: row.model_version ?? '',
        scoredAt: row.scored_at,
      }));
    } catch (error) {
      this.logger.error(`Failed to query aesthetic scores for ${assetIds.length} assets`, error);
      throw error;
    }
  }
}
