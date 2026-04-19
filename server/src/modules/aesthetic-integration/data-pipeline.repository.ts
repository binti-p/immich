import { Injectable, Logger } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DB } from 'src/schema';
import { AestheticScoreDto } from './dto/aesthetic-score.dto';

/**
 * Repository for querying aesthetic scores from the main Immich database.
 * Tables are created by migration 1780000000000-AddAestheticHub.
 */
@Injectable()
export class DataPipelineRepository {
  private readonly logger = new Logger(DataPipelineRepository.name);

  constructor(@InjectKysely() private readonly db: Kysely<DB>) {}

  async getScoresByAssetIds(assetIds: string[]): Promise<AestheticScoreDto[]> {
    if (assetIds.length === 0) {
      return [];
    }

    try {
      type ScoreRow = {
        asset_id: string;
        user_id: string;
        score: number;
        global_score: number;
        personalized_score: number | null;
        alpha: number;
        model_version: string;
        scored_at: Date;
      };

      const result = await sql<ScoreRow>`
        SELECT asset_id, user_id, score, global_score, personalized_score, alpha, model_version, scored_at
        FROM aesthetic_scores
        WHERE asset_id = ANY(${sql.val(assetIds)}::uuid[])
      `.execute(this.db);

      return result.rows.map((row) => ({
        assetId: row.asset_id,
        userId: row.user_id,
        score: row.score,
        globalScore: row.global_score,
        personalizedScore: row.personalized_score,
        alpha: row.alpha,
        modelVersion: row.model_version,
        scoredAt: row.scored_at,
      }));
    } catch (error) {
      this.logger.error(`Failed to query aesthetic scores for ${assetIds.length} assets`, error);
      throw error;
    }
  }
}
