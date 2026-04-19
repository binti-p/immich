import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool, PoolClient, QueryResult } from 'pg';
import { AestheticScoreDto } from './dto/aesthetic-score.dto';

/**
 * Repository for managing connections to the Data Pipeline PostgreSQL database.
 * This database stores aesthetic scores computed by the external scoring pipeline.
 * 
 * Requirements: 7.1, 7.5, 9.2
 */
@Injectable()
export class DataPipelineRepository implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DataPipelineRepository.name);
  private readonly pool: Pool;

  constructor() {
    // Initialize connection pool with environment variables
    this.pool = new Pool({
      host: process.env.DATA_PIPELINE_DB_HOST,
      port: Number.parseInt(process.env.DATA_PIPELINE_DB_PORT || '5432', 10),
      database: process.env.DATA_PIPELINE_DB_NAME,
      user: process.env.DATA_PIPELINE_DB_USER,
      password: process.env.DATA_PIPELINE_DB_PASSWORD,
      max: 10, // Maximum 10 connections in pool
      idleTimeoutMillis: 30000, // 30 seconds idle timeout
    });

    // Log pool errors
    this.pool.on('error', (err) => {
      this.logger.error('Unexpected error on idle Data Pipeline DB client', err);
    });
  }

  /**
   * Lifecycle hook: verify database connectivity on module initialization
   */
  async onModuleInit() {
    try {
      await this.checkHealth();
      this.logger.log('Data Pipeline database connection established successfully');
    } catch (error) {
      this.logger.error('Failed to connect to Data Pipeline database', error);
      // Don't throw - allow graceful degradation
    }
  }

  /**
   * Lifecycle hook: close all connections on module destruction
   */
  async onModuleDestroy() {
    try {
      await this.pool.end();
      this.logger.log('Data Pipeline database connection pool closed');
    } catch (error) {
      this.logger.error('Error closing Data Pipeline database connection pool', error);
    }
  }

  /**
   * Check database connectivity and health
   * @returns Promise that resolves if database is healthy
   * @throws Error if database is not accessible
   */
  async checkHealth(): Promise<void> {
    let client: PoolClient | undefined;
    try {
      client = await this.pool.connect();
      await client.query('SELECT 1');
    } catch (error) {
      throw new Error(`Data Pipeline database health check failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      if (client) {
        client.release();
      }
    }
  }

  /**
   * Retrieve aesthetic scores for a batch of asset IDs
   * @param assetIds Array of asset UUIDs to query
   * @returns Promise resolving to array of aesthetic score DTOs
   */
  async getScoresByAssetIds(assetIds: string[]): Promise<AestheticScoreDto[]> {
    if (assetIds.length === 0) {
      return [];
    }

    try {
      const query = `
        SELECT 
          asset_id, 
          user_id, 
          score, 
          global_score, 
          personalized_score, 
          alpha, 
          model_version, 
          scored_at
        FROM aesthetic_scores
        WHERE asset_id = ANY($1::uuid[])
      `;

      const result: QueryResult = await this.pool.query(query, [assetIds]);

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

  /**
   * Get the underlying connection pool for advanced usage
   * @returns The PostgreSQL connection pool
   */
  getPool(): Pool {
    return this.pool;
  }
}
