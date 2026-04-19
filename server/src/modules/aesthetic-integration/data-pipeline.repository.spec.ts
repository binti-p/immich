import { Test, TestingModule } from '@nestjs/testing';
import { DataPipelineRepository } from './data-pipeline.repository';
import { vi, beforeEach, afterEach, describe, it, expect } from 'vitest';

// Mock the pg module
vi.mock('pg', () => {
  const mockPool = {
    query: vi.fn(),
    connect: vi.fn(),
    end: vi.fn(),
    on: vi.fn(),
    options: {
      max: 10,
      idleTimeoutMillis: 30000,
    },
  };
  return {
    Pool: vi.fn(() => mockPool),
  };
});

describe('DataPipelineRepository', () => {
  let repository: DataPipelineRepository;
  let mockPool: any;

  beforeEach(async () => {
    // Set up environment variables for testing
    process.env.DATA_PIPELINE_DB_HOST = 'localhost';
    process.env.DATA_PIPELINE_DB_PORT = '5432';
    process.env.DATA_PIPELINE_DB_NAME = 'test_db';
    process.env.DATA_PIPELINE_DB_USER = 'test_user';
    process.env.DATA_PIPELINE_DB_PASSWORD = 'test_password';

    const module: TestingModule = await Test.createTestingModule({
      providers: [DataPipelineRepository],
    }).compile();

    repository = module.get<DataPipelineRepository>(DataPipelineRepository);
    mockPool = repository.getPool();
    
    // Reset all mocks before each test
    vi.clearAllMocks();
  });

  afterEach(async () => {
    // Clean up the connection pool
    await repository.onModuleDestroy();
  });

  it('should be defined', () => {
    expect(repository).toBeDefined();
  });

  it('should initialize with correct pool configuration', () => {
    const pool = repository.getPool();
    expect(pool).toBeDefined();
    expect(pool.options.max).toBe(10);
    expect(pool.options.idleTimeoutMillis).toBe(30000);
  });

  describe('getScoresByAssetIds', () => {
    it('should return empty array for empty asset IDs', async () => {
      const result = await repository.getScoresByAssetIds([]);
      expect(result).toEqual([]);
      expect(mockPool.query).not.toHaveBeenCalled();
    });

    it('should query database with correct SQL for single asset ID', async () => {
      const assetIds = ['550e8400-e29b-41d4-a716-446655440000'];
      const mockRows = [
        {
          asset_id: '550e8400-e29b-41d4-a716-446655440000',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.8542,
          global_score: 0.82,
          personalized_score: 0.91,
          alpha: 0.3,
          model_version: 'v1.2.0',
          scored_at: new Date('2024-01-15T10:30:00Z'),
        },
      ];

      mockPool.query.mockResolvedValue({ rows: mockRows });

      const result = await repository.getScoresByAssetIds(assetIds);

      expect(mockPool.query).toHaveBeenCalledTimes(1);
      expect(mockPool.query).toHaveBeenCalledWith(
        expect.stringContaining('WHERE asset_id = ANY($1::uuid[])'),
        [assetIds]
      );
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        assetId: '550e8400-e29b-41d4-a716-446655440000',
        userId: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
        score: 0.8542,
        globalScore: 0.82,
        personalizedScore: 0.91,
        alpha: 0.3,
        modelVersion: 'v1.2.0',
        scoredAt: mockRows[0].scored_at,
      });
    });

    it('should query database with correct SQL for multiple asset IDs', async () => {
      const assetIds = [
        '550e8400-e29b-41d4-a716-446655440000',
        '660e8400-e29b-41d4-a716-446655440001',
        '770e8400-e29b-41d4-a716-446655440002',
      ];
      const mockRows = [
        {
          asset_id: '550e8400-e29b-41d4-a716-446655440000',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.8542,
          global_score: 0.82,
          personalized_score: 0.91,
          alpha: 0.3,
          model_version: 'v1.2.0',
          scored_at: new Date('2024-01-15T10:30:00Z'),
        },
        {
          asset_id: '660e8400-e29b-41d4-a716-446655440001',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.7234,
          global_score: 0.71,
          personalized_score: 0.75,
          alpha: 0.3,
          model_version: 'v1.2.0',
          scored_at: new Date('2024-01-15T10:31:00Z'),
        },
        {
          asset_id: '770e8400-e29b-41d4-a716-446655440002',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.9123,
          global_score: 0.89,
          personalized_score: 0.95,
          alpha: 0.3,
          model_version: 'v1.2.0',
          scored_at: new Date('2024-01-15T10:32:00Z'),
        },
      ];

      mockPool.query.mockResolvedValue({ rows: mockRows });

      const result = await repository.getScoresByAssetIds(assetIds);

      expect(mockPool.query).toHaveBeenCalledTimes(1);
      expect(mockPool.query).toHaveBeenCalledWith(
        expect.stringContaining('WHERE asset_id = ANY($1::uuid[])'),
        [assetIds]
      );
      expect(result).toHaveLength(3);
      expect(result[0].assetId).toBe('550e8400-e29b-41d4-a716-446655440000');
      expect(result[1].assetId).toBe('660e8400-e29b-41d4-a716-446655440001');
      expect(result[2].assetId).toBe('770e8400-e29b-41d4-a716-446655440002');
    });

    it('should correctly map database rows to AestheticScoreDto objects', async () => {
      const assetIds = ['550e8400-e29b-41d4-a716-446655440000'];
      const scoredAt = new Date('2024-01-15T10:30:00Z');
      const mockRows = [
        {
          asset_id: '550e8400-e29b-41d4-a716-446655440000',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.8542,
          global_score: 0.82,
          personalized_score: 0.91,
          alpha: 0.3,
          model_version: 'v1.2.0',
          scored_at: scoredAt,
        },
      ];

      mockPool.query.mockResolvedValue({ rows: mockRows });

      const result = await repository.getScoresByAssetIds(assetIds);

      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({
        assetId: '550e8400-e29b-41d4-a716-446655440000',
        userId: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
        score: 0.8542,
        globalScore: 0.82,
        personalizedScore: 0.91,
        alpha: 0.3,
        modelVersion: 'v1.2.0',
        scoredAt: scoredAt,
      });
    });

    it('should handle null personalized_score in result mapping', async () => {
      const assetIds = ['550e8400-e29b-41d4-a716-446655440000'];
      const mockRows = [
        {
          asset_id: '550e8400-e29b-41d4-a716-446655440000',
          user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
          score: 0.82,
          global_score: 0.82,
          personalized_score: null,
          alpha: 0.0,
          model_version: 'v1.2.0',
          scored_at: new Date('2024-01-15T10:30:00Z'),
        },
      ];

      mockPool.query.mockResolvedValue({ rows: mockRows });

      const result = await repository.getScoresByAssetIds(assetIds);

      expect(result).toHaveLength(1);
      expect(result[0].personalizedScore).toBeNull();
      expect(result[0].alpha).toBe(0.0);
    });

    it('should throw error when database query fails', async () => {
      const assetIds = ['550e8400-e29b-41d4-a716-446655440000'];
      const dbError = new Error('Database connection failed');

      mockPool.query.mockRejectedValue(dbError);

      await expect(repository.getScoresByAssetIds(assetIds)).rejects.toThrow(dbError);
      expect(mockPool.query).toHaveBeenCalledTimes(1);
    });

    it('should return empty array when no scores found for given asset IDs', async () => {
      const assetIds = ['550e8400-e29b-41d4-a716-446655440000'];
      mockPool.query.mockResolvedValue({ rows: [] });

      const result = await repository.getScoresByAssetIds(assetIds);

      expect(result).toEqual([]);
      expect(mockPool.query).toHaveBeenCalledTimes(1);
    });
  });

  describe('checkHealth', () => {
    it('should successfully check health when database is accessible', async () => {
      const mockClient = {
        query: vi.fn().mockResolvedValue({}),
        release: vi.fn(),
      };
      mockPool.connect.mockResolvedValue(mockClient);

      await expect(repository.checkHealth()).resolves.not.toThrow();
      expect(mockPool.connect).toHaveBeenCalledTimes(1);
      expect(mockClient.query).toHaveBeenCalledWith('SELECT 1');
      expect(mockClient.release).toHaveBeenCalledTimes(1);
    });

    it('should throw error when database is not accessible', async () => {
      const dbError = new Error('Connection refused');
      mockPool.connect.mockRejectedValue(dbError);

      await expect(repository.checkHealth()).rejects.toThrow('Data Pipeline database health check failed');
      expect(mockPool.connect).toHaveBeenCalledTimes(1);
    });

    it('should release client even when query fails', async () => {
      const mockClient = {
        query: vi.fn().mockRejectedValue(new Error('Query failed')),
        release: vi.fn(),
      };
      mockPool.connect.mockResolvedValue(mockClient);

      await expect(repository.checkHealth()).rejects.toThrow();
      expect(mockClient.release).toHaveBeenCalledTimes(1);
    });
  });
});
