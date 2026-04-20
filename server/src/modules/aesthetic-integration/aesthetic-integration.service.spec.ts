import { Test, TestingModule } from '@nestjs/testing';
import { vi, beforeEach, describe, it, expect } from 'vitest';
import { AestheticIntegrationService } from './aesthetic-integration.service';
import { AssetRepository } from 'src/repositories/asset.repository';

const mockDb = {
  selectFrom: vi.fn().mockReturnThis(),
  select: vi.fn().mockReturnThis(),
  where: vi.fn().mockReturnThis(),
  execute: vi.fn().mockResolvedValue([]),
};

describe('AestheticIntegrationService', () => {
  let service: AestheticIntegrationService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AestheticIntegrationService,
        {
          provide: 'KyselyModuleConnectionToken',
          useValue: mockDb,
        },
        {
          provide: AssetRepository,
          useValue: {
            getAllAssetIds: vi.fn().mockResolvedValue([]),
            getById: vi.fn().mockResolvedValue(null),
            updateAestheticScore: vi.fn().mockResolvedValue(undefined),
          },
        },
      ],
    }).compile();

    service = module.get<AestheticIntegrationService>(AestheticIntegrationService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('getScoresForAssets', () => {
    it('returns empty map for empty input', async () => {
      const result = await service.getScoresForAssets([]);
      expect(result.size).toBe(0);
    });

    it('returns empty map on DB error (graceful degradation)', async () => {
      mockDb.execute.mockRejectedValueOnce(new Error('DB error'));
      const result = await service.getScoresForAssets(['asset-1']);
      expect(result.size).toBe(0);
    });
  });

  describe('rescoreAll', () => {
    it('returns a jobId immediately', async () => {
      const result = await service.rescoreAll();
      expect(result).toHaveProperty('jobId');
      expect(typeof result.jobId).toBe('string');
    });
  });
});
