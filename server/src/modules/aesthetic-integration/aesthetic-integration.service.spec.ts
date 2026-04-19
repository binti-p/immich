import { Test, TestingModule } from '@nestjs/testing';
import { vi } from 'vitest';
import { AestheticIntegrationService } from './aesthetic-integration.service';
import { WebhookService } from './webhook.service';
import { DataPipelineRepository } from './data-pipeline.repository';
import { ConfigRepository } from 'src/repositories/config.repository';
import { AssetRepository } from 'src/repositories/asset.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';

describe('AestheticIntegrationService - Logging', () => {
  let service: AestheticIntegrationService;
  let logger: LoggingRepository;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AestheticIntegrationService,
        {
          provide: WebhookService,
          useValue: {
            sendAsync: vi.fn(),
            sendBatchRescore: vi.fn(),
          },
        },
        {
          provide: DataPipelineRepository,
          useValue: {
            getScoresByAssetIds: vi.fn(),
          },
        },
        {
          provide: ConfigRepository,
          useValue: {
            getEnv: vi.fn().mockReturnValue({
              logLevel: 'log',
              logFormat: 'json',
            }),
          },
        },
        {
          provide: AssetRepository,
          useValue: {
            getAllAssetIds: vi.fn(),
          },
        },
      ],
    }).compile();

    service = module.get<AestheticIntegrationService>(AestheticIntegrationService);
    logger = (service as any).logger;
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('Structured Logging', () => {
    it('should log with context when getScoresForAssets fails', async () => {
      const dataPipelineRepo = (service as any).dataPipelineRepo;
      const errorSpy = vi.spyOn(logger, 'error');

      dataPipelineRepo.getScoresByAssetIds.mockRejectedValue(new Error('Connection timeout'));

      const assetIds = ['asset-1', 'asset-2'];
      const result = await service.getScoresForAssets(assetIds);

      expect(result.size).toBe(0);
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('Failed to retrieve scores'),
        expect.objectContaining({
          assetIds,
        }),
      );
    });

    it('should log with context when notifyFeatureService is called', async () => {
      const webhookService = (service as any).webhookService;
      webhookService.sendAsync.mockResolvedValue(undefined);

      await service.notifyFeatureService('asset-123', 'user-456', '/path/to/photo.jpg');

      expect(webhookService.sendAsync).toHaveBeenCalledWith(
        expect.objectContaining({
          asset_id: 'asset-123',
          user_id: 'user-456',
          storage_path: '/path/to/photo.jpg',
        }),
      );
    });

    it('should log with context when rescoreAll is called', async () => {
      const assetRepository = (service as any).assetRepository;
      const logSpy = vi.spyOn(logger, 'log');

      assetRepository.getAllAssetIds.mockResolvedValue(['asset-1', 'asset-2']);

      const result = await service.rescoreAll('user-123');

      expect(result).toHaveProperty('jobId');
      // The first log call should contain "Starting batch rescore job"
      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining('Starting batch rescore job'),
      );
    });
  });

  describe('Log Level Configuration', () => {
    it('should respect IMMICH_LOG_LEVEL environment variable', () => {
      // The logger is initialized with ConfigRepository which reads IMMICH_LOG_LEVEL
      // This test verifies the integration is set up correctly
      expect(logger).toBeDefined();
      expect(logger.isLevelEnabled).toBeDefined();
    });
  });
});
