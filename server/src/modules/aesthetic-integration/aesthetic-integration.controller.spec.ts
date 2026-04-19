import { Test, TestingModule } from '@nestjs/testing';
import { Mocked, vitest } from 'vitest';
import { AestheticIntegrationController } from './aesthetic-integration.controller';
import { AestheticIntegrationService } from './aesthetic-integration.service';

describe('AestheticIntegrationController', () => {
  let controller: AestheticIntegrationController;
  let service: Mocked<AestheticIntegrationService>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AestheticIntegrationController],
      providers: [
        {
          provide: AestheticIntegrationService,
          useValue: {
            rescoreAll: vitest.fn(),
          },
        },
      ],
    }).compile();

    controller = module.get<AestheticIntegrationController>(AestheticIntegrationController);
    service = module.get<AestheticIntegrationService>(
      AestheticIntegrationService,
    ) as Mocked<AestheticIntegrationService>;
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });

  describe('rescoreAll', () => {
    it('should call service.rescoreAll with userId', async () => {
      const mockJobId = '123e4567-e89b-12d3-a456-426614174000';
      const mockAuth = { user: { id: 'admin-user-id', isAdmin: true } } as any;
      const mockDto = { userId: 'test-user-id' };

      service.rescoreAll.mockResolvedValue({ jobId: mockJobId });

      const result = await controller.rescoreAll(mockAuth, mockDto);

      expect(service.rescoreAll).toHaveBeenCalledWith('test-user-id');
      expect(result).toEqual({ jobId: mockJobId });
    });

    it('should call service.rescoreAll without userId', async () => {
      const mockJobId = '123e4567-e89b-12d3-a456-426614174000';
      const mockAuth = { user: { id: 'admin-user-id', isAdmin: true } } as any;
      const mockDto = {};

      service.rescoreAll.mockResolvedValue({ jobId: mockJobId });

      const result = await controller.rescoreAll(mockAuth, mockDto);

      expect(service.rescoreAll).toHaveBeenCalledWith(undefined);
      expect(result).toEqual({ jobId: mockJobId });
    });
  });
});
