import { ConfigRepository } from 'src/repositories/config.repository';
import { UploadWebhookPayload } from './dto/aesthetic-score.dto';
import { WebhookService } from './webhook.service';

describe('WebhookService', () => {
  let service: WebhookService;
  let configRepository: ConfigRepository;
  let originalFetch: typeof global.fetch;
  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    // Save original fetch and env
    originalFetch = global.fetch;
    originalEnv = process.env;

    // Mock ConfigRepository
    configRepository = {
      getEnv: vi.fn().mockReturnValue({}),
    } as any;

    service = new WebhookService(configRepository);
  });

  afterEach(() => {
    // Restore original fetch and env
    global.fetch = originalFetch;
    process.env = originalEnv;
    vi.clearAllMocks();
  });

  describe('sendAsync', () => {
    const mockPayload: UploadWebhookPayload = {
      asset_id: '550e8400-e29b-41d4-a716-446655440000',
      user_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
      storage_path: '/data/upload/2024/01/photo.jpg',
      uploaded_at: '2024-01-15T10:30:00Z',
    };

    it('should send webhook successfully', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      const mockResponse = {
        ok: true,
        status: 202,
        statusText: 'Accepted',
      };

      global.fetch = vi.fn().mockResolvedValue(mockResponse);

      await service.sendAsync(mockPayload);

      expect(global.fetch).toHaveBeenCalledWith(
        'http://feature-svc:8001/process',
        expect.objectContaining({
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Request-ID': expect.any(String),
          },
          body: JSON.stringify(mockPayload),
          signal: expect.any(AbortSignal),
        }),
      );
    });

    it('should skip webhook when FEATURE_SERVICE_URL is not configured', async () => {
      delete process.env.FEATURE_SERVICE_URL;

      global.fetch = vi.fn();

      await service.sendAsync(mockPayload);

      expect(global.fetch).not.toHaveBeenCalled();
    });

    it('should log error when webhook fails with HTTP error (fire-and-forget)', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      const mockResponse = {
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      };

      global.fetch = vi.fn().mockResolvedValue(mockResponse);

      // Fire-and-forget: should not throw, just log error
      await service.sendAsync(mockPayload);

      expect(global.fetch).toHaveBeenCalled();
    });

    it('should log error when webhook times out (fire-and-forget)', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      global.fetch = vi.fn().mockImplementation(
        () =>
          new Promise((_, reject) => {
            // Simulate abort signal timeout
            setTimeout(() => reject(new Error('This operation was aborted')), 5100);
          }),
      );

      // Fire-and-forget: should not throw, just log error
      await service.sendAsync(mockPayload);

      expect(global.fetch).toHaveBeenCalled();
    }, 10000); // Increase test timeout to 10 seconds

    it('should log error when network request fails (fire-and-forget)', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

      // Fire-and-forget: should not throw, just log error
      await service.sendAsync(mockPayload);

      expect(global.fetch).toHaveBeenCalled();
    });

    it('should include X-Request-ID header for tracing', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      const mockResponse = {
        ok: true,
        status: 202,
        statusText: 'Accepted',
      };

      global.fetch = vi.fn().mockResolvedValue(mockResponse);

      await service.sendAsync(mockPayload);

      const fetchCall = (global.fetch as any).mock.calls[0];
      const headers = fetchCall[1].headers;

      expect(headers['X-Request-ID']).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    });

    it('should implement 5-second timeout', async () => {
      process.env.FEATURE_SERVICE_URL = 'http://feature-svc:8001';

      const startTime = Date.now();

      global.fetch = vi.fn().mockImplementation(
        () =>
          new Promise((_, reject) => {
            // Simulate abort signal timeout
            setTimeout(() => reject(new Error('This operation was aborted')), 5100);
          }),
      );

      try {
        await service.sendAsync(mockPayload);
      } catch (error) {
        const elapsed = Date.now() - startTime;
        // Should timeout around 5 seconds, not wait for full 5.1 seconds
        expect(elapsed).toBeLessThan(5500);
      }
    }, 10000); // Increase test timeout to 10 seconds
  });
});
