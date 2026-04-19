import { BadRequestException } from '@nestjs/common';
import { TimelineSortMode } from 'src/dtos/time-bucket.dto';
import { AssetVisibility } from 'src/enum';
import { TimelineService } from 'src/services/timeline.service';
import { authStub } from 'test/fixtures/auth.stub';
import { newTestService, ServiceMocks } from 'test/utils';

describe(TimelineService.name, () => {
  let sut: TimelineService;
  let mocks: ServiceMocks;

  beforeEach(() => {
    ({ sut, mocks } = newTestService(TimelineService));
  });

  describe('getTimeBuckets', () => {
    it("should return buckets if userId and albumId aren't set", async () => {
      mocks.asset.getTimeBuckets.mockResolvedValue([{ timeBucket: 'bucket', count: 1 }]);

      await expect(sut.getTimeBuckets(authStub.admin, {})).resolves.toEqual(
        expect.arrayContaining([{ timeBucket: 'bucket', count: 1 }]),
      );
      expect(mocks.asset.getTimeBuckets).toHaveBeenCalledWith({
        userIds: [authStub.admin.user.id],
      });
    });

    it('should pass bbox options to repository when all bbox fields are provided', async () => {
      mocks.asset.getTimeBuckets.mockResolvedValue([{ timeBucket: 'bucket', count: 1 }]);

      await sut.getTimeBuckets(authStub.admin, {
        bbox: {
          west: -70,
          south: -30,
          east: 120,
          north: 55,
        },
      });

      expect(mocks.asset.getTimeBuckets).toHaveBeenCalledWith({
        userIds: [authStub.admin.user.id],
        bbox: { west: -70, south: -30, east: 120, north: 55 },
      });
    });
  });

  describe('getTimeBucketsWithTotal', () => {
    it('should return buckets with total count', async () => {
      mocks.asset.getTimeBuckets.mockResolvedValue([
        { timeBucket: '2024-01-01', count: 10 },
        { timeBucket: '2024-01-02', count: 20 },
        { timeBucket: '2024-01-03', count: 30 },
      ]);

      await expect(sut.getTimeBucketsWithTotal(authStub.admin, {})).resolves.toEqual({
        buckets: [
          { timeBucket: '2024-01-01', count: 10 },
          { timeBucket: '2024-01-02', count: 20 },
          { timeBucket: '2024-01-03', count: 30 },
        ],
        totalCount: 60,
      });
      expect(mocks.asset.getTimeBuckets).toHaveBeenCalledWith({
        userIds: [authStub.admin.user.id],
      });
    });

    it('should return zero total count for empty buckets', async () => {
      mocks.asset.getTimeBuckets.mockResolvedValue([]);

      await expect(sut.getTimeBucketsWithTotal(authStub.admin, {})).resolves.toEqual({
        buckets: [],
        totalCount: 0,
      });
    });

    it('should calculate total count correctly with single bucket', async () => {
      mocks.asset.getTimeBuckets.mockResolvedValue([{ timeBucket: '2024-01-01', count: 42 }]);

      await expect(sut.getTimeBucketsWithTotal(authStub.admin, {})).resolves.toEqual({
        buckets: [{ timeBucket: '2024-01-01', count: 42 }],
        totalCount: 42,
      });
    });
  });

  describe('getTimeBucket', () => {
    it('should return the assets for a album time bucket if user has album.read', async () => {
      mocks.access.album.checkOwnerAccess.mockResolvedValue(new Set(['album-id']));
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(sut.getTimeBucket(authStub.admin, { timeBucket: 'bucket', albumId: 'album-id' })).resolves.toEqual(
        json,
      );

      expect(mocks.access.album.checkOwnerAccess).toHaveBeenCalledWith(authStub.admin.user.id, new Set(['album-id']));
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        {
          timeBucket: 'bucket',
          albumId: 'album-id',
        },
        authStub.admin,
      );
    });

    it('should return the assets for a archive time bucket if user has archive.read', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          visibility: AssetVisibility.Archive,
          userId: authStub.admin.user.id,
        }),
      ).resolves.toEqual(json);
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        expect.objectContaining({
          timeBucket: 'bucket',
          visibility: AssetVisibility.Archive,
          userIds: [authStub.admin.user.id],
        }),
        authStub.admin,
      );
    });

    it('should include partner shared assets', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });
      mocks.partner.getAll.mockResolvedValue([]);

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          visibility: AssetVisibility.Timeline,
          userId: authStub.admin.user.id,
          withPartners: true,
        }),
      ).resolves.toEqual(json);
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        {
          timeBucket: 'bucket',
          visibility: AssetVisibility.Timeline,
          withPartners: true,
          userIds: [authStub.admin.user.id],
        },
        authStub.admin,
      );
    });

    it('should check permissions to read tag', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });
      mocks.access.tag.checkOwnerAccess.mockResolvedValue(new Set(['tag-123']));

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          userId: authStub.admin.user.id,
          tagId: 'tag-123',
        }),
      ).resolves.toEqual(json);
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        {
          tagId: 'tag-123',
          timeBucket: 'bucket',
          userIds: [authStub.admin.user.id],
        },
        authStub.admin,
      );
    });

    it('should return the assets for a library time bucket if user has library.read', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          userId: authStub.admin.user.id,
        }),
      ).resolves.toEqual(json);
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        expect.objectContaining({
          timeBucket: 'bucket',
          userIds: [authStub.admin.user.id],
        }),
        authStub.admin,
      );
    });

    it('should throw an error if withParners is true and visibility true or undefined', async () => {
      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          visibility: AssetVisibility.Archive,
          withPartners: true,
          userId: authStub.admin.user.id,
        }),
      ).rejects.toThrow(BadRequestException);

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          visibility: undefined,
          withPartners: true,
          userId: authStub.admin.user.id,
        }),
      ).rejects.toThrow(BadRequestException);
    });

    it('should throw an error if withParners is true and isFavorite is either true or false', async () => {
      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          isFavorite: true,
          withPartners: true,
          userId: authStub.admin.user.id,
        }),
      ).rejects.toThrow(BadRequestException);

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          isFavorite: false,
          withPartners: true,
          userId: authStub.admin.user.id,
        }),
      ).rejects.toThrow(BadRequestException);
    });

    it('should throw an error if withParners is true and isTrash is true', async () => {
      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          isTrashed: true,
          withPartners: true,
          userId: authStub.admin.user.id,
        }),
      ).rejects.toThrow(BadRequestException);
    });

    it('should forward sortBy=aesthetic to the repository options', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          userId: authStub.admin.user.id,
          sortBy: TimelineSortMode.Aesthetic,
        }),
      ).resolves.toEqual(json);

      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        expect.objectContaining({
          timeBucket: 'bucket',
          userIds: [authStub.admin.user.id],
          sortBy: TimelineSortMode.Aesthetic,
        }),
        authStub.admin,
      );
    });

    it('should call the repository with sortBy: undefined when sortBy is absent', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          userId: authStub.admin.user.id,
        }),
      ).resolves.toEqual(json);

      const [, options] = mocks.asset.getTimeBucket.mock.calls[0];
      expect(options.sortBy).toBeUndefined();
    });

    it('should forward sortBy=date to the repository options', async () => {
      const json = `[{ id: ['asset-id'] }]`;
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      await expect(
        sut.getTimeBucket(authStub.admin, {
          timeBucket: 'bucket',
          userId: authStub.admin.user.id,
          sortBy: TimelineSortMode.Date,
        }),
      ).resolves.toEqual(json);

      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        'bucket',
        expect.objectContaining({
          timeBucket: 'bucket',
          userIds: [authStub.admin.user.id],
          sortBy: TimelineSortMode.Date,
        }),
        authStub.admin,
      );
    });

    // Integration-style tests (6.1, 6.2): verify response content with concrete example data

    it('6.1 - with sortBy=aesthetic, scored assets appear before unscored assets in the response', async () => {
      // Simulate a repository response where the DB has already applied aesthetic sort:
      // asset-scored-high (0.9), asset-scored-low (0.3), asset-unscored (null)
      const responsePayload = {
        id: ['asset-scored-high', 'asset-scored-low', 'asset-unscored'],
        aestheticScore: [0.9, 0.3, null],
        fileCreatedAt: ['2024-01-01T12:00:00Z', '2024-01-01T11:00:00Z', '2024-01-01T10:00:00Z'],
        ownerId: ['user-1', 'user-1', 'user-1'],
        ratio: [1, 1, 1],
        isFavorite: [false, false, false],
        visibility: ['TIMELINE', 'TIMELINE', 'TIMELINE'],
        isTrashed: [false, false, false],
        isImage: [true, true, true],
        thumbhash: [null, null, null],
        localOffsetHours: [0, 0, 0],
        duration: [null, null, null],
        projectionType: [null, null, null],
        livePhotoVideoId: [null, null, null],
        city: [null, null, null],
        country: [null, null, null],
      };
      const json = JSON.stringify(responsePayload);
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      const result = await sut.getTimeBucket(authStub.admin, {
        timeBucket: '2024-01-01',
        userId: authStub.admin.user.id,
        sortBy: TimelineSortMode.Aesthetic,
      });

      const parsed = JSON.parse(result);

      // Verify the repository was called with sortBy=aesthetic
      expect(mocks.asset.getTimeBucket).toHaveBeenCalledWith(
        '2024-01-01',
        expect.objectContaining({ sortBy: TimelineSortMode.Aesthetic }),
        authStub.admin,
      );

      // Verify scored assets appear before unscored assets
      const scoredIndices = parsed.aestheticScore
        .map((score: number | null, i: number) => ({ score, i }))
        .filter(({ score }: { score: number | null }) => score !== null)
        .map(({ i }: { i: number }) => i);
      const unscoredIndices = parsed.aestheticScore
        .map((score: number | null, i: number) => ({ score, i }))
        .filter(({ score }: { score: number | null }) => score === null)
        .map(({ i }: { i: number }) => i);

      // Every scored index must be less than every unscored index
      for (const s of scoredIndices) {
        for (const u of unscoredIndices) {
          expect(s).toBeLessThan(u);
        }
      }

      // Verify scored assets are in descending order
      const scoredScores = scoredIndices.map((i: number) => parsed.aestheticScore[i] as number);
      for (let i = 0; i < scoredScores.length - 1; i++) {
        expect(scoredScores[i]).toBeGreaterThanOrEqual(scoredScores[i + 1]);
      }

      // Verify the specific IDs are in the expected order
      expect(parsed.id[0]).toBe('asset-scored-high');
      expect(parsed.id[1]).toBe('asset-scored-low');
      expect(parsed.id[2]).toBe('asset-unscored');
    });

    it('6.2 - without sortBy, assets are returned in chronological (descending) order', async () => {
      // Simulate a repository response where the DB has applied default chronological sort (desc)
      const responsePayload = {
        id: ['asset-newest', 'asset-middle', 'asset-oldest'],
        aestheticScore: [null, null, null],
        fileCreatedAt: ['2024-01-03T12:00:00Z', '2024-01-02T12:00:00Z', '2024-01-01T12:00:00Z'],
        ownerId: ['user-1', 'user-1', 'user-1'],
        ratio: [1, 1, 1],
        isFavorite: [false, false, false],
        visibility: ['TIMELINE', 'TIMELINE', 'TIMELINE'],
        isTrashed: [false, false, false],
        isImage: [true, true, true],
        thumbhash: [null, null, null],
        localOffsetHours: [0, 0, 0],
        duration: [null, null, null],
        projectionType: [null, null, null],
        livePhotoVideoId: [null, null, null],
        city: [null, null, null],
        country: [null, null, null],
      };
      const json = JSON.stringify(responsePayload);
      mocks.asset.getTimeBucket.mockResolvedValue({ assets: json });

      const result = await sut.getTimeBucket(authStub.admin, {
        timeBucket: '2024-01-01',
        userId: authStub.admin.user.id,
        // no sortBy — should default to chronological
      });

      const parsed = JSON.parse(result);

      // Verify the repository was called without sortBy (undefined)
      const [, options] = mocks.asset.getTimeBucket.mock.calls[0];
      expect(options.sortBy).toBeUndefined();

      // Verify assets are in descending chronological order
      const dates = parsed.fileCreatedAt.map((d: string) => new Date(d).getTime());
      for (let i = 0; i < dates.length - 1; i++) {
        expect(dates[i]).toBeGreaterThanOrEqual(dates[i + 1]);
      }

      // Verify the specific IDs are in the expected order (newest first)
      expect(parsed.id[0]).toBe('asset-newest');
      expect(parsed.id[1]).toBe('asset-middle');
      expect(parsed.id[2]).toBe('asset-oldest');
    });
  });
});
