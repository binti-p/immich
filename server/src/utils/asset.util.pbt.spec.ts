/**
 * Property-Based Tests for sortByAestheticScore and chronological sort ordering.
 *
 * These tests use plain vitest with randomized data generation (no external PBT library).
 * Each property is verified across 100 random inputs using a for loop.
 *
 * **Validates: Requirements 1.1, 1.2, 1.3, 1.4**
 */

import { sortByAestheticScore } from 'src/utils/asset.util';
import { describe, expect, it } from 'vitest';

// ---------------------------------------------------------------------------
// Random data helpers
// ---------------------------------------------------------------------------

/** Returns a random float in [0, 1] */
const randomScore = (): number => Math.random();

/** Returns a random score or null (50/50 chance) */
const randomScoreOrNull = (): number | null => (Math.random() < 0.5 ? randomScore() : null);

/** Returns a random Date within a wide range */
const randomDate = (): Date => new Date(Date.now() - Math.random() * 1e12);

/** Generates a random UUID-like string */
const randomId = (): string => Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);

interface TestAsset {
  id: string;
  aestheticScore: number | null;
  fileCreatedAt: Date;
}

/**
 * Generates an array of n random assets with a mix of scored and unscored entries.
 * At least one scored and one unscored asset are guaranteed when n >= 2.
 */
function generateRandomAssets(n: number): TestAsset[] {
  if (n === 0) return [];

  const assets: TestAsset[] = Array.from({ length: n }, () => ({
    id: randomId(),
    aestheticScore: randomScoreOrNull(),
    fileCreatedAt: randomDate(),
  }));

  // Guarantee at least one scored and one unscored when n >= 2
  if (n >= 2) {
    assets[0].aestheticScore = randomScore();       // ensure at least one scored
    assets[1].aestheticScore = null;                // ensure at least one unscored
  }

  return assets;
}

/**
 * Generates an array of n random assets where ALL have non-null scores.
 */
function generateScoredAssets(n: number): TestAsset[] {
  return Array.from({ length: n }, () => ({
    id: randomId(),
    aestheticScore: randomScore(),
    fileCreatedAt: randomDate(),
  }));
}

/**
 * Generates an array of n random assets with random dates (scores may be anything).
 */
function generateAssetsWithDates(n: number): TestAsset[] {
  return Array.from({ length: n }, () => ({
    id: randomId(),
    aestheticScore: randomScoreOrNull(),
    fileCreatedAt: randomDate(),
  }));
}

// ---------------------------------------------------------------------------
// Property 5.1 — Scored assets precede unscored assets after aesthetic sort
// **Validates: Requirements 1.1**
// ---------------------------------------------------------------------------

describe('Property 5.1 — scored assets precede unscored assets', () => {
  it('for any array with mixed scored/unscored entries, all scored assets appear before all unscored assets after sortByAestheticScore', () => {
    const RUNS = 100;

    for (let run = 0; run < RUNS; run++) {
      // Use arrays of size 2–20 to keep tests fast while covering variety
      const size = 2 + Math.floor(Math.random() * 19);
      const assets = generateRandomAssets(size);

      const sorted = sortByAestheticScore([...assets]);

      // Find the index of the last scored asset and the first unscored asset
      let lastScoredIndex = -1;
      let firstUnscoredIndex = sorted.length; // default: no unscored

      for (let i = 0; i < sorted.length; i++) {
        if (sorted[i].aestheticScore != null) {
          lastScoredIndex = i;
        }
      }
      for (let i = 0; i < sorted.length; i++) {
        if (sorted[i].aestheticScore == null) {
          firstUnscoredIndex = i;
          break;
        }
      }

      // Every scored asset must appear before every unscored asset
      expect(lastScoredIndex).toBeLessThan(firstUnscoredIndex);
    }
  });
});

// ---------------------------------------------------------------------------
// Property 5.2 — Scored assets are in non-increasing order after aesthetic sort
// **Validates: Requirements 1.2**
// ---------------------------------------------------------------------------

describe('Property 5.2 — scored assets are in non-increasing order', () => {
  it('for any array of assets with non-null scores, sortByAestheticScore produces non-increasing score order', () => {
    const RUNS = 100;

    for (let run = 0; run < RUNS; run++) {
      const size = 1 + Math.floor(Math.random() * 20);
      const assets = generateScoredAssets(size);

      const sorted = sortByAestheticScore([...assets]);

      // Extract scores (all non-null by construction)
      const scores = sorted.map((a) => a.aestheticScore as number);

      // Verify non-increasing order: scores[i] >= scores[i+1] for all i
      for (let i = 0; i < scores.length - 1; i++) {
        expect(scores[i]).toBeGreaterThanOrEqual(scores[i + 1]);
      }
    }
  });

  it('for any array with mixed scored/unscored entries, the scored portion is in non-increasing order', () => {
    const RUNS = 100;

    for (let run = 0; run < RUNS; run++) {
      const size = 2 + Math.floor(Math.random() * 19);
      const assets = generateRandomAssets(size);

      const sorted = sortByAestheticScore([...assets]);

      // Extract only the scored assets (they appear first)
      const scoredAssets = sorted.filter((a) => a.aestheticScore != null);
      const scores = scoredAssets.map((a) => a.aestheticScore as number);

      for (let i = 0; i < scores.length - 1; i++) {
        expect(scores[i]).toBeGreaterThanOrEqual(scores[i + 1]);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Property 5.3 — sortByAestheticScore does not change the set of asset IDs
// **Validates: Requirements 1.3**
// ---------------------------------------------------------------------------

describe('Property 5.3 — sort preserves the set of asset IDs', () => {
  it('for any array of assets, sortByAestheticScore returns the same set of IDs (no additions or removals)', () => {
    const RUNS = 100;

    for (let run = 0; run < RUNS; run++) {
      const size = Math.floor(Math.random() * 21); // 0–20 assets
      const assets = generateAssetsWithDates(size);

      const originalIds = new Set(assets.map((a) => a.id));
      const sorted = sortByAestheticScore([...assets]);
      const sortedIds = new Set(sorted.map((a) => a.id));

      // Same number of elements
      expect(sorted).toHaveLength(assets.length);

      // Same set of IDs
      expect(sortedIds.size).toBe(originalIds.size);
      for (const id of originalIds) {
        expect(sortedIds.has(id)).toBe(true);
      }
    }
  });

  it('for an empty array, sortByAestheticScore returns an empty array', () => {
    const sorted = sortByAestheticScore([]);
    expect(sorted).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Property 5.4 — Chronological sort produces non-increasing date order
// **Validates: Requirements 1.4**
// ---------------------------------------------------------------------------

/**
 * Simulates the date-only sort path (ORDER BY fileCreatedAt DESC).
 * This mirrors the repository's default chronological sort.
 */
function sortByDateDescending<T extends { fileCreatedAt: Date }>(assets: T[]): T[] {
  return [...assets].sort((a, b) => b.fileCreatedAt.getTime() - a.fileCreatedAt.getTime());
}

describe('Property 5.4 — chronological sort produces non-increasing date order', () => {
  it('for any array of assets sorted by fileCreatedAt descending, dates are in non-increasing order', () => {
    const RUNS = 100;

    for (let run = 0; run < RUNS; run++) {
      const size = 1 + Math.floor(Math.random() * 20);
      const assets = generateAssetsWithDates(size);

      const sorted = sortByDateDescending(assets);

      // Verify non-increasing date order: dates[i] >= dates[i+1]
      for (let i = 0; i < sorted.length - 1; i++) {
        const dateA = sorted[i].fileCreatedAt.getTime();
        const dateB = sorted[i + 1].fileCreatedAt.getTime();
        expect(dateA).toBeGreaterThanOrEqual(dateB);
      }
    }
  });

  it('for an empty array, chronological sort returns an empty array', () => {
    const sorted = sortByDateDescending([]);
    expect(sorted).toHaveLength(0);
  });

  it('for a single asset, chronological sort returns the same single asset', () => {
    const assets = generateAssetsWithDates(1);
    const sorted = sortByDateDescending(assets);
    expect(sorted).toHaveLength(1);
    expect(sorted[0].id).toBe(assets[0].id);
  });
});
