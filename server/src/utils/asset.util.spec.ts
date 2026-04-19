import { sortByAestheticScore } from 'src/utils/asset.util';
import { describe, expect, it } from 'vitest';

describe('sortByAestheticScore', () => {
  it('should sort assets by aesthetic score in descending order', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.5 },
      { id: '2', name: 'asset2', aestheticScore: 0.9 },
      { id: '3', name: 'asset3', aestheticScore: 0.3 },
    ];

    const sorted = sortByAestheticScore(assets);

    expect(sorted[0].id).toBe('2'); // 0.9
    expect(sorted[1].id).toBe('1'); // 0.5
    expect(sorted[2].id).toBe('3'); // 0.3
  });

  it('should place assets with null scores at the end', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.5 },
      { id: '2', name: 'asset2', aestheticScore: null },
      { id: '3', name: 'asset3', aestheticScore: 0.9 },
      { id: '4', name: 'asset4', aestheticScore: null },
    ];

    const sorted = sortByAestheticScore(assets);

    expect(sorted[0].id).toBe('3'); // 0.9
    expect(sorted[1].id).toBe('1'); // 0.5
    expect(sorted[2].id).toBe('2'); // null
    expect(sorted[3].id).toBe('4'); // null
  });

  it('should place assets with undefined scores at the end', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.5 },
      { id: '2', name: 'asset2', aestheticScore: undefined },
      { id: '3', name: 'asset3', aestheticScore: 0.9 },
    ];

    const sorted = sortByAestheticScore(assets);

    expect(sorted[0].id).toBe('3'); // 0.9
    expect(sorted[1].id).toBe('1'); // 0.5
    expect(sorted[2].id).toBe('2'); // undefined
  });

  it('should handle all assets having null scores', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: null },
      { id: '2', name: 'asset2', aestheticScore: null },
      { id: '3', name: 'asset3', aestheticScore: null },
    ];

    const sorted = sortByAestheticScore(assets);

    // Order should be preserved when all scores are null
    expect(sorted).toHaveLength(3);
    expect(sorted.every(a => a.aestheticScore === null)).toBe(true);
  });

  it('should handle empty array', () => {
    const assets: Array<{ id: string; aestheticScore: number | null }> = [];

    const sorted = sortByAestheticScore(assets);

    expect(sorted).toHaveLength(0);
  });

  it('should handle single asset', () => {
    const assets = [{ id: '1', name: 'asset1', aestheticScore: 0.7 }];

    const sorted = sortByAestheticScore(assets);

    expect(sorted).toHaveLength(1);
    expect(sorted[0].id).toBe('1');
  });

  it('should handle assets with same scores', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.5 },
      { id: '2', name: 'asset2', aestheticScore: 0.5 },
      { id: '3', name: 'asset3', aestheticScore: 0.9 },
    ];

    const sorted = sortByAestheticScore(assets);

    expect(sorted[0].id).toBe('3'); // 0.9
    // Assets with same score (0.5) can be in any order
    expect([sorted[1].id, sorted[2].id]).toContain('1');
    expect([sorted[1].id, sorted[2].id]).toContain('2');
  });

  it('should handle mixed null and scored assets', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: null },
      { id: '2', name: 'asset2', aestheticScore: 0.8 },
      { id: '3', name: 'asset3', aestheticScore: null },
      { id: '4', name: 'asset4', aestheticScore: 0.2 },
      { id: '5', name: 'asset5', aestheticScore: 0.95 },
    ];

    const sorted = sortByAestheticScore(assets);

    // Scored assets first, in descending order
    expect(sorted[0].id).toBe('5'); // 0.95
    expect(sorted[1].id).toBe('2'); // 0.8
    expect(sorted[2].id).toBe('4'); // 0.2
    // Null assets at the end
    expect(sorted[3].aestheticScore).toBeNull();
    expect(sorted[4].aestheticScore).toBeNull();
  });

  it('should sort in-place and return the same array reference', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.5 },
      { id: '2', name: 'asset2', aestheticScore: 0.9 },
    ];

    const sorted = sortByAestheticScore(assets);

    // Should return the same array reference (in-place sort)
    expect(sorted).toBe(assets);
  });

  it('should handle edge case scores (0.0 and 1.0)', () => {
    const assets = [
      { id: '1', name: 'asset1', aestheticScore: 0.0 },
      { id: '2', name: 'asset2', aestheticScore: 1.0 },
      { id: '3', name: 'asset3', aestheticScore: 0.5 },
    ];

    const sorted = sortByAestheticScore(assets);

    expect(sorted[0].id).toBe('2'); // 1.0
    expect(sorted[1].id).toBe('3'); // 0.5
    expect(sorted[2].id).toBe('1'); // 0.0
  });
});
