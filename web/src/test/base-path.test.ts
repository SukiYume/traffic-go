import { describe, expect, it } from 'vitest';
import { deriveAppBasePath } from '../base-path';

describe('base path helpers', () => {
  it('derives a nested nginx prefix from built asset URLs', () => {
    expect(deriveAppBasePath('https://example.com/ops/traffic/assets/index.js')).toBe(
      '/ops/traffic',
    );
  });

  it('returns root for top-level assets and unparsable URLs', () => {
    expect(deriveAppBasePath('https://example.com/assets/index.js')).toBe('');
    expect(deriveAppBasePath('::not-a-url::')).toBe('');
  });
});
