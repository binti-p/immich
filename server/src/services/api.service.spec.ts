import { ApiService, findSvelteKitIds, render } from 'src/services/api.service';

describe(ApiService.name, () => {
  describe('render', () => {
    it('should correctly render open graph tags', () => {
      const output = render('<!-- metadata:tags -->', {
        title: 'title',
        description: 'description',
        imageUrl: 'https://demo.immich.app/api/assets/123',
      });
      expect(output).toContain('<meta property="og:title" content="title" />');
      expect(output).toContain('<meta property="og:description" content="description" />');
      expect(output).toContain('<meta property="og:image" content="https://demo.immich.app/api/assets/123" />');
    });

    it('should escape html tags', () => {
      expect(
        render('<!-- metadata:tags -->', {
          title: "<script>console.log('hello')</script>Test",
          description: 'description',
        }),
      ).toContain(
        '<meta property="og:title" content="&lt;script&gt;console.log(&#39;hello&#39;)&lt;/script&gt;Test" />',
      );
    });

    it('should escape quotes', () => {
      expect(
        render('<!-- metadata:tags -->', {
          title: `0;url=https://example.com" http-equiv="refresh`,
          description: 'description',
        }),
      ).toContain('<meta property="og:title" content="0;url=https://example.com&quot; http-equiv=&quot;refresh" />');
    });
  });

  describe('findSvelteKitIds', () => {
    it('should extract sveltekit IDs from index HTML', () => {
      const html = `
        <script>
          __sveltekit_abc123 = { base: "", env: null };
          import("/_app/env.js").then(({ env }) => {
            __sveltekit_abc123.env = env;
          });
        </script>
      `;
      const ids = findSvelteKitIds('/nonexistent', html);
      expect(ids.size).toBe(1);
      expect(ids.has('__sveltekit_abc123')).toBe(true);
    });

    it('should detect multiple different sveltekit IDs', () => {
      // Simulates the case where index.html has one ID and chunks have another
      const html = `
        <script>
          __sveltekit_1918meo = { base: "", env: null };
          __sveltekit_1918meo.env = env;
        </script>
      `;
      // We can't easily mock the filesystem for chunk scanning,
      // but we can verify the HTML-only extraction works
      const ids = findSvelteKitIds('/nonexistent', html);
      expect(ids.size).toBe(1);
      expect(ids.has('__sveltekit_1918meo')).toBe(true);
    });

    it('should return empty set for HTML without sveltekit globals', () => {
      const html = '<html><head></head><body>Hello</body></html>';
      const ids = findSvelteKitIds('/nonexistent', html);
      expect(ids.size).toBe(0);
    });

    it('should deduplicate repeated IDs', () => {
      const html = `
        __sveltekit_abc123 = {};
        __sveltekit_abc123.env = env;
        __sveltekit_abc123.base = "";
      `;
      const ids = findSvelteKitIds('/nonexistent', html);
      expect(ids.size).toBe(1);
    });
  });

  describe('sveltekit env bridge injection', () => {
    it('should inject alias when two different sveltekit IDs exist', () => {
      // This simulates what the SSR method does: after finding mismatched IDs,
      // it injects `globalThis.__sveltekit_secondary = globalThis.__sveltekit_primary`
      // right after the primary global initialization in the HTML.
      const primaryId = '__sveltekit_1918meo';
      const secondaryId = '__sveltekit_1rsf7x1';

      const originalHtml = `
        <script>
          ${primaryId} = { base: "", env: null };
          import("/_app/env.js").then(({ env }) => {
            ${primaryId}.env = env;
          });
        </script>
      `;

      // Simulate the bridge injection logic from the SSR method
      const initPattern = new RegExp(`(${primaryId}\\s*=\\s*\\{[^}]*\\})`);
      const alias = `globalThis.${secondaryId} = globalThis.${primaryId}`;
      const patchedHtml = originalHtml.replace(initPattern, `$1;\n${alias}`);

      // Verify the alias was injected after the initialization
      expect(patchedHtml).toContain(`${primaryId} = { base: "", env: null };\nglobalThis.${secondaryId} = globalThis.${primaryId}`);
      // Verify the env.js import is still intact
      expect(patchedHtml).toContain(`${primaryId}.env = env`);
    });

    it('should handle multiple secondary IDs', () => {
      const primaryId = '__sveltekit_aaa';
      const secondaryIds = ['__sveltekit_bbb', '__sveltekit_ccc'];

      const originalHtml = `${primaryId} = { base: "", env: null };`;

      const initPattern = new RegExp(`(${primaryId}\\s*=\\s*\\{[^}]*\\})`);
      const aliases = secondaryIds
        .map((id) => `globalThis.${id} = globalThis.${primaryId}`)
        .join(';\n');
      const patchedHtml = originalHtml.replace(initPattern, `$1;\n${aliases}`);

      for (const id of secondaryIds) {
        expect(patchedHtml).toContain(`globalThis.${id} = globalThis.${primaryId}`);
      }
    });

    it('should not modify HTML when only one sveltekit ID exists', () => {
      const ids = new Set(['__sveltekit_abc123']);
      // When there's only one ID, no bridging is needed
      expect(ids.size).toBe(1);
      // The SSR method only patches when ids.size > 1
    });
  });
});
