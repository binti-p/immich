import { Injectable, NotAcceptableException } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { escape } from 'lodash';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { ConfigRepository } from 'src/repositories/config.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { AuthService } from 'src/services/auth.service';
import { SharedLinkService } from 'src/services/shared-link.service';
import { OpenGraphTags } from 'src/utils/misc';

export const render = (index: string, meta: OpenGraphTags) => {
  const [title, description, imageUrl] = [meta.title, meta.description, meta.imageUrl].map((item) =>
    item ? escape(item) : '',
  );

  const tags = `
    <meta name="description" content="${description}" />

    <!-- Facebook Meta Tags -->
    <meta property="og:type" content="website" />
    <meta property="og:title" content="${title}" />
    <meta property="og:description" content="${description}" />
    ${imageUrl ? `<meta property="og:image" content="${imageUrl}" />` : ''}

    <!-- Twitter Meta Tags -->
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="${title}" />
    <meta name="twitter:description" content="${description}" />

    ${imageUrl ? `<meta name="twitter:image" content="${imageUrl}" />` : ''}`;

  return index.replace('<!-- metadata:tags -->', tags);
};

/**
 * Scans the built web assets for all unique __sveltekit_* app IDs.
 * When @immich/ui (or other pre-built SvelteKit libs) are bundled, they may
 * use a different app ID than the main web app, causing $env/dynamic/public
 * to read from a global that was never populated by the bootstrap script.
 */
export function findSvelteKitIds(webRoot: string, indexHtml: string): Set<string> {
  const ids = new Set<string>();
  const pattern = /__sveltekit_[a-z0-9]+/g;

  // IDs from index.html
  for (const match of indexHtml.matchAll(pattern)) {
    ids.add(match[0]);
  }

  // IDs from JS chunks
  try {
    const chunksDir = join(webRoot, '_app', 'immutable', 'chunks');
    for (const file of readdirSync(chunksDir)) {
      if (!file.endsWith('.js')) {
        continue;
      }
      const content = readFileSync(join(chunksDir, file), 'utf8');
      for (const match of content.matchAll(pattern)) {
        ids.add(match[0]);
      }
    }
  } catch {
    // chunks dir may not exist in dev
  }

  return ids;
}

@Injectable()
export class ApiService {
  constructor(
    private authService: AuthService,
    private sharedLinkService: SharedLinkService,
    private configRepository: ConfigRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(ApiService.name);
  }

  ssr(excludePaths: string[]) {
    const { resourcePaths } = this.configRepository.getEnv();

    let index = '';
    try {
      index = readFileSync(resourcePaths.web.indexHtml).toString();

      // Bridge mismatched __sveltekit_* app IDs between the main app and
      // pre-built libraries like @immich/ui. The primary ID (from index.html)
      // is the one whose env gets populated; we alias all others to it.
      const sveltekitIds = findSvelteKitIds(resourcePaths.web.root, index);
      if (sveltekitIds.size > 1) {
        const [primaryId, ...secondaryIds] = sveltekitIds;
        // We need the alias to happen AFTER the primary global is initialized
        // but BEFORE the JS chunks try to access the secondary globals.
        // The inline bootstrap script sets `__sveltekit_XXX = { base: "", env: null }`
        // then imports env.js which sets .env, then imports start.js/app.js.
        // We inject aliases right after the primary global initialization.
        const initPattern = new RegExp(
          `(${primaryId}\\s*=\\s*\\{[^}]*\\})`,
        );
        const aliases = secondaryIds
          .map((id) => `globalThis.${id} = globalThis.${primaryId}`)
          .join(';\n');
        index = index.replace(initPattern, `$1;\n${aliases}`);
        this.logger.log(`SvelteKit env bridge: aliased ${secondaryIds.join(', ')} → ${primaryId}`);
      }
    } catch {
      this.logger.warn(`Unable to open ${resourcePaths.web.indexHtml}, skipping SSR.`);
    }

    return async (request: Request, res: Response, next: NextFunction) => {
      const method = request.method.toLowerCase();
      if (
        request.url.startsWith('/api') ||
        (method !== 'get' && method !== 'head') ||
        excludePaths.some((item) => request.url.startsWith(item))
      ) {
        return next();
      }

      const responseType = request.accepts('text/html');
      if (!responseType) {
        throw new NotAcceptableException(
          `The route ${request.path} was requested as ${request.header('accept')}, but only returns text/html`,
        );
      }

      let status = 200;
      let html = index;

      const defaultDomain = request.host ? `${request.protocol}://${request.host}` : undefined;

      let meta: OpenGraphTags | null = null;

      const shareKey = request.url.match(/^\/share\/(.+)$/);
      if (shareKey) {
        try {
          const key = shareKey[1];
          const auth = await this.authService.validateSharedLinkKey(key);
          meta = await this.sharedLinkService.getMetadataTags(auth, defaultDomain);
        } catch {
          status = 404;
        }
      }

      const shareSlug = request.url.match(/^\/s\/(.+)$/);
      if (shareSlug) {
        try {
          const slug = shareSlug[1];
          const auth = await this.authService.validateSharedLinkSlug(slug);
          meta = await this.sharedLinkService.getMetadataTags(auth, defaultDomain);
        } catch {
          status = 404;
        }
      }

      if (meta) {
        html = render(index, meta);
      }

      res.status(status).type(responseType).header('Cache-Control', 'no-store').send(html);
    };
  }
}
