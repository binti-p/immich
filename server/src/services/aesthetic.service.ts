import { Injectable } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { LoggingRepository } from 'src/repositories/logging.repository';

// Module-level singleton so services without explicit constructors can use it
let _instance: AestheticService | null = null;

@Injectable()
export class AestheticService {
  private readonly serviceUrl: string;

  constructor(private readonly logger: LoggingRepository) {
    this.logger.setContext(AestheticService.name);
    this.serviceUrl =
      process.env.AESTHETIC_SERVICE_URL ?? 'http://aesthetic-service:8002';
    _instance = this;
  }

  /** Access the singleton from services that can't inject it via constructor */
  static get instance(): AestheticService | null {
    return _instance;
  }

  /**
   * Fire-and-forget: POST /users/register to aesthetic-service.
   * Called when a new Immich user is created. Initializes interaction counts
   * and zero-vector embedding so scoring works from first upload.
   */
  registerUser(userId: string): void {
    this._registerUser(userId).catch(() => {});
  }

  private async _registerUser(userId: string): Promise<void> {
    try {
      await fetch(`${this.serviceUrl}/users/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: userId }),
      });
    } catch (error) {
      this.logger.error(`[aesthetic] registerUser failed for user ${userId}: ${error}`);
    }
  }

  /**
   * Fire-and-forget: POST /score-image to aesthetic-service.
   * Called after upload completes. Never blocks the upload response.
   */
  scoreImage(assetId: string, userId: string): void {
    this._scoreImage(assetId, userId).catch(() => {
      // already logged inside
    });
  }

  private async _scoreImage(assetId: string, userId: string): Promise<void> {
    try {
      await fetch(`${this.serviceUrl}/score-image`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ asset_id: assetId, user_id: userId }),
      });
    } catch (error) {
      this.logger.error(`[aesthetic] scoreImage failed for asset ${assetId}: ${error}`);
    }
  }

  /**
   * Fire-and-forget: POST /events/interaction to aesthetic-service.
   * Called after user actions (favorite, archive, delete, download, album_add, share).
   * Never blocks the action response.
   */
  recordInteraction(assetId: string, userId: string, eventType: string, label: number): void {
    this._recordInteraction(assetId, userId, eventType, label).catch(() => {
      // already logged inside
    });
  }

  private async _recordInteraction(
    assetId: string,
    userId: string,
    eventType: string,
    label: number,
  ): Promise<void> {
    try {
      const eventTime = new Date().toISOString();
      const eventId = createHash('sha256')
        .update(`${assetId}${userId}${eventType}${eventTime}`)
        .digest('hex');

      await fetch(`${this.serviceUrl}/events/interaction`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event_id: eventId,
          asset_id: assetId,
          user_id: userId,
          event_type: eventType,
          label,
          source: 'immich_upload',
          event_time: eventTime,
        }),
      });
    } catch (error) {
      this.logger.error(
        `[aesthetic] recordInteraction failed for asset ${assetId} event ${eventType}: ${error}`,
      );
    }
  }
}
