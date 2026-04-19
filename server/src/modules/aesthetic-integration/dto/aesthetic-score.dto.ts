export interface AestheticScoreDto {
  assetId: string;
  userId: string;
  score: number;
  globalScore: number;
  personalizedScore: number | null;
  alpha: number;
  modelVersion: string;
  scoredAt: Date;
}

export interface UploadWebhookPayload {
  asset_id: string;
  user_id: string;
  storage_path: string;
  uploaded_at: string; // ISO 8601
}

export interface RescoreAllDto {
  userId?: string;
}

export interface RescoreAllResponseDto {
  jobId: string;
}
