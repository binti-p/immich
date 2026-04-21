import { Kysely, sql } from 'kysely';

/**
 * Add status column to inference_log to track scoring attempts.
 * 
 * Status values:
 * - 'success': Scoring completed successfully
 * - 'failed_clip_missing': CLIP embedding not ready after retries
 * - 'failed_error': Other errors during scoring
 */
export async function up(db: Kysely<any>): Promise<void> {
  // Add status column with default 'success' for existing rows
  await sql`ALTER TABLE "inference_log"
    ADD COLUMN IF NOT EXISTS "status" character varying NOT NULL DEFAULT 'success';`.execute(db);

  // Add error_message column for debugging failed attempts
  await sql`ALTER TABLE "inference_log"
    ADD COLUMN IF NOT EXISTS "errorMessage" text;`.execute(db);

  // Add index for querying failed attempts
  await sql`CREATE INDEX IF NOT EXISTS "idx_inference_log_status"
    ON "inference_log"("status")
    WHERE "status" != 'success';`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP INDEX IF EXISTS "idx_inference_log_status";`.execute(db);
  await sql`ALTER TABLE "inference_log" DROP COLUMN IF EXISTS "errorMessage";`.execute(db);
  await sql`ALTER TABLE "inference_log" DROP COLUMN IF EXISTS "status";`.execute(db);
}
