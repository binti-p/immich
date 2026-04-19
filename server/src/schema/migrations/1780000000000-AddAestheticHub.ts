import { Kysely, sql } from 'kysely';

/**
 * Migration to add aesthetic scoring tables.
 * Note: Foreign key constraints to users/assets tables are NOT added here
 * because those tables are created in the same migration transaction and
 * Kysely's migration system has issues with cross-migration FK references.
 * The tables will work correctly without FK constraints - just without
 * referential integrity enforcement at the database level.
 */
export async function up(db: Kysely<any>): Promise<void> {
  // Table 1: model_versions (no foreign keys)
  await sql`CREATE TABLE IF NOT EXISTS "model_versions" (
    "version_id" varchar NOT NULL PRIMARY KEY,
    "dataset_version" varchar NOT NULL,
    "mlp_object_key" varchar NOT NULL,
    "embeddings_object_key" varchar NOT NULL,
    "is_cold_start" boolean NOT NULL DEFAULT false,
    "activated_at" timestamptz,
    "deactivated_at" timestamptz,
    "created_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);
  
  // Partial unique index for active models
  await sql`CREATE UNIQUE INDEX IF NOT EXISTS "idx_model_versions_active" 
    ON "model_versions"("is_cold_start") 
    WHERE "activated_at" IS NOT NULL AND "deactivated_at" IS NULL;`.execute(db);

  // Table 2: user_embeddings
  await sql`CREATE TABLE IF NOT EXISTS "user_embeddings" (
    "user_id" uuid NOT NULL PRIMARY KEY,
    "embedding" double precision[] NOT NULL,
    "model_version" varchar REFERENCES "model_versions"("version_id"),
    "updated_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);

  // Table 3: user_interaction_counts
  await sql`CREATE TABLE IF NOT EXISTS "user_interaction_counts" (
    "user_id" uuid NOT NULL PRIMARY KEY,
    "interaction_count" integer NOT NULL DEFAULT 0,
    "updated_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);

  // Table 4: interaction_events
  await sql`CREATE TABLE IF NOT EXISTS "interaction_events" (
    "event_id" varchar NOT NULL PRIMARY KEY,
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "event_type" varchar NOT NULL,
    "label" double precision NOT NULL,
    "source" varchar NOT NULL CHECK ("source" = 'immich_upload'),
    "event_time" timestamptz NOT NULL,
    "ingested_at" timestamptz NOT NULL DEFAULT now(),
    "deleted_at" timestamptz
  );`.execute(db);
  
  // Indexes for interaction_events
  await sql`CREATE INDEX IF NOT EXISTS "idx_ie_user_time" 
    ON "interaction_events"("user_id", "event_time") 
    WHERE "deleted_at" IS NULL;`.execute(db);
  await sql`CREATE INDEX IF NOT EXISTS "idx_ie_asset" 
    ON "interaction_events"("asset_id") 
    WHERE "deleted_at" IS NULL;`.execute(db);

  // Table 5: inference_log
  await sql`CREATE TABLE IF NOT EXISTS "inference_log" (
    "request_id" varchar NOT NULL PRIMARY KEY,
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "model_version" varchar REFERENCES "model_versions"("version_id"),
    "is_cold_start" boolean NOT NULL,
    "alpha" double precision NOT NULL,
    "request_received_at" timestamptz NOT NULL,
    "computed_at" timestamptz NOT NULL
  );`.execute(db);

  // Table 6: aesthetic_scores
  await sql`CREATE TABLE IF NOT EXISTS "aesthetic_scores" (
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "score" double precision NOT NULL CHECK ("score" >= 0.0 AND "score" <= 1.0),
    "model_version" varchar REFERENCES "model_versions"("version_id"),
    "is_cold_start" boolean NOT NULL,
    "alpha" double precision NOT NULL,
    "inference_request_id" varchar REFERENCES "inference_log"("request_id"),
    "scored_at" timestamptz NOT NULL,
    PRIMARY KEY ("asset_id", "user_id")
  );`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE IF EXISTS "aesthetic_scores" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "inference_log" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "interaction_events" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "user_interaction_counts" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "user_embeddings" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "model_versions" CASCADE;`.execute(db);
}
