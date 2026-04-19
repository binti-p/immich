import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  // Table 1: model_versions
  await sql`CREATE TABLE "model_versions" (
    "version_id" varchar NOT NULL,
    "dataset_version" varchar NOT NULL,
    "mlp_object_key" varchar NOT NULL,
    "embeddings_object_key" varchar NOT NULL,
    "is_cold_start" boolean NOT NULL DEFAULT false,
    "activated_at" timestamptz,
    "deactivated_at" timestamptz,
    "created_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);
  
  await sql`ALTER TABLE "model_versions" ADD CONSTRAINT "PK_model_versions" PRIMARY KEY ("version_id");`.execute(db);
  
  // Partial unique index for active models
  await sql`CREATE UNIQUE INDEX "idx_model_versions_active" 
    ON "model_versions"("is_cold_start") 
    WHERE "activated_at" IS NOT NULL AND "deactivated_at" IS NULL;`.execute(db);

  // Table 2: user_embeddings
  await sql`CREATE TABLE "user_embeddings" (
    "user_id" uuid NOT NULL,
    "embedding" float[] NOT NULL,
    "model_version" varchar,
    "updated_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);
  
  await sql`ALTER TABLE "user_embeddings" ADD CONSTRAINT "PK_user_embeddings" PRIMARY KEY ("user_id");`.execute(db);
  await sql`ALTER TABLE "user_embeddings" ADD CONSTRAINT "FK_user_embeddings_user" 
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "user_embeddings" ADD CONSTRAINT "FK_user_embeddings_model" 
    FOREIGN KEY ("model_version") REFERENCES "model_versions"("version_id");`.execute(db);

  // Table 3: user_interaction_counts
  await sql`CREATE TABLE "user_interaction_counts" (
    "user_id" uuid NOT NULL,
    "interaction_count" integer NOT NULL DEFAULT 0,
    "updated_at" timestamptz NOT NULL DEFAULT now()
  );`.execute(db);
  
  await sql`ALTER TABLE "user_interaction_counts" ADD CONSTRAINT "PK_user_interaction_counts" PRIMARY KEY ("user_id");`.execute(db);
  await sql`ALTER TABLE "user_interaction_counts" ADD CONSTRAINT "FK_user_interaction_counts_user" 
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;`.execute(db);

  // Table 4: interaction_events
  await sql`CREATE TABLE "interaction_events" (
    "event_id" varchar NOT NULL,
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "event_type" varchar NOT NULL,
    "label" float NOT NULL,
    "source" varchar NOT NULL,
    "event_time" timestamptz NOT NULL,
    "ingested_at" timestamptz NOT NULL DEFAULT now(),
    "deleted_at" timestamptz
  );`.execute(db);
  
  await sql`ALTER TABLE "interaction_events" ADD CONSTRAINT "PK_interaction_events" PRIMARY KEY ("event_id");`.execute(db);
  await sql`ALTER TABLE "interaction_events" ADD CONSTRAINT "FK_interaction_events_asset" 
    FOREIGN KEY ("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "interaction_events" ADD CONSTRAINT "FK_interaction_events_user" 
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "interaction_events" ADD CONSTRAINT "CHK_interaction_events_source" 
    CHECK ("source" = 'immich_upload');`.execute(db);
  
  // Indexes for interaction_events
  await sql`CREATE INDEX "idx_ie_user_time" 
    ON "interaction_events"("user_id", "event_time") 
    WHERE "deleted_at" IS NULL;`.execute(db);
  await sql`CREATE INDEX "idx_ie_asset" 
    ON "interaction_events"("asset_id") 
    WHERE "deleted_at" IS NULL;`.execute(db);

  // Table 5: inference_log
  await sql`CREATE TABLE "inference_log" (
    "request_id" varchar NOT NULL,
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "model_version" varchar,
    "is_cold_start" boolean NOT NULL,
    "alpha" float NOT NULL,
    "request_received_at" timestamptz NOT NULL,
    "computed_at" timestamptz NOT NULL
  );`.execute(db);
  
  await sql`ALTER TABLE "inference_log" ADD CONSTRAINT "PK_inference_log" PRIMARY KEY ("request_id");`.execute(db);
  await sql`ALTER TABLE "inference_log" ADD CONSTRAINT "FK_inference_log_asset" 
    FOREIGN KEY ("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "inference_log" ADD CONSTRAINT "FK_inference_log_user" 
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "inference_log" ADD CONSTRAINT "FK_inference_log_model" 
    FOREIGN KEY ("model_version") REFERENCES "model_versions"("version_id");`.execute(db);

  // Table 6: aesthetic_scores
  await sql`CREATE TABLE "aesthetic_scores" (
    "asset_id" uuid NOT NULL,
    "user_id" uuid NOT NULL,
    "score" float NOT NULL,
    "model_version" varchar,
    "is_cold_start" boolean NOT NULL,
    "alpha" float NOT NULL,
    "inference_request_id" varchar,
    "scored_at" timestamptz NOT NULL
  );`.execute(db);
  
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "PK_aesthetic_scores" PRIMARY KEY ("asset_id", "user_id");`.execute(db);
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "FK_aesthetic_scores_asset" 
    FOREIGN KEY ("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "FK_aesthetic_scores_user" 
    FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE;`.execute(db);
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "FK_aesthetic_scores_model" 
    FOREIGN KEY ("model_version") REFERENCES "model_versions"("version_id");`.execute(db);
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "FK_aesthetic_scores_inference" 
    FOREIGN KEY ("inference_request_id") REFERENCES "inference_log"("request_id");`.execute(db);
  await sql`ALTER TABLE "aesthetic_scores" ADD CONSTRAINT "CHK_aesthetic_scores_range" 
    CHECK ("score" >= 0.0 AND "score" <= 1.0);`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  // Drop tables in reverse order
  await sql`DROP TABLE IF EXISTS "aesthetic_scores" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "inference_log" CASCADE;`.execute(db);
  await sql`DROP INDEX IF EXISTS "idx_ie_asset";`.execute(db);
  await sql`DROP INDEX IF EXISTS "idx_ie_user_time";`.execute(db);
  await sql`DROP TABLE IF EXISTS "interaction_events" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "user_interaction_counts" CASCADE;`.execute(db);
  await sql`DROP TABLE IF EXISTS "user_embeddings" CASCADE;`.execute(db);
  await sql`DROP INDEX IF EXISTS "idx_model_versions_active";`.execute(db);
  await sql`DROP TABLE IF EXISTS "model_versions" CASCADE;`.execute(db);
}
