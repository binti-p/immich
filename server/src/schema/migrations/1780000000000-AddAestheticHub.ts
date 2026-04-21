import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  // Table 1: model_versions (no foreign keys)
  await sql`CREATE TABLE IF NOT EXISTS "model_versions" (
    "versionId" character varying NOT NULL,
    "datasetVersion" character varying NOT NULL,
    "mlpObjectKey" character varying NOT NULL,
    "embeddingsObjectKey" character varying NOT NULL,
    "activatedAt" timestamp with time zone,
    "deactivatedAt" timestamp with time zone,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT "model_versions_pkey" PRIMARY KEY ("versionId")
  );`.execute(db);

  // Unique index for active models (only one active at a time)
  await sql`CREATE UNIQUE INDEX IF NOT EXISTS "idx_model_versions_active"
    ON "model_versions"("versionId")
    WHERE "activatedAt" IS NOT NULL AND "deactivatedAt" IS NULL;`.execute(db);

  // Table 2: user_embeddings
  await sql`CREATE TABLE IF NOT EXISTS "user_embeddings" (
    "userId" uuid NOT NULL,
    "embedding" double precision[] NOT NULL,
    "modelVersion" character varying,
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT "user_embeddings_pkey" PRIMARY KEY ("userId"),
    CONSTRAINT "user_embeddings_userId_fkey" FOREIGN KEY ("userId") REFERENCES "user" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "user_embeddings_modelVersion_fkey" FOREIGN KEY ("modelVersion") REFERENCES "model_versions" ("versionId") ON UPDATE CASCADE ON DELETE SET NULL
  );`.execute(db);

  // Table 3: user_interaction_counts
  await sql`CREATE TABLE IF NOT EXISTS "user_interaction_counts" (
    "userId" uuid NOT NULL,
    "interactionCount" integer NOT NULL DEFAULT 0,
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT "user_interaction_counts_pkey" PRIMARY KEY ("userId"),
    CONSTRAINT "user_interaction_counts_userId_fkey" FOREIGN KEY ("userId") REFERENCES "user" ("id") ON UPDATE CASCADE ON DELETE CASCADE
  );`.execute(db);

  // Table 4: interaction_events
  await sql`CREATE TABLE IF NOT EXISTS "interaction_events" (
    "eventId" character varying NOT NULL,
    "assetId" uuid NOT NULL,
    "userId" uuid NOT NULL,
    "eventType" character varying NOT NULL,
    "label" double precision NOT NULL,
    "source" character varying NOT NULL,
    "eventTime" timestamp with time zone NOT NULL,
    "ingestedAt" timestamp with time zone NOT NULL DEFAULT now(),
    "deletedAt" timestamp with time zone,
    CONSTRAINT "interaction_events_pkey" PRIMARY KEY ("eventId"),
    CONSTRAINT "interaction_events_source_check" CHECK ("source" = 'immich_upload'),
    CONSTRAINT "interaction_events_assetId_fkey" FOREIGN KEY ("assetId") REFERENCES "asset" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "interaction_events_userId_fkey" FOREIGN KEY ("userId") REFERENCES "user" ("id") ON UPDATE CASCADE ON DELETE CASCADE
  );`.execute(db);

  await sql`CREATE INDEX IF NOT EXISTS "idx_ie_user_time"
    ON "interaction_events"("userId", "eventTime")
    WHERE "deletedAt" IS NULL;`.execute(db);
  await sql`CREATE INDEX IF NOT EXISTS "idx_ie_asset"
    ON "interaction_events"("assetId")
    WHERE "deletedAt" IS NULL;`.execute(db);

  // Table 5: inference_log
  await sql`CREATE TABLE IF NOT EXISTS "inference_log" (
    "requestId" character varying NOT NULL,
    "assetId" uuid NOT NULL,
    "userId" uuid NOT NULL,
    "modelVersion" character varying,
    "isColdStart" boolean NOT NULL,
    "alpha" double precision NOT NULL,
    "requestReceivedAt" timestamp with time zone NOT NULL,
    "computedAt" timestamp with time zone NOT NULL,
    CONSTRAINT "inference_log_pkey" PRIMARY KEY ("requestId"),
    CONSTRAINT "inference_log_assetId_fkey" FOREIGN KEY ("assetId") REFERENCES "asset" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "inference_log_userId_fkey" FOREIGN KEY ("userId") REFERENCES "user" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "inference_log_modelVersion_fkey" FOREIGN KEY ("modelVersion") REFERENCES "model_versions" ("versionId") ON UPDATE CASCADE ON DELETE SET NULL
  );`.execute(db);

  // Table 6: aesthetic_scores
  await sql`CREATE TABLE IF NOT EXISTS "aesthetic_scores" (
    "assetId" uuid NOT NULL,
    "userId" uuid NOT NULL,
    "score" double precision NOT NULL,
    "modelVersion" character varying,
    "isColdStart" boolean NOT NULL,
    "alpha" double precision NOT NULL,
    "inferenceRequestId" character varying,
    "scoredAt" timestamp with time zone NOT NULL,
    CONSTRAINT "aesthetic_scores_pkey" PRIMARY KEY ("assetId", "userId"),
    CONSTRAINT "aesthetic_scores_score_check" CHECK ("score" >= 0.0 AND "score" <= 1.0),
    CONSTRAINT "aesthetic_scores_assetId_fkey" FOREIGN KEY ("assetId") REFERENCES "asset" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "aesthetic_scores_userId_fkey" FOREIGN KEY ("userId") REFERENCES "user" ("id") ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT "aesthetic_scores_modelVersion_fkey" FOREIGN KEY ("modelVersion") REFERENCES "model_versions" ("versionId") ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT "aesthetic_scores_inferenceRequestId_fkey" FOREIGN KEY ("inferenceRequestId") REFERENCES "inference_log" ("requestId") ON UPDATE CASCADE ON DELETE SET NULL
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
