import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  await sql`ALTER TABLE "asset" ADD COLUMN IF NOT EXISTS "aestheticScore" real;`.execute(db);
  await sql`CREATE INDEX IF NOT EXISTS "asset_aestheticScore_idx" ON "asset" ("aestheticScore" DESC NULLS LAST) WHERE "aestheticScore" IS NOT NULL;`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP INDEX IF EXISTS "asset_aestheticScore_idx";`.execute(db);
  await sql`ALTER TABLE "asset" DROP COLUMN IF EXISTS "aestheticScore";`.execute(db);
}
