import { Check, Column, ForeignKeyColumn, PrimaryColumn, Table, Timestamp } from '@immich/sql-tools';
import { AssetTable } from 'src/schema/tables/asset.table';
import { InferenceLogTable } from 'src/schema/tables/inference-log.table';
import { ModelVersionsTable } from 'src/schema/tables/model-versions.table';
import { UserTable } from 'src/schema/tables/user.table';

@Table('aesthetic_scores')
@Check({ expression: '"score" >= 0.0 AND "score" <= 1.0', name: 'CHK_aesthetic_scores_range' })
export class AestheticScoresTable {
  @PrimaryColumn()
  @ForeignKeyColumn(() => AssetTable, { onDelete: 'CASCADE' })
  assetId!: string;

  @PrimaryColumn()
  @ForeignKeyColumn(() => UserTable, { onDelete: 'CASCADE' })
  userId!: string;

  @Column({ type: 'double precision' })
  score!: number;

  @Column({ nullable: true })
  @ForeignKeyColumn(() => ModelVersionsTable, { onDelete: 'SET NULL', nullable: true })
  modelVersion!: string | null;

  @Column({ type: 'boolean' })
  isColdStart!: boolean;

  @Column({ type: 'double precision' })
  alpha!: number;

  @Column({ nullable: true })
  @ForeignKeyColumn(() => InferenceLogTable, { onDelete: 'SET NULL', nullable: true })
  inferenceRequestId!: string | null;

  @Column({ type: 'timestamp with time zone' })
  scoredAt!: Timestamp;
}
