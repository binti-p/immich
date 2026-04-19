import { Column, ForeignKeyColumn, PrimaryColumn, Table, Timestamp } from '@immich/sql-tools';
import { AssetTable } from 'src/schema/tables/asset.table';
import { ModelVersionsTable } from 'src/schema/tables/model-versions.table';
import { UserTable } from 'src/schema/tables/user.table';

@Table('inference_log')
export class InferenceLogTable {
  @PrimaryColumn()
  requestId!: string;

  @Column()
  @ForeignKeyColumn(() => AssetTable, { onDelete: 'CASCADE' })
  assetId!: string;

  @Column()
  @ForeignKeyColumn(() => UserTable, { onDelete: 'CASCADE' })
  userId!: string;

  @Column({ nullable: true })
  @ForeignKeyColumn(() => ModelVersionsTable, { onDelete: 'SET NULL', nullable: true })
  modelVersion!: string | null;

  @Column({ type: 'boolean' })
  isColdStart!: boolean;

  @Column({ type: 'double precision' })
  alpha!: number;

  @Column({ type: 'timestamp with time zone' })
  requestReceivedAt!: Timestamp;

  @Column({ type: 'timestamp with time zone' })
  computedAt!: Timestamp;
}
