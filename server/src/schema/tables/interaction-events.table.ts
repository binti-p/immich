import { Check, Column, CreateDateColumn, ForeignKeyColumn, PrimaryColumn, Table, Timestamp } from '@immich/sql-tools';
import { Generated } from 'kysely';
import { AssetTable } from 'src/schema/tables/asset.table';
import { UserTable } from 'src/schema/tables/user.table';

@Table({ name: 'interaction_events', synchronize: false })
@Check({ expression: `"source" = 'immich_upload'`, name: 'interaction_events_source_check' })
export class InteractionEventsTable {
  @PrimaryColumn()
  eventId!: string;

  @Column()
  @ForeignKeyColumn(() => AssetTable, { onDelete: 'CASCADE' })
  assetId!: string;

  @Column()
  @ForeignKeyColumn(() => UserTable, { onDelete: 'CASCADE' })
  userId!: string;

  @Column()
  eventType!: string;

  @Column({ type: 'double precision' })
  label!: number;

  @Column()
  source!: string;

  @Column({ type: 'timestamp with time zone' })
  eventTime!: Timestamp;

  @CreateDateColumn()
  ingestedAt!: Generated<Timestamp>;

  @Column({ type: 'timestamp with time zone', nullable: true })
  deletedAt!: Timestamp | null;
}
