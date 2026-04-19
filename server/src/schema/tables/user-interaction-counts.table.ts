import { Column, ForeignKeyColumn, PrimaryColumn, Table, Timestamp, UpdateDateColumn } from '@immich/sql-tools';
import { Generated } from 'kysely';
import { UserTable } from 'src/schema/tables/user.table';

@Table('user_interaction_counts')
export class UserInteractionCountsTable {
  @PrimaryColumn()
  @ForeignKeyColumn(() => UserTable, { onDelete: 'CASCADE' })
  userId!: string;

  @Column({ type: 'integer', default: 0 })
  interactionCount!: Generated<number>;

  @UpdateDateColumn()
  updatedAt!: Generated<Timestamp>;
}
