import { Column, ForeignKeyColumn, PrimaryColumn, Table, Timestamp, UpdateDateColumn } from '@immich/sql-tools';
import { Generated } from 'kysely';
import { ModelVersionsTable } from 'src/schema/tables/model-versions.table';
import { UserTable } from 'src/schema/tables/user.table';

@Table({ name: 'user_embeddings', synchronize: false })
export class UserEmbeddingsTable {
  @PrimaryColumn()
  @ForeignKeyColumn(() => UserTable, { onDelete: 'CASCADE' })
  userId!: string;

  // Embedding stored as double precision array - type defined in migration
  @Column()
  embedding!: number[];

  @Column({ nullable: true })
  @ForeignKeyColumn(() => ModelVersionsTable, { onDelete: 'SET NULL', nullable: true })
  modelVersion!: string | null;

  @UpdateDateColumn()
  updatedAt!: Generated<Timestamp>;
}
