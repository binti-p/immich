import { Column, CreateDateColumn, PrimaryColumn, Table, Timestamp } from '@immich/sql-tools';
import { Generated } from 'kysely';

@Table({ name: 'model_versions', synchronize: false })
export class ModelVersionsTable {
  @PrimaryColumn()
  versionId!: string;

  @Column()
  datasetVersion!: string;

  @Column()
  mlpObjectKey!: string;

  @Column()
  embeddingsObjectKey!: string;

  @Column({ type: 'boolean', default: false })
  isColdStart!: Generated<boolean>;

  @Column({ type: 'timestamp with time zone', nullable: true })
  activatedAt!: Timestamp | null;

  @Column({ type: 'timestamp with time zone', nullable: true })
  deactivatedAt!: Timestamp | null;

  @CreateDateColumn()
  createdAt!: Generated<Timestamp>;
}
