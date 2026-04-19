import { Module } from '@nestjs/common';
import { AssetRepository } from 'src/repositories/asset.repository';
import { ConfigRepository } from 'src/repositories/config.repository';
import { TelemetryRepository } from 'src/repositories/telemetry.repository';
import { AestheticIntegrationController } from './aesthetic-integration.controller';
import { AestheticIntegrationService } from './aesthetic-integration.service';
import { DataPipelineRepository } from './data-pipeline.repository';
import { WebhookService } from './webhook.service';

@Module({
  controllers: [AestheticIntegrationController],
  providers: [
    AestheticIntegrationService,
    DataPipelineRepository,
    WebhookService,
    ConfigRepository,
    AssetRepository,
    TelemetryRepository,
  ],
  exports: [AestheticIntegrationService],
})
export class AestheticIntegrationModule {}
