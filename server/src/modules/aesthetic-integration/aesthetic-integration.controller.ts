import { Body, Controller, HttpCode, HttpStatus, Post, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Endpoint, HistoryBuilder } from 'src/decorators';
import { AuthDto } from 'src/dtos/auth.dto';
import { ApiTag, Permission } from 'src/enum';
import { Auth, Authenticated } from 'src/middleware/auth.guard';
import { AestheticIntegrationService } from './aesthetic-integration.service';
import { RescoreAllDto, RescoreAllResponseDto, ScoreCallbackPayload } from './dto/aesthetic-score.dto';

@ApiTags(ApiTag.Jobs)
@Controller('aesthetic')
export class AestheticIntegrationController {
  constructor(private readonly aestheticIntegrationService: AestheticIntegrationService) {}

  @Post('score-callback')
  @HttpCode(HttpStatus.NO_CONTENT)
  @Endpoint({
    summary: 'Receive aesthetic score callback from scoring service',
    description: 'Called by the scoring service after scoring an asset. Updates asset.aestheticScore in the Immich database.',
    history: new HistoryBuilder().added('v1'),
  })
  async scoreCallback(@Body() payload: ScoreCallbackPayload): Promise<void> {
    await this.aestheticIntegrationService.receiveScoreCallback(payload);
  }

  @Post('admin/rescore-all')
  @Authenticated({ permission: Permission.AdminUserRead, admin: true })
  @HttpCode(HttpStatus.ACCEPTED)
  @Endpoint({
    summary: 'Trigger batch rescoring of assets',
    description:
      'Queue all assets for aesthetic rescoring. Optionally filter by userId. Returns a job ID for tracking the async operation.',
    history: new HistoryBuilder().added('v1'),
  })
  rescoreAll(@Auth() auth: AuthDto, @Query() dto: RescoreAllDto): Promise<RescoreAllResponseDto> {
    return this.aestheticIntegrationService.rescoreAll(dto.userId);
  }
}
