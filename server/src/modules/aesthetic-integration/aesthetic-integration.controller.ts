import { Controller, HttpCode, HttpStatus, Post, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Endpoint, HistoryBuilder } from 'src/decorators';
import { AuthDto } from 'src/dtos/auth.dto';
import { ApiTag, Permission } from 'src/enum';
import { Auth, Authenticated } from 'src/middleware/auth.guard';
import { AestheticIntegrationService } from './aesthetic-integration.service';
import { RescoreAllDto, RescoreAllResponseDto } from './dto/aesthetic-score.dto';

@ApiTags(ApiTag.Jobs)
@Controller('admin')
export class AestheticIntegrationController {
  constructor(private readonly aestheticIntegrationService: AestheticIntegrationService) {}

  @Post('rescore-all')
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
