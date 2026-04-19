import { BadRequestException, Injectable } from '@nestjs/common';
import _ from 'lodash';
import { DateTime, Duration } from 'luxon';
import { JOBS_ASSET_PAGINATION_SIZE } from 'src/constants';
import { AssetFile } from 'src/database';
import { OnJob } from 'src/decorators';
import { AssetResponseDto, SanitizedAssetResponseDto, mapAsset } from 'src/dtos/asset-response.dto';
import {
  AssetBulkDeleteDto,
  AssetBulkUpdateDto,
  AssetCopyDto,
  AssetJobName,
  AssetJobsDto,
  AssetMetadataBulkDeleteDto,
  AssetMetadataBulkResponseDto,
  AssetMetadataBulkUpsertDto,
  AssetMetadataResponseDto,
  AssetMetadataUpsertDto,
  AssetStatsDto,
  UpdateAssetDto,
  mapStats,
} from 'src/dtos/asset.dto';
import { AuthDto } from 'src/dtos/auth.dto';
import { AssetEditAction, AssetEditActionItem, AssetEditsCreateDto, AssetEditsResponseDto } from 'src/dtos/editing.dto';
import { AssetOcrResponseDto } from 'src/dtos/ocr.dto';
import {
  AssetFileType,
  AssetStatus,
  AssetType,
  AssetVisibility,
  JobName,
  JobStatus,
  Permission,
  QueueName,
} from 'src/enum';
import { AestheticIntegrationService } from 'src/modules/aesthetic-integration/aesthetic-integration.service';
import { AccessRepository } from 'src/repositories/access.repository';
import { ActivityRepository } from 'src/repositories/activity.repository';
import { AlbumUserRepository } from 'src/repositories/album-user.repository';
import { AlbumRepository } from 'src/repositories/album.repository';
import { ApiKeyRepository } from 'src/repositories/api-key.repository';
import { AppRepository } from 'src/repositories/app.repository';
import { AssetEditRepository } from 'src/repositories/asset-edit.repository';
import { AssetJobRepository } from 'src/repositories/asset-job.repository';
import { AssetRepository } from 'src/repositories/asset.repository';
import { ConfigRepository } from 'src/repositories/config.repository';
import { CronRepository } from 'src/repositories/cron.repository';
import { CryptoRepository } from 'src/repositories/crypto.repository';
import { DatabaseRepository } from 'src/repositories/database.repository';
import { DownloadRepository } from 'src/repositories/download.repository';
import { DuplicateRepository } from 'src/repositories/duplicate.repository';
import { EmailRepository } from 'src/repositories/email.repository';
import { EventRepository } from 'src/repositories/event.repository';
import { JobRepository } from 'src/repositories/job.repository';
import { LibraryRepository } from 'src/repositories/library.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { MachineLearningRepository } from 'src/repositories/machine-learning.repository';
import { MapRepository } from 'src/repositories/map.repository';
import { MediaRepository } from 'src/repositories/media.repository';
import { MemoryRepository } from 'src/repositories/memory.repository';
import { MetadataRepository } from 'src/repositories/metadata.repository';
import { MoveRepository } from 'src/repositories/move.repository';
import { NotificationRepository } from 'src/repositories/notification.repository';
import { OAuthRepository } from 'src/repositories/oauth.repository';
import { OcrRepository } from 'src/repositories/ocr.repository';
import { PartnerRepository } from 'src/repositories/partner.repository';
import { PersonRepository } from 'src/repositories/person.repository';
import { PluginRepository } from 'src/repositories/plugin.repository';
import { ProcessRepository } from 'src/repositories/process.repository';
import { SearchRepository } from 'src/repositories/search.repository';
import { ServerInfoRepository } from 'src/repositories/server-info.repository';
import { SessionRepository } from 'src/repositories/session.repository';
import { SharedLinkAssetRepository } from 'src/repositories/shared-link-asset.repository';
import { SharedLinkRepository } from 'src/repositories/shared-link.repository';
import { StackRepository } from 'src/repositories/stack.repository';
import { StorageRepository } from 'src/repositories/storage.repository';
import { SyncCheckpointRepository } from 'src/repositories/sync-checkpoint.repository';
import { SyncRepository } from 'src/repositories/sync.repository';
import { SystemMetadataRepository } from 'src/repositories/system-metadata.repository';
import { TagRepository } from 'src/repositories/tag.repository';
import { TelemetryRepository } from 'src/repositories/telemetry.repository';
import { TrashRepository } from 'src/repositories/trash.repository';
import { UserRepository } from 'src/repositories/user.repository';
import { VersionHistoryRepository } from 'src/repositories/version-history.repository';
import { ViewRepository } from 'src/repositories/view-repository';
import { WebsocketRepository } from 'src/repositories/websocket.repository';
import { WorkflowRepository } from 'src/repositories/workflow.repository';
import { BaseService } from 'src/services/base.service';
import { JobItem, JobOf } from 'src/types';
import { requireElevatedPermission } from 'src/utils/access';
import {
  getAssetFiles,
  getDimensions,
  isPanorama,
  onAfterUnlink,
  onBeforeLink,
  onBeforeUnlink,
  sortByAestheticScore,
} from 'src/utils/asset.util';
import { updateLockedColumns } from 'src/utils/database';
import { extractTimeZone } from 'src/utils/date';
import { transformOcrBoundingBox } from 'src/utils/transform';

@Injectable()
export class AssetService extends BaseService {
  constructor(
    logger: LoggingRepository,
    accessRepository: AccessRepository,
    activityRepository: ActivityRepository,
    albumRepository: AlbumRepository,
    albumUserRepository: AlbumUserRepository,
    apiKeyRepository: ApiKeyRepository,
    appRepository: AppRepository,
    assetRepository: AssetRepository,
    assetEditRepository: AssetEditRepository,
    assetJobRepository: AssetJobRepository,
    configRepository: ConfigRepository,
    cronRepository: CronRepository,
    cryptoRepository: CryptoRepository,
    databaseRepository: DatabaseRepository,
    downloadRepository: DownloadRepository,
    duplicateRepository: DuplicateRepository,
    emailRepository: EmailRepository,
    eventRepository: EventRepository,
    jobRepository: JobRepository,
    libraryRepository: LibraryRepository,
    machineLearningRepository: MachineLearningRepository,
    mapRepository: MapRepository,
    mediaRepository: MediaRepository,
    memoryRepository: MemoryRepository,
    metadataRepository: MetadataRepository,
    moveRepository: MoveRepository,
    notificationRepository: NotificationRepository,
    oauthRepository: OAuthRepository,
    ocrRepository: OcrRepository,
    partnerRepository: PartnerRepository,
    personRepository: PersonRepository,
    pluginRepository: PluginRepository,
    processRepository: ProcessRepository,
    searchRepository: SearchRepository,
    serverInfoRepository: ServerInfoRepository,
    sessionRepository: SessionRepository,
    sharedLinkRepository: SharedLinkRepository,
    sharedLinkAssetRepository: SharedLinkAssetRepository,
    stackRepository: StackRepository,
    storageRepository: StorageRepository,
    syncRepository: SyncRepository,
    syncCheckpointRepository: SyncCheckpointRepository,
    systemMetadataRepository: SystemMetadataRepository,
    tagRepository: TagRepository,
    telemetryRepository: TelemetryRepository,
    trashRepository: TrashRepository,
    userRepository: UserRepository,
    versionRepository: VersionHistoryRepository,
    viewRepository: ViewRepository,
    websocketRepository: WebsocketRepository,
    workflowRepository: WorkflowRepository,
    private readonly aestheticIntegrationService: AestheticIntegrationService,
  ) {
    super(
      logger,
      accessRepository,
      activityRepository,
      albumRepository,
      albumUserRepository,
      apiKeyRepository,
      appRepository,
      assetRepository,
      assetEditRepository,
      assetJobRepository,
      configRepository,
      cronRepository,
      cryptoRepository,
      databaseRepository,
      downloadRepository,
      duplicateRepository,
      emailRepository,
      eventRepository,
      jobRepository,
      libraryRepository,
      machineLearningRepository,
      mapRepository,
      mediaRepository,
      memoryRepository,
      metadataRepository,
      moveRepository,
      notificationRepository,
      oauthRepository,
      ocrRepository,
      partnerRepository,
      personRepository,
      pluginRepository,
      processRepository,
      searchRepository,
      serverInfoRepository,
      sessionRepository,
      sharedLinkRepository,
      sharedLinkAssetRepository,
      stackRepository,
      storageRepository,
      syncRepository,
      syncCheckpointRepository,
      systemMetadataRepository,
      tagRepository,
      telemetryRepository,
      trashRepository,
      userRepository,
      versionRepository,
      viewRepository,
      websocketRepository,
      workflowRepository,
    );
  }
  async getStatistics(auth: AuthDto, dto: AssetStatsDto) {
    if (dto.visibility === AssetVisibility.Locked) {
      requireElevatedPermission(auth);
    }

    const stats = await this.assetRepository.getStatistics(auth.user.id, dto);
    return mapStats(stats);
  }

  async get(auth: AuthDto, id: string): Promise<AssetResponseDto | SanitizedAssetResponseDto> {
    await this.requireAccess({ auth, permission: Permission.AssetRead, ids: [id] });

    const asset = await this.assetRepository.getById(id, {
      exifInfo: true,
      owner: true,
      faces: { person: true },
      stack: { assets: true },
      edits: true,
      tags: true,
    });

    if (!asset) {
      throw new BadRequestException('Asset not found');
    }

    if (auth.sharedLink && !auth.sharedLink.showExif) {
      return mapAsset(asset, { stripMetadata: true, withStack: true, auth });
    }

    const data = mapAsset(asset, { withStack: true, auth });

    if (auth.sharedLink) {
      delete data.owner;
    }

    if (data.ownerId !== auth.user.id || auth.sharedLink) {
      data.people = [];
    }

    // Query aesthetic score for this asset
    const scoresMap = await this.getAestheticScoresForAssets([id]);
    data.aestheticScore = scoresMap.get(id) ?? null;

    return data;
  }

  async update(auth: AuthDto, id: string, dto: UpdateAssetDto): Promise<AssetResponseDto> {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: [id] });

    const { description, dateTimeOriginal, latitude, longitude, rating, ...rest } = dto;
    const repos = { asset: this.assetRepository, event: this.eventRepository };

    let previousMotion: { id: string } | null = null;
    if (rest.livePhotoVideoId) {
      await onBeforeLink(repos, { userId: auth.user.id, livePhotoVideoId: rest.livePhotoVideoId });
    } else if (rest.livePhotoVideoId === null) {
      const asset = await this.findOrFail(id);
      if (asset.livePhotoVideoId) {
        previousMotion = await onBeforeUnlink(repos, { livePhotoVideoId: asset.livePhotoVideoId });
      }
    }

    await this.updateExif({ id, description, dateTimeOriginal, latitude, longitude, rating });

    const asset = await this.assetRepository.update({ id, ...rest });

    if (previousMotion && asset) {
      await onAfterUnlink(repos, {
        userId: auth.user.id,
        livePhotoVideoId: previousMotion.id,
        visibility: asset.visibility,
      });
    }

    if (!asset) {
      throw new BadRequestException('Asset not found');
    }

    return mapAsset(asset, { auth });
  }

  async updateAll(auth: AuthDto, dto: AssetBulkUpdateDto): Promise<void> {
    const {
      ids,
      isFavorite,
      visibility,
      dateTimeOriginal,
      latitude,
      longitude,
      rating,
      description,
      duplicateId,
      dateTimeRelative,
      timeZone,
    } = dto;
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids });

    const assetDto = _.omitBy({ isFavorite, visibility, duplicateId }, _.isUndefined);
    const exifDto = _.omitBy(
      {
        latitude,
        longitude,
        rating,
        description,
        dateTimeOriginal,
      },
      _.isUndefined,
    );

    if (Object.keys(exifDto).length > 0) {
      await this.assetRepository.updateAllExif(ids, exifDto);
    }

    const extractedTimeZone = extractTimeZone(dateTimeOriginal);

    if (
      (dateTimeRelative !== undefined && dateTimeRelative !== 0) ||
      timeZone !== undefined ||
      extractedTimeZone?.type === 'fixed'
    ) {
      await this.assetRepository.updateDateTimeOriginal(ids, dateTimeRelative, timeZone ?? extractedTimeZone?.name);
    }

    if (Object.keys(assetDto).length > 0) {
      await this.assetRepository.updateAll(ids, assetDto);
    }

    if (visibility === AssetVisibility.Locked) {
      await this.albumRepository.removeAssetsFromAll(ids);
    }

    await this.jobRepository.queueAll(ids.map((id) => ({ name: JobName.SidecarWrite, data: { id } })));
  }

  async copy(
    auth: AuthDto,
    {
      sourceId,
      targetId,
      albums = true,
      sidecar = true,
      sharedLinks = true,
      stack = true,
      favorite = true,
    }: AssetCopyDto,
  ) {
    await this.requireAccess({ auth, permission: Permission.AssetCopy, ids: [sourceId, targetId] });
    const sourceAsset = await this.assetRepository.getForCopy(sourceId);
    const targetAsset = await this.assetRepository.getForCopy(targetId);

    if (!sourceAsset || !targetAsset) {
      throw new BadRequestException('Both assets must exist');
    }

    if (sourceId === targetId) {
      throw new BadRequestException('Source and target id must be distinct');
    }

    if (albums) {
      await this.albumRepository.copyAlbums({ sourceAssetId: sourceId, targetAssetId: targetId });
    }

    if (sharedLinks) {
      await this.sharedLinkAssetRepository.copySharedLinks({ sourceAssetId: sourceId, targetAssetId: targetId });
    }

    if (stack) {
      await this.copyStack({ sourceAsset, targetAsset });
    }

    if (favorite) {
      await this.assetRepository.update({ id: targetId, isFavorite: sourceAsset.isFavorite });
    }

    if (sidecar) {
      await this.copySidecar({ sourceAsset, targetAsset });
    }
  }

  private async copyStack({
    sourceAsset,
    targetAsset,
  }: {
    sourceAsset: { id: string; stackId: string | null };
    targetAsset: { id: string; stackId: string | null };
  }) {
    if (!sourceAsset.stackId) {
      return;
    }

    if (targetAsset.stackId) {
      await this.stackRepository.merge({ sourceId: sourceAsset.stackId, targetId: targetAsset.stackId });
      await this.stackRepository.delete(sourceAsset.stackId);
    } else {
      await this.assetRepository.update({ id: targetAsset.id, stackId: sourceAsset.stackId });
    }
  }

  private async copySidecar({
    sourceAsset,
    targetAsset,
  }: {
    sourceAsset: { files: AssetFile[] };
    targetAsset: { id: string; files: AssetFile[]; originalPath: string };
  }) {
    const { sidecarFile: sourceFile } = getAssetFiles(sourceAsset.files);
    if (!sourceFile?.path) {
      return;
    }

    const { sidecarFile: targetFile } = getAssetFiles(targetAsset.files ?? []);
    if (targetFile?.path) {
      await this.storageRepository.unlink(targetFile.path);
    }

    await this.storageRepository.copyFile(sourceFile.path, `${targetAsset.originalPath}.xmp`);
    await this.assetRepository.upsertFile({
      assetId: targetAsset.id,
      path: `${targetAsset.originalPath}.xmp`,
      type: AssetFileType.Sidecar,
    });
    await this.jobRepository.queue({ name: JobName.AssetExtractMetadata, data: { id: targetAsset.id } });
  }

  @OnJob({ name: JobName.AssetDeleteCheck, queue: QueueName.BackgroundTask })
  async handleAssetDeletionCheck(): Promise<JobStatus> {
    const config = await this.getConfig({ withCache: false });
    const trashedDays = config.trash.enabled ? config.trash.days : 0;
    const trashedBefore = DateTime.now()
      .minus(Duration.fromObject({ days: trashedDays }))
      .toJSDate();

    let chunk: Array<{ id: string; isOffline: boolean }> = [];
    const queueChunk = async () => {
      if (chunk.length > 0) {
        await this.jobRepository.queueAll(
          chunk.map(({ id, isOffline }) => ({
            name: JobName.AssetDelete,
            data: { id, deleteOnDisk: !isOffline },
          })),
        );
        chunk = [];
      }
    };

    const assets = this.assetJobRepository.streamForDeletedJob(trashedBefore);
    for await (const asset of assets) {
      chunk.push(asset);
      if (chunk.length >= JOBS_ASSET_PAGINATION_SIZE) {
        await queueChunk();
      }
    }

    await queueChunk();

    return JobStatus.Success;
  }

  @OnJob({ name: JobName.AssetDelete, queue: QueueName.BackgroundTask })
  async handleAssetDeletion(job: JobOf<JobName.AssetDelete>): Promise<JobStatus> {
    const { id, deleteOnDisk } = job;

    const asset = await this.assetJobRepository.getForAssetDeletion(id);

    if (!asset) {
      return JobStatus.Failed;
    }

    // replace the parent of the stack children with a new asset
    if (asset.stack?.primaryAssetId === id) {
      // this only includes timeline visible assets and excludes the primary asset
      const stackAssetIds = asset.stack.assets.map((a) => a.id);
      if (stackAssetIds.length >= 2) {
        const newPrimaryAssetId = stackAssetIds.find((a) => a !== id)!;
        await this.stackRepository.update(asset.stack.id, {
          id: asset.stack.id,
          primaryAssetId: newPrimaryAssetId,
        });
      } else {
        await this.stackRepository.delete(asset.stack.id);
      }
    }

    await this.assetRepository.remove(asset);
    if (!asset.libraryId) {
      await this.userRepository.updateUsage(asset.ownerId, -(asset.exifInfo?.fileSizeInByte || 0));
    }

    await this.eventRepository.emit('AssetDelete', { assetId: id, userId: asset.ownerId });

    // delete the motion if it is not used by another asset
    if (asset.livePhotoVideoId) {
      const count = await this.assetRepository.getLivePhotoCount(asset.livePhotoVideoId);
      if (count === 0) {
        await this.jobRepository.queue({
          name: JobName.AssetDelete,
          data: { id: asset.livePhotoVideoId, deleteOnDisk },
        });
      }
    }

    const assetFiles = getAssetFiles(asset.files ?? []);
    const files = [
      assetFiles.thumbnailFile?.path,
      assetFiles.previewFile?.path,
      assetFiles.fullsizeFile?.path,
      assetFiles.editedFullsizeFile?.path,
      assetFiles.editedPreviewFile?.path,
      assetFiles.editedThumbnailFile?.path,
      assetFiles.encodedVideoFile?.path,
    ];

    if (deleteOnDisk && !asset.isOffline) {
      files.push(assetFiles.sidecarFile?.path, asset.originalPath);
    }

    await this.jobRepository.queue({ name: JobName.FileDelete, data: { files: files.filter(Boolean) } });

    return JobStatus.Success;
  }

  async deleteAll(auth: AuthDto, dto: AssetBulkDeleteDto): Promise<void> {
    const { ids, force } = dto;

    await this.requireAccess({ auth, permission: Permission.AssetDelete, ids });
    await this.assetRepository.updateAll(ids, {
      deletedAt: new Date(),
      status: force ? AssetStatus.Deleted : AssetStatus.Trashed,
    });
    await this.eventRepository.emit(force ? 'AssetDeleteAll' : 'AssetTrashAll', {
      assetIds: ids,
      userId: auth.user.id,
    });
  }

  async getMetadata(auth: AuthDto, id: string): Promise<AssetMetadataResponseDto[]> {
    await this.requireAccess({ auth, permission: Permission.AssetRead, ids: [id] });
    return this.assetRepository.getMetadata(id);
  }

  async getOcr(auth: AuthDto, id: string): Promise<AssetOcrResponseDto[]> {
    await this.requireAccess({ auth, permission: Permission.AssetRead, ids: [id] });
    const ocr = await this.ocrRepository.getByAssetId(id);
    const asset = await this.assetRepository.getForOcr(id);

    if (!asset) {
      throw new BadRequestException('Asset not found');
    }

    const dimensions = getDimensions({
      exifImageHeight: asset.exifImageHeight,
      exifImageWidth: asset.exifImageWidth,
      orientation: asset.orientation,
    });

    return ocr.map((item) => transformOcrBoundingBox(item, asset.edits, dimensions));
  }

  async upsertBulkMetadata(auth: AuthDto, dto: AssetMetadataBulkUpsertDto): Promise<AssetMetadataBulkResponseDto[]> {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: dto.items.map((item) => item.assetId) });

    const uniqueKeys = new Set<string>();
    for (const item of dto.items) {
      const key = `(${item.assetId}, ${item.key})`;
      if (uniqueKeys.has(key)) {
        throw new BadRequestException(`Duplicate items are not allowed: "${key}"`);
      }

      uniqueKeys.add(key);
    }

    return this.assetRepository.upsertBulkMetadata(dto.items);
  }

  async upsertMetadata(auth: AuthDto, id: string, dto: AssetMetadataUpsertDto): Promise<AssetMetadataResponseDto[]> {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: [id] });

    const uniqueKeys = new Set<string>();
    for (const { key } of dto.items) {
      if (uniqueKeys.has(key)) {
        throw new BadRequestException(`Duplicate items are not allowed: "${key}"`);
      }

      uniqueKeys.add(key);
    }

    return this.assetRepository.upsertMetadata(id, dto.items);
  }

  async getMetadataByKey(auth: AuthDto, id: string, key: string): Promise<AssetMetadataResponseDto> {
    await this.requireAccess({ auth, permission: Permission.AssetRead, ids: [id] });

    const item = await this.assetRepository.getMetadataByKey(id, key);
    if (!item) {
      throw new BadRequestException(`Metadata with key "${key}" not found for asset with id "${id}"`);
    }
    return item;
  }

  async deleteMetadataByKey(auth: AuthDto, id: string, key: string): Promise<void> {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: [id] });
    return this.assetRepository.deleteMetadataByKey(id, key);
  }

  async deleteBulkMetadata(auth: AuthDto, dto: AssetMetadataBulkDeleteDto) {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: dto.items.map((item) => item.assetId) });
    await this.assetRepository.deleteBulkMetadata(dto.items);
  }

  async run(auth: AuthDto, dto: AssetJobsDto) {
    await this.requireAccess({ auth, permission: Permission.AssetUpdate, ids: dto.assetIds });

    const jobs: JobItem[] = [];

    for (const id of dto.assetIds) {
      switch (dto.name) {
        case AssetJobName.REFRESH_FACES: {
          jobs.push({ name: JobName.AssetDetectFaces, data: { id } });
          break;
        }

        case AssetJobName.REFRESH_METADATA: {
          jobs.push({ name: JobName.AssetExtractMetadata, data: { id } });
          break;
        }

        case AssetJobName.REGENERATE_THUMBNAIL: {
          jobs.push({ name: JobName.AssetGenerateThumbnails, data: { id } });
          break;
        }

        case AssetJobName.TRANSCODE_VIDEO: {
          jobs.push({ name: JobName.AssetEncodeVideo, data: { id } });
          break;
        }
      }
    }

    await this.jobRepository.queueAll(jobs);
  }

  private async findOrFail(id: string) {
    const asset = await this.assetRepository.getById(id);
    if (!asset) {
      throw new BadRequestException('Asset not found');
    }
    return asset;
  }

  private async updateExif(dto: {
    id: string;
    description?: string;
    dateTimeOriginal?: string;
    latitude?: number;
    longitude?: number;
    rating?: number | null;
  }) {
    const { id, description, dateTimeOriginal, latitude, longitude, rating } = dto;
    const writes = _.omitBy(
      {
        description,
        dateTimeOriginal,
        timeZone: extractTimeZone(dateTimeOriginal)?.name,
        latitude,
        longitude,
        rating,
      },
      _.isUndefined,
    );

    if (Object.keys(writes).length > 0) {
      await this.assetRepository.upsertExif(
        updateLockedColumns({
          assetId: id,
          ...writes,
        }),
        { lockedPropertiesBehavior: 'append' },
      );
      await this.jobRepository.queue({ name: JobName.SidecarWrite, data: { id } });
    }
  }

  async getAssetEdits(auth: AuthDto, id: string): Promise<AssetEditsResponseDto> {
    await this.requireAccess({ auth, permission: Permission.AssetRead, ids: [id] });
    const edits = await this.assetEditRepository.getAll(id);

    return {
      assetId: id,
      edits,
    };
  }

  async editAsset(auth: AuthDto, id: string, dto: AssetEditsCreateDto): Promise<AssetEditsResponseDto> {
    await this.requireAccess({ auth, permission: Permission.AssetEditCreate, ids: [id] });

    const asset = await this.assetRepository.getForEdit(id);
    if (!asset) {
      throw new BadRequestException('Asset not found');
    }

    if (asset.type !== AssetType.Image) {
      throw new BadRequestException('Only images can be edited');
    }

    if (asset.livePhotoVideoId) {
      throw new BadRequestException('Editing live photos is not supported');
    }

    if (isPanorama(asset)) {
      throw new BadRequestException('Editing panorama images is not supported');
    }

    if (asset.originalPath?.toLowerCase().endsWith('.gif')) {
      throw new BadRequestException('Editing GIF images is not supported');
    }

    if (asset.originalPath?.toLowerCase().endsWith('.svg')) {
      throw new BadRequestException('Editing SVG images is not supported');
    }

    // check that crop parameters will not go out of bounds
    const { width: assetWidth, height: assetHeight } = getDimensions(asset);

    if (!assetWidth || !assetHeight) {
      throw new BadRequestException('Asset dimensions are not available for editing');
    }

    const edits = dto.edits as AssetEditActionItem[];
    const crop = edits.find((e) => e.action === AssetEditAction.Crop);
    if (crop) {
      if (edits[0].action !== AssetEditAction.Crop) {
        throw new BadRequestException('Crop action must be the first edit action');
      }

      // check that crop parameters will not go out of bounds
      const { width: assetWidth, height: assetHeight } = getDimensions(asset);

      if (!assetWidth || !assetHeight) {
        throw new BadRequestException('Asset dimensions are not available for editing');
      }

      const { x, y, width, height } = crop.parameters;
      if (x + width > assetWidth || y + height > assetHeight) {
        throw new BadRequestException('Crop parameters are out of bounds');
      }
    }

    const newEdits = await this.assetEditRepository.replaceAll(id, edits);
    await this.jobRepository.queue({ name: JobName.AssetEditThumbnailGeneration, data: { id } });

    // Return the asset and its applied edits
    return {
      assetId: id,
      edits: newEdits,
    };
  }

  async removeAssetEdits(auth: AuthDto, id: string): Promise<void> {
    await this.requireAccess({ auth, permission: Permission.AssetEditDelete, ids: [id] });

    const asset = await this.assetRepository.getById(id);
    if (!asset) {
      throw new BadRequestException('Asset not found');
    }

    await this.assetEditRepository.replaceAll(id, []);
    await this.jobRepository.queue({ name: JobName.AssetEditThumbnailGeneration, data: { id } });
  }

  /**
   * Query aesthetic scores for a list of assets and return a map of asset ID to score.
   * This method is used to enrich asset responses with aesthetic scores from the Data Pipeline DB.
   * 
   * @param assetIds - Array of asset IDs to query scores for
   * @returns Map of asset ID to aesthetic score (0.0 to 1.0), or empty map if feature is disabled or query fails
   */
  async getAestheticScoresForAssets(assetIds: string[]): Promise<Map<string, number>> {
    if (assetIds.length === 0) {
      return new Map();
    }

    try {
      const scoresMap = await this.aestheticIntegrationService.getScoresForAssets(assetIds);
      // Convert AestheticScoreDto map to simple score map
      const scoreValues = new Map<string, number>();
      for (const [assetId, scoreDto] of scoresMap.entries()) {
        scoreValues.set(assetId, scoreDto.score);
      }
      return scoreValues;
    } catch (error) {
      // Log error but don't fail the request - graceful degradation
      this.logger.error(`Failed to query aesthetic scores: ${error}`, {
        assetIds: assetIds.slice(0, 10), // Log first 10 IDs to avoid huge logs
        error,
      });
      return new Map();
    }
  }

  /**
   * Enrich asset responses with aesthetic scores and sort by score descending.
   * Assets without scores are placed at the end (NULLS LAST behavior).
   * 
   * This is a helper method that can be used by any service that needs to return
   * score-sorted assets. It handles score retrieval, merging, and sorting in one call.
   * 
   * @param assets - Array of asset response DTOs to enrich and sort
   * @returns The same array with aestheticScore field added and sorted by score descending
   * 
   * @example
   * const assets = await assetRepository.getAll(userId);
   * const mappedAssets = assets.map(asset => mapAsset(asset, { auth }));
   * const sortedAssets = await assetService.enrichAndSortByAestheticScore(mappedAssets);
   */
  async enrichAndSortByAestheticScore<T extends AssetResponseDto>(assets: T[]): Promise<Array<T & { aestheticScore: number | null }>> {
    if (assets.length === 0) {
      return assets as Array<T & { aestheticScore: number | null }>;
    }

    // Extract asset IDs
    const assetIds = assets.map(asset => asset.id);

    // Query aesthetic scores
    const scoresMap = await this.getAestheticScoresForAssets(assetIds);

    // Track gallery query metric
    // Requirement: 14.4
    const hasScores = scoresMap.size > 0;
    const hasScoresLabel = hasScores ? 'true' : 'false';
    this.telemetryRepository.api.addToCounter(`immich_gallery_queries_total.has_scores_${hasScoresLabel}`, 1);

    // Merge scores into asset responses
    const assetsWithScores = assets.map(asset => ({
      ...asset,
      aestheticScore: scoresMap.get(asset.id) ?? null,
    }));

    // Sort by aesthetic score using utility function
    return sortByAestheticScore(assetsWithScores);
  }
}
