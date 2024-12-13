package org.apache.pinot.tools.predownload;

import io.netty.util.internal.StringUtil;
import java.io.File;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentInfo.class);
  private final String segmentName;
  private final String tableNameWithType;

  @SuppressWarnings("NullAway.Init")
  private String downloadUrl;

  @SuppressWarnings("NullAway.Init")
  private long crc;

  @Nullable
  private String localCrc;
  private long localSizeBytes;

  @SuppressWarnings("NullAway.Init")
  private String crypterName;

  @SuppressWarnings("NullAway.Init")
  private String tier;

  public SegmentInfo(String tableNameWithType, String segmentName) {
    this.segmentName = segmentName;
    this.tableNameWithType = tableNameWithType;
    this.localSizeBytes = 0;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public String getTableNameWithType() {
    return tableNameWithType;
  }

  public String getDownloadUrl() {
    return downloadUrl;
  }

  public long getCrc() {
    return crc;
  }

  public String getCrypterName() {
    return crypterName;
  }

  public String getTier() {
    return tier;
  }

  public boolean isDownloaded() {
    return hasSameCRC();
  }

  public boolean canBeDownloaded() {
    return this.downloadUrl != null && this.downloadUrl.length() > 0;
  }

  public long getLocalSizeBytes() {
    return localSizeBytes;
  }

  @Nullable
  public String getLocalCrc() {
    return localCrc;
  }

  public void updateSegmentInfo(SegmentZKMetadata segmentZKMetadata) {
    downloadUrl = segmentZKMetadata.getDownloadUrl();
    if (StringUtil.isNullOrEmpty(downloadUrl)) {
      LOGGER.info("Segment: {} of table: {} cannot be downloaded due to empty deep store download url from ZK",
          segmentName, tableNameWithType);
    }
    crc = segmentZKMetadata.getCrc();
    crypterName = segmentZKMetadata.getCrypterName();
    tier = segmentZKMetadata.getTier();
  }

  @Nullable
  public SegmentDirectory initSegmentDirectory(IndexLoadingConfig indexLoadingConfig, TableInfo tableInfo) {
    try {
      SegmentDirectoryLoaderContext loaderContext =
          new SegmentDirectoryLoaderContext.Builder().setTableConfig(indexLoadingConfig.getTableConfig())
              .setSchema(indexLoadingConfig.getSchema()).setInstanceId(indexLoadingConfig.getInstanceId())
              .setTableDataDir(indexLoadingConfig.getTableDataDir()).setSegmentName(segmentName)
              .setSegmentCrc(String.valueOf(crc)).setSegmentTier(indexLoadingConfig.getSegmentTier())
              .setInstanceTierConfigs(indexLoadingConfig.getInstanceTierConfigs())
              .setSegmentDirectoryConfigs(indexLoadingConfig.getSegmentDirectoryConfigs()).build();
      SegmentDirectoryLoader segmentDirectoryLoader =
          SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
      File indexDir = getSegmentDataDir(tableInfo, true);
      return segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize SegmentDirectory for segment: {} of table: {}", segmentName, tableNameWithType,
          e);
      return null;
    }
  }

  public File getSegmentDataDir(@Nullable TableInfo tableInfo) {
    if (tableInfo == null) {
      throw new PredownloadException("Table info not found for segment: " + segmentName);
    }
    String tableDataDir =
        tableInfo.getInstanceDataManagerConfig().getInstanceDataDir() + File.separator + tableInfo.getTableConfig()
            .getTableName();
    return new File(tableDataDir, segmentName);
  }

  File getSegmentDataDir(@Nullable TableInfo tableInfo, boolean withTier) {
    if (tier == null || !withTier) {
      return getSegmentDataDir(tableInfo);
    }
    try {
      if (tableInfo == null) {
        throw new PredownloadException("Table info not found for segment: " + segmentName);
      }
      String tierDataDir = TierConfigUtils.getDataDirForTier(tableInfo.getTableConfig(), tier,
          tableInfo.getInstanceDataManagerConfig().getTierConfigs());
      File tierTableDataDir = new File(tierDataDir, tableNameWithType);
      return new File(tierTableDataDir, segmentName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get dataDir for segment: {} of table: {} on tier: {} due to error: {}", segmentName,
          tableNameWithType, tier, e.getMessage());
      return this.getSegmentDataDir(tableInfo);
    }
  }

  public boolean hasSameCRC() {
    try {
      return this.crc == Long.parseLong(localCrc);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public void updateSegmentInfoFromLocal(@Nullable SegmentDirectory segmentDirectory) {
    SegmentMetadataImpl segmentMetadata = (segmentDirectory == null) ? null : segmentDirectory.getSegmentMetadata();
    this.localCrc = (segmentMetadata == null) ? null : segmentMetadata.getCrc();
    this.localSizeBytes = (segmentDirectory == null) ? 0 : segmentDirectory.getDiskSizeBytes();
  }
}
