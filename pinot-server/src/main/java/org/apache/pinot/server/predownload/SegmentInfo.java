/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.predownload;

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
  private final String _segmentName;
  private final String _tableNameWithType;

  @SuppressWarnings("NullAway.Init")
  private String _downloadUrl;

  @SuppressWarnings("NullAway.Init")
  private long _crc;

  @Nullable
  private String _localCrc;
  private long _localSizeBytes;

  @SuppressWarnings("NullAway.Init")
  private String _crypterName;

  @SuppressWarnings("NullAway.Init")
  private String _tier;

  public SegmentInfo(String tableNameWithType, String segmentName) {
    _segmentName = segmentName;
    _tableNameWithType = tableNameWithType;
    _localSizeBytes = 0;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getDownloadUrl() {
    return _downloadUrl;
  }

  public long getCrc() {
    return _crc;
  }

  public String getCrypterName() {
    return _crypterName;
  }

  public String getTier() {
    return _tier;
  }

  public boolean isDownloaded() {
    return hasSameCRC();
  }

  public boolean canBeDownloaded() {
    return _downloadUrl != null && _downloadUrl.length() > 0;
  }

  public long getLocalSizeBytes() {
    return _localSizeBytes;
  }

  @Nullable
  public String getLocalCrc() {
    return _localCrc;
  }

  public void updateSegmentInfo(SegmentZKMetadata segmentZKMetadata) {
    _downloadUrl = segmentZKMetadata.getDownloadUrl();
    if (StringUtil.isNullOrEmpty(_downloadUrl)) {
      LOGGER.info("Segment: {} of table: {} cannot be downloaded due to empty deep store download url from ZK",
          _segmentName, _tableNameWithType);
    }
    _crc = segmentZKMetadata.getCrc();
    _crypterName = segmentZKMetadata.getCrypterName();
    _tier = segmentZKMetadata.getTier();
  }

  @Nullable
  public SegmentDirectory initSegmentDirectory(IndexLoadingConfig indexLoadingConfig, TableInfo tableInfo) {
    try {
      SegmentDirectoryLoaderContext loaderContext =
          new SegmentDirectoryLoaderContext.Builder().setTableConfig(indexLoadingConfig.getTableConfig())
              .setSchema(indexLoadingConfig.getSchema()).setInstanceId(indexLoadingConfig.getInstanceId())
              .setTableDataDir(indexLoadingConfig.getTableDataDir()).setSegmentName(_segmentName)
              .setSegmentCrc(String.valueOf(_crc)).setSegmentTier(indexLoadingConfig.getSegmentTier())
              .setInstanceTierConfigs(indexLoadingConfig.getInstanceTierConfigs())
              .setSegmentDirectoryConfigs(indexLoadingConfig.getSegmentDirectoryConfigs()).build();
      SegmentDirectoryLoader segmentDirectoryLoader =
          SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
      File indexDir = getSegmentDataDir(tableInfo, true);
      return segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize SegmentDirectory for segment: {} of table: {}", _segmentName,
          _tableNameWithType, e);
      return null;
    }
  }

  public File getSegmentDataDir(@Nullable TableInfo tableInfo) {
    if (tableInfo == null) {
      throw new PredownloadException("Table info not found for segment: " + _segmentName);
    }
    String tableDataDir =
        tableInfo.getInstanceDataManagerConfig().getInstanceDataDir() + File.separator + tableInfo.getTableConfig()
            .getTableName();
    return new File(tableDataDir, _segmentName);
  }

  File getSegmentDataDir(@Nullable TableInfo tableInfo, boolean withTier) {
    if (_tier == null || !withTier) {
      return getSegmentDataDir(tableInfo);
    }
    try {
      if (tableInfo == null) {
        throw new PredownloadException("Table info not found for segment: " + _segmentName);
      }
      String tierDataDir = TierConfigUtils.getDataDirForTier(tableInfo.getTableConfig(), _tier,
          tableInfo.getInstanceDataManagerConfig().getTierConfigs());
      File tierTableDataDir = new File(tierDataDir, _tableNameWithType);
      return new File(tierTableDataDir, _segmentName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get dataDir for segment: {} of table: {} on tier: {} due to error: {}", _segmentName,
          _tableNameWithType, _tier, e.getMessage());
      return getSegmentDataDir(tableInfo);
    }
  }

  public boolean hasSameCRC() {
    try {
      return _crc == Long.parseLong(_localCrc);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public void updateSegmentInfoFromLocal(@Nullable SegmentDirectory segmentDirectory) {
    SegmentMetadataImpl segmentMetadata = (segmentDirectory == null) ? null : segmentDirectory.getSegmentMetadata();
    _localCrc = (segmentMetadata == null) ? null : segmentMetadata.getCrc();
    _localSizeBytes = (segmentDirectory == null) ? 0 : segmentDirectory.getDiskSizeBytes();
  }
}
