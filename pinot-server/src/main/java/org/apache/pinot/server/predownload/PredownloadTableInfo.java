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

import java.io.File;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PredownloadTableInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredownloadTableInfo.class);
  private final String _tableNameWithType;
  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final TableConfig _tableConfig;
  @Nullable
  private final Schema _schema;

  public PredownloadTableInfo(String tableNameWithType, TableConfig tableConfig, @Nullable Schema schema,
      InstanceDataManagerConfig instanceDataManagerConfig) {
    _tableNameWithType = tableNameWithType;
    _tableConfig = tableConfig;
    _schema = schema;
    _instanceDataManagerConfig = instanceDataManagerConfig;
  }

  private static void closeSegmentDirectoryQuietly(@Nullable SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  /**
   * After loading segment metadata from ZK, try to load from local and check if we are able to skip
   * the downloading
   *
   * @param predownloadSegmentInfo SegmentInfo of segment to be loaded
   * @param instanceDataManagerConfig InstanceDataManagerConfig loaded from scheduler
   * @return true if already presents, false if needs to be downloaded
   */
  public boolean loadSegmentFromLocal(PredownloadSegmentInfo predownloadSegmentInfo,
      InstanceDataManagerConfig instanceDataManagerConfig) {
    SegmentDirectory segmentDirectory = null;
    try {
      segmentDirectory = getSegmentDirectory(predownloadSegmentInfo, instanceDataManagerConfig);
      predownloadSegmentInfo.updateSegmentInfoFromLocal(segmentDirectory);

      String segmentName = predownloadSegmentInfo.getSegmentName();
      // If the segment doesn't exist on server or its CRC has changed, then we
      // need to fall back to download the segment from deep store to load it.
      if (!predownloadSegmentInfo.hasSameCRC()) {
        if (predownloadSegmentInfo.getLocalCrc() == null) {
          LOGGER.info("Segment: {} of table: {} does not exist", segmentName, _tableNameWithType);
        } else {
          LOGGER.info("Segment: {} of table: {} has crc change from: {} to: {}", segmentName, _tableNameWithType,
              predownloadSegmentInfo.getLocalCrc(), predownloadSegmentInfo.getCrc());
        }
        return false;
      }
      LOGGER.info("Skip downloading segment: {} of table: {} as it already exists", segmentName, _tableNameWithType);
      return true;
    } finally {
      closeSegmentDirectoryQuietly(segmentDirectory);
    }
  }

  @Nullable
  private SegmentDirectory getSegmentDirectory(PredownloadSegmentInfo predownloadSegmentInfo,
      InstanceDataManagerConfig instanceDataManagerConfig) {
    String dataDir = instanceDataManagerConfig.getInstanceDataDir() + File.separator + _tableConfig.getTableName();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(instanceDataManagerConfig, _tableConfig, _schema);
    indexLoadingConfig.setSegmentTier(predownloadSegmentInfo.getTier());
    indexLoadingConfig.setTableDataDir(dataDir);
    return predownloadSegmentInfo.initSegmentDirectory(indexLoadingConfig, this);
  }
}
