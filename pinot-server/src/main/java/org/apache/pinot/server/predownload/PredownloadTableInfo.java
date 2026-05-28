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

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  /// Checks whether the segment already exists locally with a matching CRC, and if so populates
  /// its local size. Reads CRC directly from `creation.meta` and size from directory
  /// traversal — no mmap of index files is performed (segment loading).
  ///
  /// **Note:** a CRC match does not guarantee segment integrity. If the local segment directory is
  /// corrupted (e.g. truncated index files), the server startup path handles recovery via
  /// `BaseTableDataManager.addNewOnlineSegment` which performs a full segment load. If loading fails, it falls
  /// back to `downloadAndLoadSegment` to re-fetch the segment from deep store or peers.
  ///
  /// @param predownloadSegmentInfo SegmentInfo of segment to be checked
  /// @return true if the segment is present with matching CRC (download can be skipped),
  ///         false if it is missing or has a CRC mismatch
  public boolean loadSegmentFromLocal(PredownloadSegmentInfo predownloadSegmentInfo) {
    File segDir = predownloadSegmentInfo.getSegmentDataDir(this, true);
    if (!segDir.isDirectory()) {
      LOGGER.info("Segment: {} of table: {} does not exist", predownloadSegmentInfo.getSegmentName(),
          _tableNameWithType);
      return false;
    }
    predownloadSegmentInfo.updateSegmentInfoFromLocal(segDir);

    String segmentName = predownloadSegmentInfo.getSegmentName();
    if (!predownloadSegmentInfo.hasSameCRC()) {
      if (predownloadSegmentInfo.getLocalCrc() == null) {
        LOGGER.info("Segment: {} of table: {} has no creation.meta", segmentName, _tableNameWithType);
      } else {
        LOGGER.info("Segment: {} of table: {} has crc change from: {} to: {}", segmentName, _tableNameWithType,
            predownloadSegmentInfo.getLocalCrc(), predownloadSegmentInfo.getCrc());
      }
      return false;
    }
    LOGGER.info("Skip downloading segment: {} of table: {} as it already exists", segmentName, _tableNameWithType);
    return true;
  }
}
