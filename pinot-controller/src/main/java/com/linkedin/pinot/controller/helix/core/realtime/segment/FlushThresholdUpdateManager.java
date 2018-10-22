/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.realtime.segment;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager which maintains the flush threshold update objects for each table
 */
public class FlushThresholdUpdateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlushThresholdUpdateManager.class);

  private ConcurrentMap<String, FlushThresholdUpdater> _flushThresholdUpdaterMap = new ConcurrentHashMap<>();

  /**
   * Check table config for flush size.
   *
   * If flush size < 0, create a new DefaultFlushThresholdUpdater with default flush size
   * If flush size > 0, create a new DefaultFlushThresholdUpdater with given flush size.
   * If flush size == 0, create new SegmentSizeBasedFlushThresholdUpdater if not already created. Create only 1 per table, because we want to maintain tuning information for the table in the updater
   * @param realtimeTableConfig
   * @return
   */
  public FlushThresholdUpdater getFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    final String tableName = realtimeTableConfig.getTableName();
    PartitionLevelStreamConfig streamConfig =
        new PartitionLevelStreamConfig(realtimeTableConfig.getIndexingConfig().getStreamConfigs());

    final int tableFlushSize = streamConfig.getFlushThresholdRows();
    final long desiredSegmentSize = streamConfig.getFlushSegmentDesiredSizeBytes();

    if (tableFlushSize == 0) {
      return _flushThresholdUpdaterMap.computeIfAbsent(tableName,
          k -> new SegmentSizeBasedFlushThresholdUpdater(desiredSegmentSize));
    } else {
      _flushThresholdUpdaterMap.remove(tableName);
      return new DefaultFlushThresholdUpdater(tableFlushSize);
    }
  }

  public void clearFlushThresholdUpdater(TableConfig tableConfig) {
    _flushThresholdUpdaterMap.remove(tableConfig.getTableName());
  }
}
