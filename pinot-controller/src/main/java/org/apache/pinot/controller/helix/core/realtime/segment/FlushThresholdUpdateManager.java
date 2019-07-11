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
package org.apache.pinot.controller.helix.core.realtime.segment;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager which maintains the flush threshold update objects for each table
 */
public class FlushThresholdUpdateManager {

  private ConcurrentMap<String, FlushThresholdUpdater> _flushThresholdUpdaterMap = new ConcurrentHashMap<>();

  /**
   * Check table config for flush size.
   *
   * If flush size < 0, create a new DefaultFlushThresholdUpdater with default flush size
   * If flush size > 0, create a new DefaultFlushThresholdUpdater with given flush size.
   * If flush size == 0, create new SegmentSizeBasedFlushThresholdUpdater if not already created. Create only 1 per table, because we want to maintain tuning information for the table in the updater
   */
  public FlushThresholdUpdater getFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    final String tableName = realtimeTableConfig.getTableName();
    PartitionLevelStreamConfig streamConfig =
        new PartitionLevelStreamConfig(realtimeTableConfig.getIndexingConfig().getStreamConfigs());

    final int tableFlushSize = streamConfig.getFlushThresholdRows();

    if (tableFlushSize == 0) {
      final long desiredSegmentSize = streamConfig.getFlushSegmentDesiredSizeBytes();
      final int flushAutotuneInitialRows = streamConfig.getFlushAutotuneInitialRows();
      return _flushThresholdUpdaterMap.computeIfAbsent(tableName,
          k -> new SegmentSizeBasedFlushThresholdUpdater(desiredSegmentSize, flushAutotuneInitialRows));
    } else {
      _flushThresholdUpdaterMap.remove(tableName);
      return new DefaultFlushThresholdUpdater(tableFlushSize);
    }
  }

  public void clearFlushThresholdUpdater(TableConfig tableConfig) {
    _flushThresholdUpdaterMap.remove(tableConfig.getTableName());
  }
}
