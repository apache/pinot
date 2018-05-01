/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Map;
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

  public FlushThresholdUpdater getFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    String tableName = realtimeTableConfig.getTableName();
    return _flushThresholdUpdaterMap.computeIfAbsent(tableName, k -> createFlushThresholdUpdater(realtimeTableConfig));
  }

  private FlushThresholdUpdater createFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    int tableFlushSize = getLLCRealtimeTableFlushSize(realtimeTableConfig);
    if (tableFlushSize <= 0) {
      return new SegmentSizeBasedFlushThresholdUpdater();
    } else {
      return new DefaultFlushThresholdUpdater(tableFlushSize);
    }
  }

  /**
   * Returns the max number of rows that a host holds across all consuming LLC partitions.
   * This number should be divided by the number of partitions on the host, so as to get
   * the flush limit for each segment.
   *
   * If flush threshold is configured for LLC, return it, otherwise, if flush threshold is
   * configured for HLC, then return that value, else return -1.
   *
   * Flush threshold will be set to 0 for autotuning the thresholds
   *
   * @param tableConfig
   * @return -1 if tableConfig is null, or neither value is configured
   */
  private int getLLCRealtimeTableFlushSize(TableConfig tableConfig) {
    final Map<String, String> streamConfigs = tableConfig.getIndexingConfig().getStreamConfigs();
    String flushSizeStr;
    if (streamConfigs == null) {
      return -1;
    }
    if (streamConfigs.containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE)) {
      flushSizeStr = streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE);
      try {
        return Integer.parseInt(flushSizeStr);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse LLC flush size of {} for table {}", flushSizeStr, tableConfig.getTableName(), e);
      }
    }

    if (streamConfigs.containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE)) {
      flushSizeStr = streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE);
      try {
        return Integer.parseInt(flushSizeStr);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse flush size of {} for table {}", flushSizeStr, tableConfig.getTableName(), e);
      }
    }
    return -1;
  }

}
