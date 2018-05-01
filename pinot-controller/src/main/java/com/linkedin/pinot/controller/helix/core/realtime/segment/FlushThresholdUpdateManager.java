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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Manager which maintains the flush threshold update objects for each table
 */
public class FlushThresholdUpdateManager {

  private ConcurrentMap<String, FlushThresholdUpdater> _flushThresholdUpdaterMap = new ConcurrentHashMap<>();

  public FlushThresholdUpdater getFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    String tableName = realtimeTableConfig.getTableName();
    return _flushThresholdUpdaterMap.computeIfAbsent(tableName, k -> createFlushThresholdUpdater(realtimeTableConfig));
  }

  private FlushThresholdUpdater createFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    if (realtimeTableConfig.getIndexingConfig() == null
        || realtimeTableConfig.getIndexingConfig().getStreamConsumptionConfig() == null) {
      return new DefaultFlushThresholdUpdater(realtimeTableConfig);
    }
    String flushThresholdUpdaterStrategy =
        realtimeTableConfig.getIndexingConfig().getStreamConsumptionConfig().getFlushThresholdUpdateStrategy();
    if (SegmentSizeBasedFlushThresholdUpdater.class.getSimpleName().equals(flushThresholdUpdaterStrategy)) {
      return new SegmentSizeBasedFlushThresholdUpdater(realtimeTableConfig);
    }
    return new DefaultFlushThresholdUpdater(realtimeTableConfig);
  }
}
