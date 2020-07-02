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
import org.apache.pinot.spi.stream.PartitionLevelStreamConfig;


/**
 * Manager which maintains the flush threshold update objects for each table
 */
public class FlushThresholdUpdateManager {
  private final ConcurrentMap<String, FlushThresholdUpdater> _flushThresholdUpdaterMap = new ConcurrentHashMap<>();

  /**
   * Check table config for flush size.
   *
   * If flush size > 0, create a new DefaultFlushThresholdUpdater with given flush size.
   * If flush size <= 0, create new SegmentSizeBasedFlushThresholdUpdater if not already created. Create only 1 per
   * table because we want to maintain tuning information for the table in the updater.
   */
  public FlushThresholdUpdater getFlushThresholdUpdater(PartitionLevelStreamConfig streamConfig) {
    String realtimeTableName = streamConfig.getTableNameWithType();
    int flushThresholdRows = streamConfig.getFlushThresholdRows();

    if (flushThresholdRows > 0) {
      _flushThresholdUpdaterMap.remove(realtimeTableName);
      return new DefaultFlushThresholdUpdater(flushThresholdRows);
    } else {
      return _flushThresholdUpdaterMap
          .computeIfAbsent(realtimeTableName, k -> new SegmentSizeBasedFlushThresholdUpdater());
    }
  }

  public void clearFlushThresholdUpdater(String realtimeTableName) {
    _flushThresholdUpdaterMap.remove(realtimeTableName);
  }
}
