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
package org.apache.pinot.server.api.resources;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.restlet.resources.PrimaryKeyCountInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrimaryKeyCount {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryKeyCount.class);

  private PrimaryKeyCount() {
  }

  /**
   * Computes the number of primary keys for this instance
   */
  public static PrimaryKeyCountInfo computeNumberOfPrimaryKeys(String instanceId,
      InstanceDataManager instanceDataManager) {
    if (StringUtils.isEmpty(instanceId)) {
      throw new IllegalArgumentException("InstanceID cannot be null or empty while computing the number of upsert / "
          + "dedup primary keys.");
    }

    if (instanceDataManager == null) {
      throw new IllegalArgumentException("instanceDataManager cannot be null while computing the number of upsert / "
          + "dedup primary keys.");
    }

    Set<String> allTables = instanceDataManager.getAllTables();
    long totalPrimaryKeyCount = 0L;
    Set<String> tablesWithPrimaryKeys = new HashSet<>();
    for (String tableNameWithType : allTables) {
      TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
      if (tableDataManager == null) {
        LOGGER.warn("TableDataManager for table: {} is null, skipping", tableNameWithType);
        continue;
      }
      if (tableDataManager instanceof RealtimeTableDataManager) {
        Map<Integer, Long> partitionToPrimaryKeyCount =
            getPartitionToPrimaryKeyCount((RealtimeTableDataManager) tableDataManager);

        if (!partitionToPrimaryKeyCount.isEmpty()) {
          tablesWithPrimaryKeys.add(tableNameWithType);
        }

        for (Long numPrimaryKeys : partitionToPrimaryKeyCount.values()) {
          totalPrimaryKeyCount += numPrimaryKeys == null ? 0 : numPrimaryKeys;
        }
      }
    }

    return new PrimaryKeyCountInfo(instanceId, totalPrimaryKeyCount, tablesWithPrimaryKeys, System.currentTimeMillis());
  }

  private static Map<Integer, Long> getPartitionToPrimaryKeyCount(RealtimeTableDataManager tableDataManager) {
    // Fetch the primary key count per partition if either upsert or dedup is enabled
    if (tableDataManager.isUpsertEnabled()) {
      return tableDataManager.getTableUpsertMetadataManager().getPartitionToPrimaryKeyCount();
    } else if (tableDataManager.isDedupEnabled()) {
      return tableDataManager.getTableDedupMetadataManager().getPartitionToPrimaryKeyCount();
    }
    return Map.of();
  }
}
