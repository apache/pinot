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
package org.apache.pinot.segment.local.upsert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;


/**
 * The manager of the upsert metadata of a table.
 */
@ThreadSafe
public class TableUpsertMetadataManager {
  private final Map<Integer, PartitionUpsertMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  private final String _tableNameWithType;
  private final ServerMetrics _serverMetrics;
  private final TableDataManager _tableDataManager;
  private final PartialUpsertHandler _partialUpsertHandler;

  public TableUpsertMetadataManager(String tableNameWithType, ServerMetrics serverMetrics,
      TableDataManager tableDataManager, @Nullable PartialUpsertHandler partialUpsertHandler) {
    _tableNameWithType = tableNameWithType;
    _serverMetrics = serverMetrics;
    _tableDataManager = tableDataManager;
    _partialUpsertHandler = partialUpsertHandler;
  }

  public PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId,
        k -> new PartitionUpsertMetadataManager(_tableNameWithType, k, _serverMetrics, _tableDataManager,
            _partialUpsertHandler));
  }
}
