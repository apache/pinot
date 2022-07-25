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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.HashFunction;


/**
 * The manager of the upsert metadata of a table.
 */
@ThreadSafe
public class TableUpsertMetadataManager {
  private final Map<Integer, PartitionUpsertMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final String _comparisonColumn;
  private final HashFunction _hashFunction;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final ServerMetrics _serverMetrics;

  public TableUpsertMetadataManager(String tableNameWithType, List<String> primaryKeyColumns, String comparisonColumn,
      HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _serverMetrics = serverMetrics;
  }

  public PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId,
        k -> new PartitionUpsertMetadataManager(_tableNameWithType, k, _primaryKeyColumns, _comparisonColumn,
            _hashFunction, _partialUpsertHandler, _serverMetrics));
  }

  public boolean isPartialUpsertEnabled() {
    return _partialUpsertHandler != null;
  }
}
