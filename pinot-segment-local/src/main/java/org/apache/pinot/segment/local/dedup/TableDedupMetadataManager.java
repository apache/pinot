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
package org.apache.pinot.segment.local.dedup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.HashFunction;


public class TableDedupMetadataManager {
  private final Map<Integer, PartitionDedupMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  private final HelixManager _helixManager;
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  public TableDedupMetadataManager(HelixManager helixManager, String tableNameWithType, List<String> primaryKeyColumns,
      ServerMetrics serverMetrics, HashFunction hashFunction) {
    _helixManager = helixManager;
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
  }

  public PartitionDedupMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId,
        k -> new PartitionDedupMetadataManager(_helixManager, _tableNameWithType, _primaryKeyColumns, k,
            _serverMetrics, _hashFunction));
  }
}
