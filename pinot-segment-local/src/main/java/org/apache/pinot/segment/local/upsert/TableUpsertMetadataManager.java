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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMetrics;


/**
 * The manager of the upsert metadata of a table.
 */
@ThreadSafe
public class TableUpsertMetadataManager {
  private final Map<Integer, PartitionUpsertMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  private final String _tableNameWithType;
  private final ServerMetrics _serverMetrics;

  public TableUpsertMetadataManager(String tableNameWithType, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _serverMetrics = serverMetrics;
  }

  public PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap
        .computeIfAbsent(partitionId, k -> new PartitionUpsertMetadataManager(_tableNameWithType, k, _serverMetrics));
  }
}
