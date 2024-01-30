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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;


/**
 * Implementation of {@link TableUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap}.
 */
@ThreadSafe
public class ConcurrentMapTableUpsertMetadataManager extends BaseTableUpsertMetadataManager {
  private final Map<Integer, ConcurrentMapPartitionUpsertMetadataManager> _partitionMetadataManagerMap =
      new ConcurrentHashMap<>();

  @Override
  public ConcurrentMapPartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId,
        k -> new ConcurrentMapPartitionUpsertMetadataManager(_tableNameWithType, k, _context));
  }

  @Override
  public void stop() {
    for (ConcurrentMapPartitionUpsertMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.stop();
    }
  }

  @Override
  public Map<Integer, Long> getPartitionToPrimaryKeyCount() {
    Map<Integer, Long> partitionToPrimaryKeyCount = new HashMap<>();
    _partitionMetadataManagerMap.forEach(
        (partitionID, upsertMetadataManager) -> partitionToPrimaryKeyCount.put(partitionID,
            upsertMetadataManager.getNumPrimaryKeys()));
    return partitionToPrimaryKeyCount;
  }

  @Override
  public void close()
      throws IOException {
    for (ConcurrentMapPartitionUpsertMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.close();
    }
  }
}
