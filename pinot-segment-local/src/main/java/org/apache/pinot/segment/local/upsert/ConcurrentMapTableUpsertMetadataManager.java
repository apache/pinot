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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.config.table.UpsertConfig;


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
  public void setSegmentContexts(List<SegmentContext> segmentContexts, Map<String, String> queryOptions) {
    if (_consistencyMode != UpsertConfig.ConsistencyMode.NONE && !QueryOptionsUtils.isSkipUpsertView(queryOptions)) {
      // Get queryableDocIds bitmaps from partitionMetadataManagers if any consistency mode is used.
      _partitionMetadataManagerMap.forEach(
          (partitionID, upsertMetadataManager) -> upsertMetadataManager.setSegmentContexts(segmentContexts,
              queryOptions));
    }
    // If no consistency mode is used, we get queryableDocIds bitmaps as kept by the segment objects directly.
    // Even if consistency mode is used, we should still check if any segment doesn't get its validDocIds bitmap,
    // because partitionMetadataManagers may not track all segments of the table, like those out of the metadata TTL.
    for (SegmentContext segmentContext : segmentContexts) {
      if (segmentContext.getQueryableDocIdsSnapshot() == null) {
        IndexSegment segment = segmentContext.getIndexSegment();
        segmentContext.setQueryableDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment));
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    for (ConcurrentMapPartitionUpsertMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.close();
    }
  }
}
