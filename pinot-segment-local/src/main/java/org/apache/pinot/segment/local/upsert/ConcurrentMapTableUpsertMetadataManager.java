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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link TableUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap}.
 */
@ThreadSafe
public class ConcurrentMapTableUpsertMetadataManager extends BaseTableUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentMapTableUpsertMetadataManager.class);

  private final Map<Integer, BasePartitionUpsertMetadataManager> _partitionMetadataManagerMap =
      new ConcurrentHashMap<>();

  @Override
  public BasePartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId,
        k -> _context.isEnableDeletedKeysCompactionConsistency()
            ? new ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes(_tableNameWithType, k, _context)
            : new ConcurrentMapPartitionUpsertMetadataManager(_tableNameWithType, k, _context));
  }

  @Override
  public void stop() {
    for (BasePartitionUpsertMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
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
  public void lockForSegmentContexts() {
    _partitionMetadataManagerMap.forEach(
        (partitionID, upsertMetadataManager) -> upsertMetadataManager.getUpsertViewManager().lockTrackedSegments());
  }

  @Override
  public void unlockForSegmentContexts() {
    _partitionMetadataManagerMap.forEach(
        (partitionID, upsertMetadataManager) -> upsertMetadataManager.getUpsertViewManager().unlockTrackedSegments());
  }

  @Override
  public Set<String> getNewlyAddedSegments() {
    Set<String> newlyAddedSegments = new HashSet<>();
    _partitionMetadataManagerMap.forEach((partitionID, upsertMetadataManager) -> newlyAddedSegments.addAll(
        upsertMetadataManager.getNewlyAddedSegments()));
    return newlyAddedSegments;
  }

  @Override
  public void setSegmentContexts(List<SegmentContext> segmentContexts, Map<String, String> queryOptions) {
    // Get queryableDocIds bitmaps from partitionMetadataManagers if any consistency mode is used.
    // Otherwise, get queryableDocIds bitmaps as kept by the segment objects directly as before.
    if (_context.getConsistencyMode() == UpsertConfig.ConsistencyMode.NONE || QueryOptionsUtils.isSkipUpsertView(
        queryOptions)) {
      for (SegmentContext segmentContext : segmentContexts) {
        IndexSegment segment = segmentContext.getIndexSegment();
        segmentContext.setQueryableDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment));
      }
      return;
    }
    // All segments should have been tracked by partitionMetadataManagers to provide queries consistent upsert view.
    _partitionMetadataManagerMap.forEach(
        (partitionID, upsertMetadataManager) -> upsertMetadataManager.getUpsertViewManager()
            .setSegmentContexts(segmentContexts, queryOptions));
    if (LOGGER.isDebugEnabled()) {
      for (SegmentContext segmentContext : segmentContexts) {
        IndexSegment segment = segmentContext.getIndexSegment();
        if (segmentContext.getQueryableDocIdsSnapshot() == null) {
          LOGGER.debug("No upsert view for segment: {}, type: {}, total: {}", segment.getSegmentName(),
              (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.getSegmentMetadata().getTotalDocs());
        } else {
          int cardCnt = segmentContext.getQueryableDocIdsSnapshot().getCardinality();
          LOGGER.debug("Got upsert view of segment: {}, type: {}, total: {}, valid: {}", segment.getSegmentName(),
              (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.getSegmentMetadata().getTotalDocs(),
              cardCnt);
        }
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    for (BasePartitionUpsertMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.close();
    }
  }
}
