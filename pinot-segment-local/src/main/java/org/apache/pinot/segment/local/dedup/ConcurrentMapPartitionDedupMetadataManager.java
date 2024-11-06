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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;


class ConcurrentMapPartitionDedupMetadataManager extends BasePartitionDedupMetadataManager {
  @VisibleForTesting
  final ConcurrentHashMap<Object, Pair<IndexSegment, Double>> _primaryKeyToSegmentAndTimeMap =
      new ConcurrentHashMap<>();

  protected ConcurrentMapPartitionDedupMetadataManager(String tableNameWithType, int partitionId,
      DedupContext dedupContext) {
    super(tableNameWithType, partitionId, dedupContext);
  }

  @Override
  protected void doPreloadSegment(ImmutableSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      double dedupTime = dedupRecordInfo.getDedupTime();
      _primaryKeyToSegmentAndTimeMap.put(HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction),
          Pair.of(segment, dedupTime));
    }
  }

  @Override
  protected void doAddOrReplaceSegment(IndexSegment oldSegment, IndexSegment newSegment,
      Iterator<DedupRecordInfo> dedupRecordInfoIteratorOfNewSegment) {
    String segmentName = newSegment.getSegmentName();
    while (dedupRecordInfoIteratorOfNewSegment.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIteratorOfNewSegment.next();
      double dedupTime = dedupRecordInfo.getDedupTime();
      _primaryKeyToSegmentAndTimeMap.compute(HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction),
          (primaryKey, segmentAndTime) -> {
            // Stale metadata is treated as not existing when checking for deduplicates.
            if (segmentAndTime == null || isOutOfMetadataTTL(segmentAndTime.getRight())) {
              return Pair.of(newSegment, dedupTime);
            }
            // when oldSegment is null, it means we are adding a new segment
            // when oldSegment is not null, it means we are replacing an existing segment
            if (oldSegment == null) {
              _logger.warn("When adding a new segment: record in segment: {} with primary key: {} and dedup "
                      + "time: {} already exists in segment: {} with dedup time: {}", segmentName,
                  dedupRecordInfo.getPrimaryKey(), dedupTime, segmentAndTime.getLeft().getSegmentName(),
                  segmentAndTime.getRight());
            } else if (segmentAndTime.getLeft() != oldSegment) {
              _logger.warn("When replacing a segment: record in segment: {} with primary key: {} and dedup "
                      + "time: {} exists in segment: {} with dedup time: {} (but not the segment: {} to replace)",
                  segmentName, dedupRecordInfo.getPrimaryKey(), dedupTime, segmentAndTime.getLeft().getSegmentName(),
                  segmentAndTime.getRight(), oldSegment.getSegmentName());
            }
            // When dedup time is the same, we always keep the latest segment
            // This will handle segment replacement case correctly - a typical case is when a mutable segment is
            // replaced by an immutable segment
            if (segmentAndTime.getRight() <= dedupTime) {
              return Pair.of(newSegment, dedupTime);
            }
            return segmentAndTime;
          });
    }
  }

  @Override
  protected void doRemoveSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      _primaryKeyToSegmentAndTimeMap.computeIfPresent(
          HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction), (primaryKey, segmentAndTime) -> {
            // do not need to compare dedup time because we are removing the segment
            if (segmentAndTime.getLeft() == segment) {
              return null;
            } else {
              return segmentAndTime;
            }
          });
    }
  }

  @Override
  protected void doRemoveExpiredPrimaryKeys() {
    double smallestTimeToKeep = _largestSeenTime.get() - _metadataTTL;
    _primaryKeyToSegmentAndTimeMap.entrySet().removeIf(entry -> entry.getValue().getRight() < smallestTimeToKeep);
  }

  @Override
  public boolean checkRecordPresentOrUpdate(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    if (!startOperation()) {
      _logger.info("Skip adding record to {} because metadata manager is already stopped",
          indexSegment.getSegmentName());
      return true;
    }
    try {
      if (_metadataTTL > 0) {
        _largestSeenTime.getAndUpdate(time -> Math.max(time, dedupRecordInfo.getDedupTime()));
      }
      AtomicBoolean present = new AtomicBoolean(false);
      _primaryKeyToSegmentAndTimeMap.compute(HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction),
          (primaryKey, segmentAndTime) -> {
            // The dedup metadata out of TTL is cleaned up when starting the next consuming segment, so it's possible
            // when ingesting records into current segment, some dedup metadata is already becoming stale. The stale
            // metadata is treated as not existing when checking for deduplicates.
            if (segmentAndTime == null || isOutOfMetadataTTL(segmentAndTime.getRight())) {
              return Pair.of(indexSegment, dedupRecordInfo.getDedupTime());
            }
            present.set(true);
            return segmentAndTime;
          });
      if (!present.get()) {
        updatePrimaryKeyGauge();
      }
      return present.get();
    } finally {
      finishOperation();
    }
  }

  @Override
  protected long getNumPrimaryKeys() {
    return _primaryKeyToSegmentAndTimeMap.size();
  }

  @Override
  protected void doClose()
      throws IOException {
    _primaryKeyToSegmentAndTimeMap.clear();
  }
}
