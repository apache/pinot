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
import com.google.common.util.concurrent.AtomicDouble;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;


class RetentionConcurrentMapPartitionDedupMetadataManager extends BaseRetentionPartitionDedupMetadataManager {
  @VisibleForTesting
  final AtomicDouble _largestSeenTime = new AtomicDouble(0);
  @VisibleForTesting
  final ConcurrentHashMap<Object, Pair<IndexSegment, Double>> _primaryKeyToSegmentAndTimeMap =
      new ConcurrentHashMap<>();

  protected RetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction,
      double metadataTTL, String metadataTimeColumn) {
    super(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction, metadataTTL,
        metadataTimeColumn);
  }

  @Override
  protected void addSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      double metadataTime = dedupRecordInfo.getDedupTime();
      if (metadataTime >= _largestSeenTime.get() - _metadataTTL) {
        _largestSeenTime.getAndUpdate(time -> Math.max(time, metadataTime));
        _primaryKeyToSegmentAndTimeMap.put(HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction),
            Pair.of(segment, metadataTime));
      }
    }
  }

  @Override
  protected void removeSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      _primaryKeyToSegmentAndTimeMap.computeIfPresent(
          HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction), (primaryKey, segmentAndTime) -> {
            if (segmentAndTime.getLeft() == segment && segmentAndTime.getRight() == dedupRecordInfo.getDedupTime()) {
              return null;
            } else {
              return segmentAndTime;
            }
          });
    }
  }

  @Override
  public int removeExpiredPrimaryKeys() {
    double smallestTimeToKeep = _largestSeenTime.get() - _metadataTTL;
    AtomicInteger numExpiredPrimaryKeys = new AtomicInteger();
    _primaryKeyToSegmentAndTimeMap.entrySet().removeIf(entry -> {
      boolean shouldRemove = entry.getValue().getRight() < smallestTimeToKeep;
      if (shouldRemove) {
        numExpiredPrimaryKeys.incrementAndGet();
      }
      return shouldRemove;
    });
    return numExpiredPrimaryKeys.get();
  }

  @Override
  public boolean dropOrAddRecord(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    if (dedupRecordInfo.getDedupTime() < _largestSeenTime.get() - _metadataTTL) {
      return true;
    }
    _largestSeenTime.getAndUpdate(time -> Math.max(time, dedupRecordInfo.getDedupTime()));
    boolean present = _primaryKeyToSegmentAndTimeMap.putIfAbsent(
        HashUtils.hashPrimaryKey(dedupRecordInfo.getPrimaryKey(), _hashFunction),
        Pair.of(indexSegment, dedupRecordInfo.getDedupTime())) != null;
    if (!present) {
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          _primaryKeyToSegmentAndTimeMap.size());
    }
    return present;
  }
}
