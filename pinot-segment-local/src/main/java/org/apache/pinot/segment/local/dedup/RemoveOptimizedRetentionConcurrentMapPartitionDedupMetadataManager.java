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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;


class RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager
    extends BaseRetentionPartitionDedupMetadataManager {
  // Number of retention buckets to keep
  private static final int DEFAULT_RETENTION_BUCKET_COUNT = 2;

  @VisibleForTesting
  final AtomicLong _largestSeenTimeBucketId = new AtomicLong(0);
  @VisibleForTesting
  final ConcurrentHashMap<Long, ConcurrentHashMap<Object, IndexSegment>> _timeBucketToPrimaryKeyToSegmentMap
      = new ConcurrentHashMap<>();
  private final int _retentionBucketCount;

  public RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction,
      double metadataTTL, String metadataTimeColumn) {
    this(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction, metadataTTL,
        metadataTimeColumn, DEFAULT_RETENTION_BUCKET_COUNT);
  }

  @VisibleForTesting
  RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction,
      double metadataTTL, String metadataTimeColumn, int retentionBucketCount) {
    super(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction, metadataTTL,
        metadataTimeColumn);
    _retentionBucketCount = retentionBucketCount;
  }

  @Override
  void addSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
      double metadataTime = dedupRecordInfo.getDedupTime();
      long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
      // Skip the primary key if the time is out of retention
      if (bucketId >= _largestSeenTimeBucketId.get() - _retentionBucketCount) {
        // advance the largest seen time bucket id no matter the primary key is already present or not
        _largestSeenTimeBucketId.getAndUpdate(largestSeenTimeBucketId -> Math.max(largestSeenTimeBucketId, bucketId));
        Object primaryKeyHash = HashUtils.hashPrimaryKey(pk, _hashFunction);
        boolean primaryKeyPresent = false;
        long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - _retentionBucketCount;
        // iterator the time buckets to check if the primary key is already present
        for (Map.Entry<Long, ConcurrentHashMap<Object, IndexSegment>> entry
            : _timeBucketToPrimaryKeyToSegmentMap.entrySet()) {
          long timeBucketId = entry.getKey();
          // only need to check the primary key in the retention buckets
          if (timeBucketId >= smallestTimeBucketIdToKeep) {
            ConcurrentHashMap<Object, IndexSegment> primaryKeyToSegmentMap = entry.getValue();
            if (primaryKeyToSegmentMap.containsKey(primaryKeyHash)) {
              primaryKeyPresent = true;
              break;
            }
          }
        }
        if (!primaryKeyPresent) {
          _timeBucketToPrimaryKeyToSegmentMap.computeIfAbsent(bucketId, k -> new ConcurrentHashMap<>())
              .putIfAbsent(primaryKeyHash, segment);
        }
      }
    }
  }

  @Override
  void removeSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - _retentionBucketCount;
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      double metadataTime = dedupRecordInfo.getDedupTime();
      long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
      PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
      Object primaryKeyHash = HashUtils.hashPrimaryKey(pk, _hashFunction);
      // Skip the removal if the time is out of retention to save some computation time
      if (bucketId >= smallestTimeBucketIdToKeep) {
        _timeBucketToPrimaryKeyToSegmentMap.computeIfPresent(bucketId, (timeBucketId, primaryKeyToSegmentMap) -> {
          primaryKeyToSegmentMap.computeIfPresent(primaryKeyHash, (primaryKey, currentSegment) -> {
            if (currentSegment == segment) {
              return null;
            } else {
              return currentSegment;
            }
          });
          if (primaryKeyToSegmentMap.isEmpty()) {
            return null;
          } else {
            return primaryKeyToSegmentMap;
          }
        });
      }
    }
  }

  @Override
  public int removeExpiredPrimaryKeys() {
    long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - _retentionBucketCount;
    for (long timeBucketId : _timeBucketToPrimaryKeyToSegmentMap.keySet()) {
      if (timeBucketId < smallestTimeBucketIdToKeep) {
        _timeBucketToPrimaryKeyToSegmentMap.remove(timeBucketId);
      }
    }
    int numberOfRemainedPrimaryKeys = 0;
    for (ConcurrentHashMap<Object, IndexSegment> primaryKeyToSegmentMap
        : _timeBucketToPrimaryKeyToSegmentMap.values()) {
      numberOfRemainedPrimaryKeys += primaryKeyToSegmentMap.size();
    }
    return numberOfRemainedPrimaryKeys;
  }

  @Override
  public boolean dropOrAddRecord(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
    double metadataTime = dedupRecordInfo.getDedupTime();
    long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
    if (bucketId < _largestSeenTimeBucketId.get() - _retentionBucketCount) {
      // Skip the primary key if the time is out of retention
      return true;
    }
    Object primaryKeyHash = HashUtils.hashPrimaryKey(pk, _hashFunction);
    // advance the largest seen time bucket id no matter the primary key is already present or not
    _largestSeenTimeBucketId.getAndUpdate(largestSeenTimeBucketId -> Math.max(largestSeenTimeBucketId, bucketId));
    boolean primaryKeyPresent = false;
    long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - _retentionBucketCount;
    // iterator the time buckets to check if the primary key is already present
    for (Map.Entry<Long, ConcurrentHashMap<Object, IndexSegment>> entry
        : _timeBucketToPrimaryKeyToSegmentMap.entrySet()) {
      long timeBucketId = entry.getKey();
      // only need to check the primary key in the retention buckets
      if (timeBucketId >= smallestTimeBucketIdToKeep) {
        ConcurrentHashMap<Object, IndexSegment> primaryKeyToSegmentMap = entry.getValue();
        if (primaryKeyToSegmentMap.containsKey(primaryKeyHash)) {
          primaryKeyPresent = true;
          break;
        }
      }
    }
    if (!primaryKeyPresent) {
      primaryKeyPresent = _timeBucketToPrimaryKeyToSegmentMap.computeIfAbsent(bucketId, k -> new ConcurrentHashMap<>())
          .putIfAbsent(primaryKeyHash, indexSegment) != null;
    }
    return primaryKeyPresent;
  }

  @VisibleForTesting
  long getTimeBucketId(double time, double metadataTTL) {
    return (long) (time / metadataTTL * _retentionBucketCount);
  }
}
