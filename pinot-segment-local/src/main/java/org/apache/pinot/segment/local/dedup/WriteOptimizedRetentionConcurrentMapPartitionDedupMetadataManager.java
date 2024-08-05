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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;


class WriteOptimizedRetentionConcurrentMapPartitionDedupMetadataManager
    extends BaseRetentionPartitionDedupMetadataManager {
  private static final int DEFAULT_RETENTION_BUCKET_COUNT = 100;

  @VisibleForTesting
  final AtomicLong _largestSeenTimeBucketId = new AtomicLong(0);
  @VisibleForTesting
  final ConcurrentHashMap<Object, Pair<IndexSegment, Long>> _primaryKeyToSegmentAndTimeMap = new ConcurrentHashMap<>();
  // Map from bucket id to the set of primary key hashes, used for quickly removing expired primary keys
  // without this map, we would need to iterate over all primary keys to check if they are expired or not, which may be
  // expensive when there are a lot of primary keys
  @VisibleForTesting
  final ConcurrentHashMap<Long, Set<Object>> _bucketIdToPrimaryKeySetMap = new ConcurrentHashMap<>();
  // Number of retention buckets to keep
  @VisibleForTesting
  private final int _retentionBucketCount;

  public WriteOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction,
      double metadataTTL, String metadataTimeColumn) {
    this(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction, metadataTTL,
        metadataTimeColumn, DEFAULT_RETENTION_BUCKET_COUNT);
  }

  @VisibleForTesting
  WriteOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction,
      double metadataTTL, String metadataTimeColumn, int retentionBucketCount) {
    super(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, hashFunction, metadataTTL,
        metadataTimeColumn);
    _retentionBucketCount = retentionBucketCount;
  }

  @VisibleForTesting
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
        // just in case the same primary key is added multiple times before
        if (_primaryKeyToSegmentAndTimeMap.putIfAbsent(primaryKeyHash, Pair.of(segment, bucketId)) == null) {
          _bucketIdToPrimaryKeySetMap.computeIfAbsent(bucketId, k -> ConcurrentHashMap.newKeySet()).add(primaryKeyHash);
        }
      }
    }
  }

  @VisibleForTesting
  @Override
  void removeSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
      double metadataTime = dedupRecordInfo.getDedupTime();
      long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
      Object primaryKeyHash = HashUtils.hashPrimaryKey(pk, _hashFunction);
      if (_primaryKeyToSegmentAndTimeMap.computeIfPresent(primaryKeyHash, (primaryKey, currentSegmentAndTime) -> {
        if (currentSegmentAndTime.getLeft() == segment && Objects.equals(currentSegmentAndTime.getRight(), bucketId)) {
          // Remove the primary key if it's associated with the segment and time
          return null;
        } else {
          return currentSegmentAndTime;
        }
      }) == null) {
        _bucketIdToPrimaryKeySetMap.computeIfPresent(bucketId, (k, primaryKeySet) -> {
          primaryKeySet.remove(primaryKeyHash);
          if (primaryKeySet.isEmpty()) {
            // Remove the bucket if it's empty
            return null;
          } else {
            return primaryKeySet;
          }
        });
      }
    }
  }

  @Override
  public int removeExpiredPrimaryKeys() {
    long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - _retentionBucketCount;
    _bucketIdToPrimaryKeySetMap.forEach((bucketId, primaryKeySet) -> {
      if (bucketId < smallestTimeBucketIdToKeep) {
        if (primaryKeySet != null) {
          primaryKeySet.forEach(primaryKeyHash -> _primaryKeyToSegmentAndTimeMap.computeIfPresent(primaryKeyHash,
              (primaryKey, currentSegmentAndTime) -> {
                if (Objects.equals(currentSegmentAndTime.getRight(), bucketId)) {
                  return null;
                } else {
                  return currentSegmentAndTime;
                }
              }));
        }
        _bucketIdToPrimaryKeySetMap.remove(bucketId);
      }
    });
    return _primaryKeyToSegmentAndTimeMap.size();
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
    boolean previouslyPresent =
        _primaryKeyToSegmentAndTimeMap.putIfAbsent(primaryKeyHash, Pair.of(indexSegment, bucketId)) != null;
    if (!previouslyPresent) {
      _bucketIdToPrimaryKeySetMap.computeIfAbsent(bucketId, k -> ConcurrentHashMap.newKeySet()).add(primaryKeyHash);
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          _primaryKeyToSegmentAndTimeMap.size());
    }
    return previouslyPresent;
  }

  @VisibleForTesting
  long getTimeBucketId(double time, double metadataTTL) {
    return (long) (time / metadataTTL * _retentionBucketCount);
  }
}
