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
import com.google.common.base.Preconditions;
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


class RetentionConcurrentMapPartitionDedupMetadataManager implements PartitionDedupMetadataManager {
  // Number of retention buckets to keep
  private static final int RETENTION_BUCKET_COUNT = 100;
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;
  private final double _metadataTTL;
  private final String _metadataTimeColumn;

  @VisibleForTesting
  final AtomicLong _largestSeenTimeBucketId = new AtomicLong(0);
  @VisibleForTesting
  final ConcurrentHashMap<Object, Pair<IndexSegment, Long>> _primaryKeyToSegmentAndTimeMap = new ConcurrentHashMap<>();
  // Map from bucket id to the set of primary key hashes, used for quickly removing expired primary keys
  // without this map, we would need to iterate over all primary keys to check if they are expired or not, which may be
  // expensive when there are a lot of primary keys
  @VisibleForTesting
  final ConcurrentHashMap<Long, Set<Object>> _bucketIdToPrimaryKeySetMap = new ConcurrentHashMap<>();

  public RetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns,
      int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction, double metadataTTL,
      String metadataTimeColumn) {
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
    Preconditions.checkArgument(metadataTTL > 0, "metadataTTL: %s for table: %s must be positive when "
        + "RetentionConcurrentMapPartitionDedupMetadataManager is used", metadataTTL, tableNameWithType);
    _metadataTTL = metadataTTL;
    Preconditions.checkArgument(metadataTimeColumn != null,
        "When metadataTTL is configured, metadata time column must be configured for dedup enabled table: %s",
        tableNameWithType);
    _metadataTimeColumn = metadataTimeColumn;
  }

  @Override
  public void addSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _metadataTimeColumn)) {
      addSegment(segment, dedupRecordInfoReader);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while adding segment: %s of table: %s to "
          + "RetentionConcurrentMapPartitionDedupMetadataManager", segment.getSegmentName(), _tableNameWithType), e);
    }
  }

  @VisibleForTesting
  void addSegment(IndexSegment segment, DedupUtils.DedupRecordInfoReader dedupRecordInfoReader) {
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
      double metadataTime = dedupRecordInfo.getDedupTime();
      long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
      // Skip the primary key if the time is out of retention
      if (bucketId >= _largestSeenTimeBucketId.get() - RETENTION_BUCKET_COUNT) {
        // advance the largest seen time bucket id no matter the primary key is already present or not
        _largestSeenTimeBucketId.getAndUpdate(largestSeenTimeBucketId -> Math.max(largestSeenTimeBucketId, bucketId));
        Object primaryKeyHash = HashUtils.hashPrimaryKey(pk, _hashFunction);
        // just in case the same primary key is added multiple times before
        if (_primaryKeyToSegmentAndTimeMap.putIfAbsent(primaryKeyHash, Pair.of(segment, bucketId)) == null) {
          _bucketIdToPrimaryKeySetMap.computeIfAbsent(bucketId, k -> ConcurrentHashMap.newKeySet()).add(primaryKeyHash);
        }
      }
    }
    removeExpiredPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentAndTimeMap.size());
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    try (DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = new DedupUtils.DedupRecordInfoReader(segment,
        _primaryKeyColumns, _metadataTimeColumn)) {
      removeSegment(segment, dedupRecordInfoReader);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while removing segment: %s of table: %s from "
          + "RetentionConcurrentMapPartitionDedupMetadataManager", segment.getSegmentName(), _tableNameWithType), e);
    }
  }

  @VisibleForTesting
  void removeSegment(IndexSegment segment, DedupUtils.DedupRecordInfoReader dedupRecordInfoReader) {
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, segment.getSegmentMetadata().getTotalDocs());
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
    removeExpiredPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentAndTimeMap.size());
  }

  @Override
  public void removeExpiredPrimaryKeys() {
    long smallestTimeBucketIdToKeep = _largestSeenTimeBucketId.get() - RETENTION_BUCKET_COUNT;
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
  }

  @Override
  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    throw new UnsupportedOperationException("checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) is "
        + "not supported for RetentionConcurrentMapPartitionDedupMetadataManager");
  }

  @Override
  public boolean dropOrAddRecord(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    PrimaryKey pk = dedupRecordInfo.getPrimaryKey();
    double metadataTime = dedupRecordInfo.getDedupTime();
    long bucketId = getTimeBucketId(metadataTime, _metadataTTL);
    if (bucketId < _largestSeenTimeBucketId.get() - RETENTION_BUCKET_COUNT) {
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
  static long getTimeBucketId(double time, double metadataTTL) {
    return (long) (time / metadataTTL * RETENTION_BUCKET_COUNT);
  }
}
