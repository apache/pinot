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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


class NoRetentionConcurrentMapPartitionDedupMetadataManager extends BasePartitionDedupMetadataManager {
  @VisibleForTesting
  final ConcurrentHashMap<Object, IndexSegment> _primaryKeyToSegmentMap = new ConcurrentHashMap<>();

  public NoRetentionConcurrentMapPartitionDedupMetadataManager(String tableNameWithType, int partitionId,
      DedupContext dedupContext) {
    super(tableNameWithType, partitionId, dedupContext);
  }

  @Override
  public void addSegment(IndexSegment segment) {
    // Add all PKs to _primaryKeyToSegmentMap
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      _primaryKeyToSegmentMap.put(HashUtils.hashPrimaryKey(pk, _hashFunction), segment);
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentMap.size());
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    // TODO(saurabh): Explain reload scenario here
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      _primaryKeyToSegmentMap.compute(HashUtils.hashPrimaryKey(pk, _hashFunction), (primaryKey, currentSegment) -> {
        if (currentSegment == segment) {
          return null;
        } else {
          return currentSegment;
        }
      });
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentMap.size());
  }

  @VisibleForTesting
  Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment) {
    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : _primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = _primaryKeyColumns.size();
    return new Iterator<>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public PrimaryKey next() {
        Object[] values = new Object[numPrimaryKeyColumns];
        for (int i = 0; i < numPrimaryKeyColumns; i++) {
          Object value = columnToReaderMap.get(_primaryKeyColumns.get(i)).getValue(_docId);
          if (value instanceof byte[]) {
            value = new ByteArray((byte[]) value);
          }
          values[i] = value;
        }
        _docId++;
        return new PrimaryKey(values);
      }
    };
  }

  @Override
  public int removeExpiredPrimaryKeys() {
    _logger.warn(
        "removeExpiredPrimaryKeys() is not supported for NoRetentionConcurrentMapPartitionDedupMetadataManager");
    return -1;
  }

  @Override
  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    boolean present =
        _primaryKeyToSegmentMap.putIfAbsent(HashUtils.hashPrimaryKey(pk, _hashFunction), indexSegment) != null;
    if (!present) {
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          _primaryKeyToSegmentMap.size());
    }
    return present;
  }
}
