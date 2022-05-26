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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.local.utils.tablestate.TableState;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionDedupMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

  private final String _tableNameWithType;
  private final TableState _tableState;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  // TODO(saurabh) : We can replace this with a ocncurrent Set
  @VisibleForTesting
  final ConcurrentHashMap<Object, Boolean> _primaryKeySet = new ConcurrentHashMap<>();

  public PartitionDedupMetadataManager(String tableNameWithType, TableState tableState, List<String> primaryKeyColumns,
      int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _tableState = tableState;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
  }

  public void addSegment(IndexSegment segment) {
    // Add all PKs to _primaryKeySet
    Iterator<RecordInfo> recordInfoIterator = getRecordInfoIterator(segment);
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeySet.put(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction), true);
    }
  }

  public void removeSegment(IndexSegment segment) {
    // Remove all PKs from _primaryKeySet
    Iterator<RecordInfo> recordInfoIterator = getRecordInfoIterator(segment);
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeySet.remove(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction));
    }
  }

  private Iterator<RecordInfo> getRecordInfoIterator(IndexSegment segment) {
    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : _primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = _primaryKeyColumns.size();
    return new Iterator<RecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public RecordInfo next() {
        Object[] values = new Object[numPrimaryKeyColumns];
        for (int i = 0; i < numPrimaryKeyColumns; i++) {
          Object value = columnToReaderMap.get(_primaryKeyColumns.get(i)).getValue(_docId);
          if (value instanceof byte[]) {
            value = new ByteArray((byte[]) value);
          }
          values[i] = value;
        }
        PrimaryKey primaryKey = new PrimaryKey(values);
        return new RecordInfo(primaryKey, _docId++, null);
      }
    };
  }


  public boolean checkRecordPresentOrUpdate(RecordInfo recordInfo) {
    while (!_tableState.isAllSegmentsLoaded()) {
      LOGGER.info("Sleeping 1 second waiting for all segments loaded for partial-upsert table: {}", _tableNameWithType);
      try {
        //noinspection BusyWait
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    Boolean isPresent =
        _primaryKeySet.putIfAbsent(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction), true);
    return (isPresent != null);
  }
}
