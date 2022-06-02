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
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionDedupMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDedupMetadataManager.class);
  private static boolean _allSegmentsLoaded;

  private final HelixManager _helixManager;
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  @VisibleForTesting
  final ConcurrentHashMap<Object, IndexSegment> _primaryKeyToSegmentMap = new ConcurrentHashMap<>();

  public PartitionDedupMetadataManager(HelixManager helixManager, String tableNameWithType,
      List<String> primaryKeyColumns, int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction) {
    _helixManager = helixManager;
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
  }

  public void addSegment(IndexSegment segment) {
    // Add all PKs to _primaryKeyToSegmentMap
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment, _primaryKeyColumns);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      _primaryKeyToSegmentMap.put(HashUtils.hashPrimaryKey(pk, _hashFunction), segment);
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentMap.size());
  }

  public void removeSegment(IndexSegment segment) {
    // TODO(saurabh): Explain reload scenario here
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment, _primaryKeyColumns);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      _primaryKeyToSegmentMap.compute(HashUtils.hashPrimaryKey(pk, _hashFunction),
          (primaryKey, currentSegment) -> {
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
  public static Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment, List<String> primaryKeyColumns) {
    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = primaryKeyColumns.size();
    return new Iterator<PrimaryKey>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public PrimaryKey next() {
        Object[] values = new Object[numPrimaryKeyColumns];
        for (int i = 0; i < numPrimaryKeyColumns; i++) {
          Object value = columnToReaderMap.get(primaryKeyColumns.get(i)).getValue(_docId);
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

  private synchronized void waitTillAllSegmentsLoaded() {
    if (_allSegmentsLoaded) {
      return;
    }

    while (!TableStateUtils.isAllSegmentsLoaded(_helixManager, _tableNameWithType)) {
      LOGGER.info("Sleeping 1 second waiting for all segments loaded for partial-upsert table: {}", _tableNameWithType);
      try {
        //noinspection BusyWait
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    _allSegmentsLoaded = true;
  }

  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    if (!_allSegmentsLoaded) {
      waitTillAllSegmentsLoaded();
    }

    boolean result =
        (_primaryKeyToSegmentMap.putIfAbsent(HashUtils.hashPrimaryKey(pk, _hashFunction), indexSegment)
            != null);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _primaryKeyToSegmentMap.size());

    return result;
  }
}
