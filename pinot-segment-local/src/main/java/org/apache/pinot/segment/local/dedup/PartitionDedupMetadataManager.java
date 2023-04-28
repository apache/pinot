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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.ingestion.dedup.LocalKeyValueStore;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.ByteArray;


public class PartitionDedupMetadataManager {
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final DedupConfig _dedupConfig;
  @VisibleForTesting final LocalKeyValueStore _keyValueStore;

  public PartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns, int partitionId,
      ServerMetrics serverMetrics, DedupConfig dedupConfig) {
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _dedupConfig = dedupConfig;
    try {
      byte[] id = (tableNameWithType + "@" + partitionId).getBytes();
      _keyValueStore = StringUtils.isEmpty(dedupConfig.getKeyStore())
              ? new ConcurrentHashMapKeyValueStore(id)
              : PluginManager.get().createInstance(
                      dedupConfig.getKeyStore(),
                      new Class[]{byte[].class},
                      new Object[]{id}
              );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void addSegment(IndexSegment segment) {
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    byte[] serializedSegment = serializeSegment(segment);
    List<Pair<byte[], byte[]>> keyValues = new ArrayList<>();
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      byte[] serializedPrimaryKey = serializePrimaryKey(HashUtils.hashPrimaryKey(pk, _dedupConfig.getHashFunction()));
      keyValues.add(Pair.of(serializedPrimaryKey, serializedSegment));
    }
    _keyValueStore.putBatch(keyValues);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _keyValueStore.getKeyCount());
  }

  @VisibleForTesting
  static byte[] serializeSegment(IndexSegment segment) {
    return segment.getSegmentName().getBytes();
  }

  @VisibleForTesting
  static byte[] serializePrimaryKey(Object pk) {
    if (pk instanceof PrimaryKey) {
      return ((PrimaryKey) pk).asBytes();
    }
    if (pk instanceof ByteArray) {
      return ((ByteArray) pk).getBytes();
    }
    throw new RuntimeException("Invalid primary key: " + pk);
  }

  public void removeSegment(IndexSegment segment) {
    // TODO(saurabh): Explain reload scenario here
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    byte[] segmentBytes = serializeSegment(segment);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      byte[] pkBytes = serializePrimaryKey(pk);
      if (Objects.deepEquals(_keyValueStore.get(pkBytes), segmentBytes)) {
        _keyValueStore.delete(pkBytes);
      }
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        _keyValueStore.getKeyCount());
  }

  @VisibleForTesting
  Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment) {
    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : _primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = _primaryKeyColumns.size();
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

  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    byte[] keyBytes = serializePrimaryKey(HashUtils.hashPrimaryKey(pk, _dedupConfig.getHashFunction()));
    if (Objects.isNull(_keyValueStore.get(keyBytes))) {
        _keyValueStore.put(keyBytes, serializeSegment(indexSegment));
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
          _keyValueStore.getKeyCount());
      return false;
    }
    return true;
  }
}
