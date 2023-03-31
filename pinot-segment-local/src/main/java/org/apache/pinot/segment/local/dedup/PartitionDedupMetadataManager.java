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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;


public class PartitionDedupMetadataManager {
  @VisibleForTesting
  static final String DEDUP_DATA_DIR = "/tmp/dudup-data";
  @VisibleForTesting
  static final RocksDB ROCKS_DB = initRocksDB();
  private final String _tableNameWithType;
  private final List<String> _primaryKeyColumns;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  @VisibleForTesting
  final ColumnFamilyHandle _columnFamilyHandle;

  private static RocksDB initRocksDB() {
      RocksDB.loadLibrary();
      final Options options = new Options();
      options.setCreateIfMissing(true);
      File dbDir = new File(DEDUP_DATA_DIR);
      try {
        Files.createDirectories(dbDir.getParentFile().toPath());
        Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        return RocksDB.open(options, dbDir.getAbsolutePath());
      } catch (IOException | RocksDBException ex) {
        throw new RuntimeException(ex);
      }
  }

  public PartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns, int partitionId,
      ServerMetrics serverMetrics, HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _primaryKeyColumns = primaryKeyColumns;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
    try {
      _columnFamilyHandle = ROCKS_DB.createColumnFamily(
          new ColumnFamilyDescriptor((tableNameWithType + "@" + partitionId).getBytes()));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void addSegment(IndexSegment segment) {
    // Add all PKs to the rocksdb column family
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    WriteBatch writeBatch = new WriteBatch();
    byte[] serializedSegment = serializeSegment(segment);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      byte[] serializedPrimaryKey = serializePrimaryKey(HashUtils.hashPrimaryKey(pk, _hashFunction));
      try {
        writeBatch.put(_columnFamilyHandle, serializedPrimaryKey, serializedSegment);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      ROCKS_DB.write(new WriteOptions(), writeBatch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        getKeyCount());
  }

  @VisibleForTesting
  long getKeyCount() {
    try {
    return ROCKS_DB.getLongProperty(_columnFamilyHandle, "rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
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
    throw new RuntimeException();
  }

  public void removeSegment(IndexSegment segment) {
    // TODO(saurabh): Explain reload scenario here
    Iterator<PrimaryKey> primaryKeyIterator = getPrimaryKeyIterator(segment);
    byte[] segmentBytes = serializeSegment(segment);
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey pk = primaryKeyIterator.next();
      byte[] pkBytes = serializePrimaryKey(pk);
      try {
        if (Objects.deepEquals(ROCKS_DB.get(_columnFamilyHandle, pkBytes), segmentBytes)) {
          ROCKS_DB.delete(_columnFamilyHandle, pkBytes);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
        getKeyCount());
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
    byte[] keyBytes = serializePrimaryKey(HashUtils.hashPrimaryKey(pk, _hashFunction));
    try {
      if (Objects.isNull(ROCKS_DB.get(_columnFamilyHandle, keyBytes))) {
          ROCKS_DB.put(_columnFamilyHandle, keyBytes, serializeSegment(indexSegment));
        _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT,
            getKeyCount());
        return false;
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    return true;
  }
}
