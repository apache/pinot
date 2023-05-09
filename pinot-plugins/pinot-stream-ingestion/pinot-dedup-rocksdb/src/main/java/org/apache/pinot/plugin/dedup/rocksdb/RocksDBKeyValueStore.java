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
package org.apache.pinot.plugin.dedup.rocksdb;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.ingestion.dedup.LocalKeyValueStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBKeyValueStore implements LocalKeyValueStore {
  private static final RocksDB ROCKS_DB = initRocksDB();

  private final ColumnFamilyHandle _columnFamilyHandle;

  private static RocksDB initRocksDB() {
      RocksDB.loadLibrary();
      final Options options = new Options();
      options.setCreateIfMissing(true);
      File dbDir;
    try {
      dbDir = Files.createTempDirectory("dedup-data").toFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
        return RocksDB.open(options, dbDir.getAbsolutePath());
      } catch (RocksDBException ex) {
        throw new RuntimeException(ex);
      }
  }

  public RocksDBKeyValueStore(byte[] id) {
    try {
      _columnFamilyHandle = ROCKS_DB.createColumnFamily(new ColumnFamilyDescriptor(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    try {
      return ROCKS_DB.get(_columnFamilyHandle, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(byte[] key) {
    try {
      ROCKS_DB.delete(_columnFamilyHandle, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(byte[] key, byte[] value) {
    try {
      ROCKS_DB.put(_columnFamilyHandle, key, value);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putBatch(List<Pair<byte[], byte[]>> keyValues) {
    WriteBatch writeBatch = new WriteBatch();
    try {
      for (Pair<byte[], byte[]> pair : keyValues) {
        writeBatch.put(_columnFamilyHandle, pair.getKey(), pair.getValue());
      }
      RocksDBKeyValueStore.ROCKS_DB.write(new WriteOptions(), writeBatch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getKeyCount() {
    try {
      return ROCKS_DB.getLongProperty(_columnFamilyHandle, "rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @VisibleForTesting
  public void compact()
      throws RocksDBException {
    ROCKS_DB.compactRange(_columnFamilyHandle);
  }
}
