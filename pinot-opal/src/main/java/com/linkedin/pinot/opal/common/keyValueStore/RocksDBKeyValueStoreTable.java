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
package com.linkedin.pinot.opal.common.keyValueStore;

import com.google.common.base.Preconditions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

public class RocksDBKeyValueStoreTable implements KeyValueStoreTable<byte[], byte[]> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKeyValueStoreTable.class);
  private final String _path;
  private final Options _options;
  private final ReadOptions _readOptions;
  private final WriteOptions _writeOptions;
  private final RocksDB _db;

  public RocksDBKeyValueStoreTable(String path, Options options, ReadOptions readOptions, WriteOptions writeOptions)
      throws IOException {
    _path = path;
    _options = options;
    _readOptions = readOptions;
    _writeOptions = writeOptions;
    try {
      _db = RocksDB.open(path);
    } catch (RocksDBException e) {
      throw new IOException("failed to open rocksdb db", e);
    }
  }

  @Override
  public Map<byte[], byte[]> multiGet(List<byte[]> keys) throws IOException {
    try {
      return _db.multiGet(keys);
    } catch (RocksDBException e) {
      throw new IOException("failed to get keys from rocksdb " + _path, e);
    }
  }

  @Override
  public void multiPut(List<byte[]> keys, List<byte[]> values) throws IOException {
    Preconditions.checkState(keys.size() == values.size(),
        "keys size {} does not match values size {}", keys.size(), values.size());
    if (keys.size() == 0) {
      return;
    }
    final WriteBatch batch = new WriteBatch();
    try {
      for (int i = 0; i < keys.size(); i++) {
        batch.put(keys.get(i), values.get(i));
      }
      _db.write(_writeOptions, batch);
    } catch (RocksDBException e) {
      throw new IOException("failed to put data to rocksdb table " + _path, e);
    }

  }

  @Override
  public void deleteTable() throws IOException {
    String backupPath = _path + ".bak";
    Files.delete(Paths.get(backupPath));
    Files.move(Paths.get(_path), Paths.get(backupPath), StandardCopyOption.COPY_ATTRIBUTES);
  }
}
