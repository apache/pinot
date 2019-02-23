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
import com.linkedin.pinot.opal.common.utils.CommonUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksMemEnv;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class RocksDBKeyValueStoreDB implements KeyValueStoreDB<byte[], byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKeyValueStoreDB.class);

  private String _DBBasePath;
  private Options _rocksDBOptions;
  private WriteOptions _writeOptions;
  private ReadOptions _readOptions;
  private final ConcurrentMap<String, RocksDBKeyValueStoreTable> _rocksDBTables = new ConcurrentHashMap<>();

  @Override
  public void init(Configuration configuration) {
    CommonUtils.printConfiguration(configuration, "key value store");
    _DBBasePath = configuration.getString(RocksDBConfig.DATABASE_DIR);
    LOGGER.info("rocksdb config {}", _DBBasePath);
    Preconditions.checkState(StringUtils.isNotEmpty(_DBBasePath), "db path should not be empty");
    _rocksDBOptions = getDBOptions(configuration);
    _writeOptions = getWriteOptions(configuration);
    _readOptions = getReadOptions(configuration);
  }

  @Override
  public KeyValueStoreTable<byte[], byte[]> getTable(String tableName) {
    return _rocksDBTables.computeIfAbsent(tableName, t -> {
      LOGGER.info("adding table {}", tableName);
      String path = getPathForTable(t);
      try {
        return new RocksDBKeyValueStoreTable(_DBBasePath, _rocksDBOptions, _readOptions, _writeOptions);
      } catch (IOException e) {
        throw new RuntimeException("failed to open rocksdb for path " + path, e);
      }
    });
  }

  @Override
  public void deleteTable(String tableName) {
    LOGGER.info("dropping table {}", tableName);
    RocksDBKeyValueStoreTable table = _rocksDBTables.remove(tableName);
    try {
      table.deleteTable();
    } catch (IOException e) {
      LOGGER.error("failed to delete/move files", e);
    }
  }

  private String getPathForTable(String table) {
    return Paths.get(_DBBasePath, table).toString();
  }

  private Options getDBOptions(Configuration configuration) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    if (configuration.getBoolean(RocksDBConfig.USE_MEMORY_CONFIG, false)) {
      options.setEnv(new RocksMemEnv());
    }
    options.setMemTableConfig(new SkipListMemTableConfig());
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockCacheSize(-1).setCacheNumShardBits(-1);
    options.setTableFormatConfig(tableConfig);

    options.setWriteBufferSize(configuration.getLong(RocksDBConfig.WRITE_BUFFER_SIZE, 4L * SizeUnit.MB));
    options.setMaxWriteBufferNumber(configuration.getInt(RocksDBConfig.MAX_WRITE_BUFFER_NUMBER, 2));
    options.setMaxBackgroundCompactions(configuration.getInt(RocksDBConfig.MAX_BACKGROUND_COMPACTION_THREADS,
        options.maxBackgroundCompactions()));
    options.getEnv().setBackgroundThreads(configuration.getInt(RocksDBConfig.MAX_BACKGROUND_COMPACTION_THREADS,
        options.maxBackgroundCompactions()));
    options.setMaxBackgroundFlushes(configuration.getInt(RocksDBConfig.MAX_BACKGROUND_FLUSH,
        options.maxBackgroundFlushes()));
    options.setMaxBackgroundJobs(configuration.getInt(RocksDBConfig.MAX_OPEN_FILES,
        options.maxOpenFiles()));
    options.setUseFsync(configuration.getBoolean(RocksDBConfig.USE_FSYNC, false));
    options.setDeleteObsoleteFilesPeriodMicros(configuration.getLong(RocksDBConfig.DELETE_OBSOLETE_FILES_PERIOD,
        0));
    options.setTableCacheNumshardbits(configuration.getInt(RocksDBConfig.TABLE_NUM_SHARD, 4));
    options.setAllowMmapReads(configuration.getBoolean(RocksDBConfig.MMAP_READ, false));
    options.setAllowMmapWrites(configuration.getBoolean(RocksDBConfig.MMAP_WRITE, false));
    options.setAdviseRandomOnOpen(configuration.getBoolean(RocksDBConfig.ADVICE_ON_RANDOM, false));
    options.setNumLevels(configuration.getInt(RocksDBConfig.NUM_LEVELS, 7));

    // other default options, can be add to config later
    // from rocks db performance benchmark suit
    options.setBloomLocality(0);
    options.setTargetFileSizeBase(10 * 1048576);
    options.setMaxBytesForLevelBase(10 * 1048576);
    options.setLevelZeroStopWritesTrigger(12);
    options.setLevelZeroSlowdownWritesTrigger(8);
    options.setLevelZeroFileNumCompactionTrigger(4);
    options.setMaxCompactionBytes(0);
    options.setDisableAutoCompactions(false);
    options.setMaxSuccessiveMerges(0);
    options.setWalTtlSeconds(0);
    options.setWalSizeLimitMB(0);

    LOGGER.info("starting with options {}", options.toString());
    return options;
  }

  private WriteOptions getWriteOptions(Configuration configuration) {
    WriteOptions writeOptions = new WriteOptions();
    writeOptions.setSync(configuration.getBoolean(RocksDBConfig.WRITE_SYNC, true));
    writeOptions.setDisableWAL(configuration.getBoolean(RocksDBConfig.WRITE_DISABLE_WAL, false));
    return writeOptions;
  }

  private ReadOptions getReadOptions(Configuration configuration) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setVerifyChecksums(configuration.getBoolean(RocksDBConfig.READ_VERIFY_CHECKSUM, false));
    readOptions.setVerifyChecksums(configuration.getBoolean(RocksDBConfig.READ_USE_TAILING, false));
    return readOptions;
  }

}
