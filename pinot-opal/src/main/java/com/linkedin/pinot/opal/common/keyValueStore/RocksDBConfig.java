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

import org.rocksdb.util.SizeUnit;

public class RocksDBConfig {
  public static final String USE_MEMORY_CONFIG = "rocksdb.env.memory";
  public static final String KEY_SIZE = "rocksdb.key.size";
  public static final String WRITE_BUFFER_SIZE = "rocksdb.write.buffer.size";
  public static final String MAX_WRITE_BUFFER_NUMBER = "rocksdb.write.max.buffer";
  public static final String MAX_BACKGROUND_COMPACTION_THREADS = "rocksdb.compaction.max";
  public static final String MAX_BACKGROUND_FLUSH = "rocksdb.background.flush.max";
  public static final String MAX_BACKGROUND_JOBS = "rocksdb.background.jobs.max";
  public static final String MAX_OPEN_FILES = "rocksdb.files.open.max";
  public static final String USE_FSYNC = "rocksdb.fsync.enable";
  public static final String DELETE_OBSOLETE_FILES_PERIOD = "rocksdb.obsolete.file.delete.micro";
  public static final String TABLE_NUM_SHARD = "rocksdb.table.shard.num";
  public static final String MMAP_READ = "rocksdb.mmap.reads";
  public static final String MMAP_WRITE = "rocksdb.mmap.writes";
  public static final String ADVICE_ON_RANDOM = "rocksdb.advice.random";
  public static final String NUM_LEVELS = "rocksdb.num.levels";

  public static final String DATABASE_DIR = "rocksdb.database.dir";

  public static final String WRITE_SYNC = "rocksdb.writes.sync";
  public static final String WRITE_DISABLE_WAL = "rocksdb.writes.wal.disable";

  public static final String READ_VERIFY_CHECKSUM = "rocksdb.reads.checksum";
  public static final String READ_USE_TAILING = "rocksdb.reads.tailing";

  // default values
  public static final long  DEFAULT_WRITE_BUFFER_SIZE = 4 * SizeUnit.MB;
  public static final int DEFAULT_WRITE_BUFFER_NUM = 2;
}
