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
package org.apache.pinot.core.data.manager.ingest;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.ShardLog;
import org.apache.pinot.spi.ingest.ShardLogProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Local file-based implementation of {@link ShardLogProvider}.
 *
 * <p>Creates and manages {@link LocalShardLog} instances, one per table-partition combination.
 * All log directories are stored under {@code <dataDir>/insert/logs/<tableNameWithType>/<partitionId>/}.
 *
 * <p>This implementation is thread-safe. The same {@link LocalShardLog} instance is returned for
 * repeated calls with the same table and partition arguments.
 */
public class LocalShardLogProvider implements ShardLogProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalShardLogProvider.class);
  private static final String LOGS_DIR = "logs";

  private File _logsBaseDir;
  private final ConcurrentMap<String, LocalShardLog> _shardLogs = new ConcurrentHashMap<>();

  @Override
  public void init(PinotConfiguration config) {
    String dataDir = config.getProperty("dataDir", config.getProperty("pinot.server.instance.dataDir", "/tmp/pinot"));
    _logsBaseDir = new File(new File(dataDir, "insert"), LOGS_DIR);
    if (!_logsBaseDir.exists() && !_logsBaseDir.mkdirs()) {
      throw new RuntimeException("Failed to create logs base directory: " + _logsBaseDir);
    }
    LOGGER.info("Initialized LocalShardLogProvider with logs directory: {}", _logsBaseDir);
  }

  /**
   * Initializes the provider with an explicit base directory. Useful for testing.
   *
   * @param logsBaseDir the base directory for shard log files
   */
  public void init(File logsBaseDir) {
    _logsBaseDir = logsBaseDir;
    if (!_logsBaseDir.exists() && !_logsBaseDir.mkdirs()) {
      throw new RuntimeException("Failed to create logs base directory: " + _logsBaseDir);
    }
  }

  @Override
  public ShardLog getShardLog(String tableNameWithType, int partitionId) {
    String key = tableNameWithType + "/" + partitionId;
    return _shardLogs.computeIfAbsent(key, k -> {
      File logDir = new File(_logsBaseDir, tableNameWithType + File.separator + partitionId);
      try {
        return new LocalShardLog(logDir);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create LocalShardLog for " + key, e);
      }
    });
  }

  @Override
  public void close()
      throws IOException {
    _shardLogs.clear();
    LOGGER.info("Closed LocalShardLogProvider");
  }
}
