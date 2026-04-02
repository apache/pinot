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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for creating {@link RowInsertExecutor} instances.
 *
 * <p>This factory handles the "ROW" executor type. It initializes a {@link LocalShardLogProvider}
 * and {@link LocalPreparedStore} during initialization, and creates executors that use these
 * shared infrastructure components.
 *
 * <p>This class is thread-safe.
 */
public class RowInsertExecutorFactory implements InsertExecutorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowInsertExecutorFactory.class);
  private static final String EXECUTOR_TYPE = "ROW";

  private LocalShardLogProvider _shardLogProvider;
  private LocalPreparedStore _preparedStore;
  private ConcurrentHashMap<String, TableConfig> _tableConfigs;

  @Override
  public String getExecutorType() {
    return EXECUTOR_TYPE;
  }

  @Override
  public void init(PinotConfiguration config) {
    _shardLogProvider = new LocalShardLogProvider();
    _shardLogProvider.init(config);

    String dataDir = config.getProperty("dataDir", config.getProperty("pinot.server.instance.dataDir", "/tmp/pinot"));
    _preparedStore = new LocalPreparedStore();
    _preparedStore.init(config, new File(dataDir));

    _tableConfigs = new ConcurrentHashMap<>();

    LOGGER.info("Initialized RowInsertExecutorFactory");
  }

  @Override
  public InsertExecutor create(PinotConfiguration config) {
    return new RowInsertExecutor(_shardLogProvider, _preparedStore, _tableConfigs);
  }

  /**
   * Registers a table configuration. Must be called when a table is created or updated.
   *
   * @param tableNameWithType the fully qualified table name
   * @param tableConfig the table configuration
   */
  public void registerTableConfig(String tableNameWithType, TableConfig tableConfig) {
    _tableConfigs.put(tableNameWithType, tableConfig);
  }

  /**
   * Removes a table configuration. Should be called when a table is dropped.
   *
   * @param tableNameWithType the fully qualified table name
   */
  public void removeTableConfig(String tableNameWithType) {
    _tableConfigs.remove(tableNameWithType);
  }

  /**
   * Returns the shard log provider used by this factory.
   */
  public LocalShardLogProvider getShardLogProvider() {
    return _shardLogProvider;
  }

  /**
   * Returns the prepared store used by this factory.
   */
  public LocalPreparedStore getPreparedStore() {
    return _preparedStore;
  }
}
