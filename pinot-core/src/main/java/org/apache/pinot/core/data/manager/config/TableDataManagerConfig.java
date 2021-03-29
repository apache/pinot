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
package org.apache.pinot.core.data.manager.config;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The config used for TableDataManager.
 */
public class TableDataManagerConfig {
  private static final String TABLE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String TABLE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String TABLE_DATA_MANAGER_CONSUMER_DIRECTORY = "consumerDirectory";
  private static final String TABLE_DATA_MANAGER_NAME = "name";
  private static final String TABLE_IS_DIMENSION = "isDimTable";
  private static final String TABLE_DATA_MANGER_AUTH_TOKEN = "authToken";

  private final Configuration _tableDataManagerConfig;

  public TableDataManagerConfig(@Nonnull Configuration tableDataManagerConfig) {
    _tableDataManagerConfig = tableDataManagerConfig;
  }

  public Configuration getConfig() {
    return _tableDataManagerConfig;
  }

  public String getTableDataManagerType() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_TYPE);
  }

  public String getDataDir() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_DATA_DIRECTORY);
  }

  public String getConsumerDir() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_CONSUMER_DIRECTORY);
  }

  public String getTableName() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANAGER_NAME);
  }

  public boolean isDimTable() {
    return _tableDataManagerConfig.getBoolean(TABLE_IS_DIMENSION);
  }

  public String getAuthToken() {
    return _tableDataManagerConfig.getString(TABLE_DATA_MANGER_AUTH_TOKEN);
  }

  public static TableDataManagerConfig getDefaultHelixTableDataManagerConfig(
      @Nonnull InstanceDataManagerConfig instanceDataManagerConfig, @Nonnull String tableNameWithType) {
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(TABLE_DATA_MANAGER_NAME, tableNameWithType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_DATA_DIRECTORY,
        instanceDataManagerConfig.getInstanceDataDir() + "/" + tableNameWithType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_CONSUMER_DIRECTORY, instanceDataManagerConfig.getConsumerDir());
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    Preconditions.checkNotNull(tableType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_TYPE, tableType.name());
    defaultConfig.addProperty(TABLE_DATA_MANGER_AUTH_TOKEN, instanceDataManagerConfig.getAuthToken());

    return new TableDataManagerConfig(defaultConfig);
  }

  public void overrideConfigs(@Nonnull TableConfig tableConfig, String authToken) {
    // Override table level configs

    _tableDataManagerConfig.addProperty(TABLE_IS_DIMENSION, tableConfig.isDimTable());
    _tableDataManagerConfig.addProperty(TABLE_DATA_MANGER_AUTH_TOKEN, authToken);

    // If we wish to override some table level configs using table config, override them here
    // Note: the configs in TableDataManagerConfig is immutable once the table is created, which mean it will not pick
    // up the latest table config
  }
}
