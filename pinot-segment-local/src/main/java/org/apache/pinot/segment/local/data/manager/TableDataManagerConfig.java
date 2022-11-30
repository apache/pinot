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
package org.apache.pinot.segment.local.data.manager;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The config used for TableDataManager.
 */
public class TableDataManagerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataManagerConfig.class);

  private static final String TABLE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String TABLE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String TABLE_DATA_MANAGER_CONSUMER_DIRECTORY = "consumerDirectory";
  private static final String TABLE_DATA_MANAGER_NAME = "name";
  private static final String TABLE_IS_DIMENSION = "isDimTable";
  private static final String TABLE_DATA_MANAGER_AUTH = "auth";
  private static final String TABLE_DELETED_SEGMENTS_CACHE_SIZE = "deletedSegmentsCacheSize";
  private static final String TABLE_DELETED_SEGMENTS_CACHE_TTL_MINUTES = "deletedSegmentsCacheTTL";
  private static final String TABLE_PEER_DOWNLOAD_SCHEME = "peerDownloadScheme";

  private final Configuration _tableDataManagerConfig;

  public TableDataManagerConfig(Configuration tableDataManagerConfig) {
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

  public Configuration getAuthConfig() {
    return _tableDataManagerConfig.subset(TABLE_DATA_MANAGER_AUTH);
  }

  public int getTableDeletedSegmentsCacheSize() {
    return _tableDataManagerConfig.getInt(TABLE_DELETED_SEGMENTS_CACHE_SIZE);
  }

  public int getTableDeletedSegmentsCacheTtlMinutes() {
    return _tableDataManagerConfig.getInt(TABLE_DELETED_SEGMENTS_CACHE_TTL_MINUTES);
  }

  public String getTablePeerDownloadScheme() {
    return _tableDataManagerConfig.getString(TABLE_PEER_DOWNLOAD_SCHEME);
  }

  public static TableDataManagerConfig getDefaultHelixTableDataManagerConfig(
      InstanceDataManagerConfig instanceDataManagerConfig, String tableNameWithType) {
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(TABLE_DATA_MANAGER_NAME, tableNameWithType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_DATA_DIRECTORY,
        instanceDataManagerConfig.getInstanceDataDir() + "/" + tableNameWithType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_CONSUMER_DIRECTORY, instanceDataManagerConfig.getConsumerDir());
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    Preconditions.checkNotNull(tableType);
    defaultConfig.addProperty(TABLE_DATA_MANAGER_TYPE, tableType.name());
    defaultConfig.addProperty(TABLE_DELETED_SEGMENTS_CACHE_SIZE,
        instanceDataManagerConfig.getDeletedSegmentsCacheSize());
    defaultConfig.addProperty(TABLE_DELETED_SEGMENTS_CACHE_TTL_MINUTES,
        instanceDataManagerConfig.getDeletedSegmentsCacheTtlMinutes());
    // allow null
    String segmentPeerDownloadScheme = instanceDataManagerConfig.getSegmentPeerDownloadScheme();
    LOGGER.info("instance level segment peer download scheme = {}", segmentPeerDownloadScheme);
    defaultConfig.addProperty(TABLE_PEER_DOWNLOAD_SCHEME, segmentPeerDownloadScheme);


    // copy auth-related configs
    instanceDataManagerConfig.getConfig().subset(TABLE_DATA_MANAGER_AUTH).toMap()
        .forEach((key, value) -> defaultConfig.setProperty(TABLE_DATA_MANAGER_AUTH + "." + key, value));

    return new TableDataManagerConfig(defaultConfig);
  }

  public void overrideConfigs(TableConfig tableConfig) {
    // Override table level configs

    _tableDataManagerConfig.addProperty(TABLE_IS_DIMENSION, tableConfig.isDimTable());
    SegmentsValidationAndRetentionConfig segmentConfig = tableConfig.getValidationConfig();
    if (segmentConfig != null && segmentConfig.getPeerSegmentDownloadScheme() != null) {
      _tableDataManagerConfig.setProperty(TABLE_PEER_DOWNLOAD_SCHEME, segmentConfig.getPeerSegmentDownloadScheme());
    }
    LOGGER.info("final segment peer download scheme = {}", getTablePeerDownloadScheme());

    // If we wish to override some table level configs using table config, override them here
    // Note: the configs in TableDataManagerConfig is immutable once the table is created, which mean it will not pick
    // up the latest table config
  }
}
