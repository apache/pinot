/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.manager.config;

import com.linkedin.pinot.common.segment.ReadMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The config used for InstanceDataManager.
 *
 *
 */
public class FileBasedInstanceDataManagerConfig implements InstanceDataManagerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceDataManagerConfig.class);

  // Key of instance id
  private static final String INSTANCE_ID = "id";
  // Key of instance data directory
  private static final String INSTANCE_DATA_DIR = "dataDir";
  // Key of instance segment tar directory
  private static final String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
  // Key of segment directory
  private static final String INSTANCE_BOOTSTRAP_SEGMENT_DIR = "bootstrap.segment.dir";
  // Key of table names that will be holding from initialization
  private static final String INSTANCE_TABLE_NAME = "tableName";
  // Key of table data directory
  private static final String KEY_OF_TABLE_DATA_DIRECTORY = "directory";
  // Key of table data directory
  private static final String KEY_OF_TABLE_NAME = "name";
  // Key of instance level segment read mode
  private static final String READ_MODE = "readMode";
  // Key of the segment format this server can read
  private static final String SEGMENT_FORMAT_VERSION = "segment.format.version";
  // Key of whether to enable default columns
  private static final String ENABLE_DEFAULT_COLUMNS = "enable.default.columns";
  // Key of whether to enable split commit
  private static final String ENABLE_SPLIT_COMMIT = "enable.split.commit";

  private static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, INSTANCE_TABLE_NAME };
  private Configuration _instanceDataManagerConfiguration = null;
  private Map<String, TableDataManagerConfig> _tableDataManagerConfigMap =
      new HashMap<String, TableDataManagerConfig>();

  public FileBasedInstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    checkRequiredKeys();
    for (String tableName : getTableNames()) {
      Configuration tableConfig = _instanceDataManagerConfiguration.subset(tableName);
      tableConfig.addProperty(KEY_OF_TABLE_NAME, tableName);
      if (!tableConfig.containsKey(KEY_OF_TABLE_DATA_DIRECTORY)) {
        tableConfig.addProperty(KEY_OF_TABLE_DATA_DIRECTORY, getInstanceDataDir() + "/" + tableName
            + "/index/node" + getInstanceId());
      }
      _tableDataManagerConfigMap.put(tableName, new TableDataManagerConfig(tableConfig));
    }
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_instanceDataManagerConfiguration.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  @Override
  public Configuration getConfig() {
    return _instanceDataManagerConfiguration;
  }

  @Override
  public String getInstanceId() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_ID);
  }

  @Override
  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_DATA_DIR);
  }

  @Override
  public String getInstanceSegmentTarDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_TAR_DIR);
  }

  @Override
  public String getInstanceBootstrapSegmentDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_BOOTSTRAP_SEGMENT_DIR);
  }

  @SuppressWarnings("unchecked")
  public List<String> getTableNames() {
    return _instanceDataManagerConfiguration.getList(INSTANCE_TABLE_NAME);
  }

  public TableDataManagerConfig getTableDataManagerConfig(String tableName) {
    return _tableDataManagerConfigMap.get(tableName);
  }

  @Override
  public ReadMode getReadMode() {
    try {
      return ReadMode.valueOf(_instanceDataManagerConfiguration.getString(READ_MODE));
    } catch (Exception e) {
      LOGGER.warn("Caught exception file getting the read mode", e);
      return ReadMode.DEFAULT_MODE;
    }

  }

  @Override
  public String getSegmentFormatVersion() {
    return _instanceDataManagerConfiguration.getString(SEGMENT_FORMAT_VERSION);
  }

  @Override
  public String getAvgMultiValueCount() {
    return null;
  }

  @Override
  public boolean isEnableDefaultColumns() {
    return _instanceDataManagerConfiguration.getBoolean(ENABLE_DEFAULT_COLUMNS, false);
  }

  @Override
  public boolean isEnableSplitCommit() {
    return _instanceDataManagerConfiguration.getBoolean(ENABLE_SPLIT_COMMIT, false);
  }

  @Override
  public String toString() {
    String configString = "";
    configString += "Instance Id: " + getInstanceId();
    configString += "\n\tInstance Data Dir: " + getInstanceDataDir();
    configString += "\n\tInstance Segment Tar Dir: " + getInstanceSegmentTarDir();
    configString += "\n\tBootstrap Segment Dir: " + getInstanceBootstrapSegmentDir();
    configString += "\n\tRead Mode: " + getReadMode();
    return configString;
  }
}
