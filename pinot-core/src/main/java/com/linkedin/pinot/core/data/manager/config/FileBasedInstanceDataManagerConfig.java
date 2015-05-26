/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.common.segment.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The config used for InstanceDataManager.
 *
 *
 */
public class FileBasedInstanceDataManagerConfig implements InstanceDataManagerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceDataManagerConfig.class);

  private static final String INSTANCE_SEGMENT_METADATA_LOADER_CLASS = "segment.metadata.loader.class";
  // Key of instance id
  public static String INSTANCE_ID = "id";
  // Key of instance data directory
  public static String INSTANCE_DATA_DIR = "dataDir";
  // Key of instance segment tar directory
  public static String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
  // Key of segment directory
  public static String INSTANCE_BOOTSTRAP_SEGMENT_DIR = "bootstrap.segment.dir";
  // Key of table names that will be holding from initialization.
  public static String INSTANCE_TABLE_NAME = "tableName";
  // Key of table data directory
  public static String kEY_OF_TABLE_DATA_DIRECTORY = "directory";
  // Key of table data directory
  public static String kEY_OF_TABLE_NAME = "name";
  // Key of instance level segment read mode.
  public static String READ_MODE = "readMode";

  private static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, INSTANCE_TABLE_NAME };
  private Configuration _instanceDataManagerConfiguration = null;
  private Map<String, TableDataManagerConfig> _tableDataManagerConfigMap =
      new HashMap<String, TableDataManagerConfig>();

  public FileBasedInstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    checkRequiredKeys();
    for (String tableName : getTableNames()) {
      Configuration tableConfig = _instanceDataManagerConfiguration.subset(tableName);
      tableConfig.addProperty(kEY_OF_TABLE_NAME, tableName);
      if (!tableConfig.containsKey(kEY_OF_TABLE_DATA_DIRECTORY)) {
        tableConfig.addProperty(kEY_OF_TABLE_DATA_DIRECTORY, getInstanceDataDir() + "/" + tableName
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

  public Configuration getConfig() {
    return _instanceDataManagerConfiguration;
  }

  public String getInstanceId() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_ID);
  }

  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_DATA_DIR);
  }

  public String getInstanceSegmentTarDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_TAR_DIR);
  }

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

  public String getSegmentMetadataLoaderClass() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_METADATA_LOADER_CLASS);
  }

  @Override
  public ReadMode getReadMode() {
    try {
      return ReadMode.valueOf(_instanceDataManagerConfiguration.getString(READ_MODE));
    } catch (Exception e) {
      LOGGER.warn("Caught exception file getting the read mode", e);
      return null;
    }

  }

  public String toString() {
    String configString = "";
    configString += "Instance Id: " + getInstanceId();
    configString += "\n\tInstance Data Dir: " + getInstanceDataDir();
    configString += "\n\tInstance Segment Tar Dir: " + getInstanceSegmentTarDir();
    configString += "\n\tBootstrap Segment Dir: " + getInstanceBootstrapSegmentDir();
    configString += "\n\tSegment Metadata Loader Clas: " + getSegmentMetadataLoaderClass();
    configString += "\n\tRead Mode: " + getReadMode();
    return configString;
  }
}
