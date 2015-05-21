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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;


/**
 * The config used for ResourceDataManager.
 *
 * @author xiafu
 *
 */
public class TableDataManagerConfig {

  private static final String RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS = "numQueryExecutorThreads";
  private static final String RESOURCE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String READ_MODE = "readMode";
  private static final String RESOURCE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String RESOURCE_DATA_MANAGER_NAME = "name";

  private final Configuration _resourceDataManagerConfig;

  public TableDataManagerConfig(Configuration resourceDataManagerConfig) throws ConfigurationException {
    _resourceDataManagerConfig = resourceDataManagerConfig;
  }

  public String getReadMode() {
    return _resourceDataManagerConfig.getString(READ_MODE);
  }

  public Configuration getConfig() {
    return _resourceDataManagerConfig;
  }

  public String getResourceDataManagerType() {
    return _resourceDataManagerConfig.getString(RESOURCE_DATA_MANAGER_TYPE);
  }

  public String getDataDir() {
    return _resourceDataManagerConfig.getString(RESOURCE_DATA_MANAGER_DATA_DIRECTORY);
  }

  public String getResourceName() {
    return _resourceDataManagerConfig.getString(RESOURCE_DATA_MANAGER_NAME);
  }

  public int getNumberOfResourceQueryExecutorThreads() {
    return _resourceDataManagerConfig.getInt(RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 10);
  }

  public static TableDataManagerConfig getDefaultHelixResourceDataManagerConfig(
      InstanceDataManagerConfig _instanceDataManagerConfig, String resourceName) throws ConfigurationException {
    TableType resourceType = BrokerRequestUtils.getResourceTypeFromResourceName(resourceName);

    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NAME, resourceName);
    String dataDir = _instanceDataManagerConfig.getInstanceDataDir() + "/" + resourceName;
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_DATA_DIRECTORY, dataDir);
    if (_instanceDataManagerConfig.getReadMode() != null) {
      defaultConfig.addProperty(READ_MODE, _instanceDataManagerConfig.getReadMode().toString());
    } else {
      defaultConfig.addProperty(READ_MODE, ReadMode.heap);
    }
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 20);
    TableDataManagerConfig resourceDataManagerConfig = new TableDataManagerConfig(defaultConfig);

    switch (resourceType) {
      case OFFLINE:
        defaultConfig.addProperty(RESOURCE_DATA_MANAGER_TYPE, "offline");
        break;
      case REALTIME:
        defaultConfig.addProperty(RESOURCE_DATA_MANAGER_TYPE, "realtime");
        break;

      default:
        throw new UnsupportedOperationException("Not supported resource type for - " + resourceName);
    }

    return resourceDataManagerConfig;
  }

  public void overrideConfigs(Map<String, String> metadata) {
    for (Entry<String, String> entry : metadata.entrySet()) {
      _resourceDataManagerConfig.addProperty(entry.getKey(), entry.getValue());
    }
  }

  public IndexLoadingConfigMetadata getIndexLoadingConfigMetadata() {
    IndexLoadingConfigMetadata indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(_resourceDataManagerConfig);
    return indexLoadingConfigMetadata;
  }
}
