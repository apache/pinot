package com.linkedin.pinot.core.data.manager.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * The config used for ResourceDataManager.
 * 
 * @author xiafu
 *
 */
public class ResourceDataManagerConfig {

  private static final String RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS = "numQueryExecutorThreads";
  private static final String RESOURCE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String READ_MODE = "readMode";
  private static final String RESOURCE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String RESOURCE_DATA_MANAGER_NAME = "name";

  private final Configuration _resourceDataManagerConfig;

  public ResourceDataManagerConfig(Configuration resourceDataManagerConfig) throws ConfigurationException {
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
    return _resourceDataManagerConfig.getInt(RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 0);
  }

  public static ResourceDataManagerConfig getDefaultHelixOfflineResourceDataManagerConfig(
      HelixInstanceDataManagerConfig _instanceDataManagerConfig, String resourceName) throws ConfigurationException {
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NAME, resourceName);
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_TYPE, "offline");
    String dataDir =
        _instanceDataManagerConfig.getInstanceDataDir() + "/" + resourceName + "/index/node"
            + _instanceDataManagerConfig.getInstanceId();
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_DATA_DIRECTORY, dataDir);
    if (_instanceDataManagerConfig.getReadMode() != null) {
      defaultConfig.addProperty(READ_MODE, _instanceDataManagerConfig.getReadMode().toString());
    }
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 20);
    ResourceDataManagerConfig resourceDataManagerConfig = new ResourceDataManagerConfig(defaultConfig);
    return resourceDataManagerConfig;
  }
}
