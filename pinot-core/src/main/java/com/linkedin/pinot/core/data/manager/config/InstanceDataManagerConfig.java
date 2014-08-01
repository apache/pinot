package com.linkedin.pinot.core.data.manager.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * The config used for InstanceDataManager.
 * 
 * @author xiafu
 *
 */
public class InstanceDataManagerConfig {

  // Key of instance id
  public static String INSTANCE_ID = "id";
  // Key of instance data directory
  public static String INSTANCE_DATA_DIR = "dataDir";
  // Key of resource data directory
  public static String kEY_OF_DATA_DIRECTORY = "directory";
  // Key of resource names that will be holding from initialization.
  public static String INSTANCE_RESOURCE_NAME = "resourceName";

  private static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, INSTANCE_RESOURCE_NAME };
  private Configuration _instanceDataManagerConfiguration = null;
  private Map<String, ResourceDataManagerConfig> _resourceDataManagerConfigMap =
      new HashMap<String, ResourceDataManagerConfig>();

  public InstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    checkRequiredKeys();
    for (String resourceName : getResourceNames()) {
      Configuration resourceConfig = _instanceDataManagerConfiguration.subset(resourceName);
      if (!resourceConfig.containsKey(kEY_OF_DATA_DIRECTORY)) {
        resourceConfig.addProperty(kEY_OF_DATA_DIRECTORY, getInstanceDataDir() + "/" + resourceName + "/index/node"
            + getInstanceId());
      }
      _resourceDataManagerConfigMap.put(resourceName, new ResourceDataManagerConfig(resourceConfig));
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

  public int getInstanceId() {
    return _instanceDataManagerConfiguration.getInt(INSTANCE_ID);
  }

  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_DATA_DIR);
  }

  @SuppressWarnings("unchecked")
  public List<String> getResourceNames() {
    return _instanceDataManagerConfiguration.getList(INSTANCE_RESOURCE_NAME);
  }

  public ResourceDataManagerConfig getResourceDataManagerConfig(String resourceName) {
    return _resourceDataManagerConfigMap.get(resourceName);
  }
}
