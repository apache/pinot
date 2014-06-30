package com.linkedin.pinot.server.conf;

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

  public static String INSTANCE_PREFIX = "instance";
  public static String INSTANCE_ID = "id";
  public static String INSTANCE_DATA_DIR = "dataDir";
  public static String DIRECTORY = "directory";
  public static String INSTANCE_RESOURCE_NAME = "resourceName";

  private Configuration _instanceDataManagerConfiguration = null;
  private Map<String, ResourceDataManagerConfig> _resourceDataManagerConfigMap =
      new HashMap<String, ResourceDataManagerConfig>();

  public InstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig.subset(INSTANCE_PREFIX);
    for (String resourceName : getResourceNames()) {
      Configuration resourceConfig = _instanceDataManagerConfiguration.subset(resourceName);
      if (!resourceConfig.containsKey(DIRECTORY)) {
        resourceConfig.addProperty(DIRECTORY, getInstanceDataDir() + "/" + resourceName + "/index/node"
            + getInstanceId());
      }
      _resourceDataManagerConfigMap.put(resourceName, new ResourceDataManagerConfig(resourceConfig));
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
