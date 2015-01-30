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

  private static final String INSTANCE_SEGMENT_METADATA_LOADER_CLASS = "segment.metadata.loader.class";
  // Key of instance id
  public static final String INSTANCE_ID = "id";
  // Key of instance data directory
  public static final String INSTANCE_DATA_DIR = "dataDir";
  // Key of instance segment tar directory
  public static final String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
  // Key of segment directory
  public static final String INSTANCE_BOOTSTRAP_SEGMENT_DIR = "bootstrap.segment.dir";
  // Key of resource names that will be holding from initialization.
  public static final String INSTANCE_RESOURCE_NAME = "resourceName";
  // Key of resource data directory
  public static final String kEY_OF_RESOURCE_DATA_DIRECTORY = "directory";
  // Key of resource data directory
  public static final String kEY_OF_RESOURCE_NAME = "name";

  private final static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, INSTANCE_RESOURCE_NAME };
  private Configuration _instanceDataManagerConfiguration = null;
  private Map<String, ResourceDataManagerConfig> _resourceDataManagerConfigMap =
      new HashMap<String, ResourceDataManagerConfig>();

  public InstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    checkRequiredKeys();
    for (String resourceName : getResourceNames()) {
      Configuration resourceConfig = _instanceDataManagerConfiguration.subset(resourceName);
      resourceConfig.addProperty(kEY_OF_RESOURCE_NAME, resourceName);
      if (!resourceConfig.containsKey(kEY_OF_RESOURCE_DATA_DIRECTORY)) {
        resourceConfig.addProperty(kEY_OF_RESOURCE_DATA_DIRECTORY, getInstanceDataDir() + "/" + resourceName
            + "/index/node" + getInstanceId());
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
  public List<String> getResourceNames() {
    return _instanceDataManagerConfiguration.getList(INSTANCE_RESOURCE_NAME);
  }

  public ResourceDataManagerConfig getResourceDataManagerConfig(String resourceName) {
    return _resourceDataManagerConfigMap.get(resourceName);
  }

  public String getSegmentMetadataLoaderClass() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_METADATA_LOADER_CLASS);
  }
}
