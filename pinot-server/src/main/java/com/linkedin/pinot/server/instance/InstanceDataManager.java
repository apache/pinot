package com.linkedin.pinot.server.instance;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.server.conf.InstanceDataManagerConfig;
import com.linkedin.pinot.server.conf.ResourceDataManagerConfig;
import com.linkedin.pinot.server.resource.ResourceDataManager;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 * 
 * @author xiafu
 *
 */
public class InstanceDataManager {

  private static final InstanceDataManager INSTANCE_DATA_MANAGER = new InstanceDataManager();
  public static final Logger LOGGER = LoggerFactory.getLogger(InstanceDataManager.class);
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, ResourceDataManager> _resourceDataManagerMap = new HashMap<String, ResourceDataManager>();
  private boolean _isStarted = false;

  public InstanceDataManager() {
    if (INSTANCE_DATA_MANAGER != null) {
      throw new IllegalStateException("InstanceDataManager is already instantiated");
    }
  }

  public static InstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public void init(InstanceDataManagerConfig instanceDataManagerConfig) throws ConfigurationException {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    for (String resourceName : instanceDataManagerConfig.getResourceNames()) {
      ResourceDataManagerConfig resourceDataManagerConfig =
          _instanceDataManagerConfig.getResourceDataManagerConfig(resourceName);
      ResourceDataManager resourceDataManager = new ResourceDataManager(resourceDataManagerConfig);
      resourceDataManager.init();
      _resourceDataManagerMap.put(resourceName, resourceDataManager);
    }
  }

  public synchronized void start() {
    for (ResourceDataManager resourceDataManager : _resourceDataManagerMap.values()) {
      resourceDataManager.start();
    }
    _isStarted = true;
  }

  public boolean isStarted() {
    return _isStarted;
  }

  public synchronized void addResourceDataManager(String resourceName, ResourceDataManager resourceDataManager) {
    _resourceDataManagerMap.put(resourceName, resourceDataManager);
  }

  public Collection<ResourceDataManager> getResourceDataManagers() {
    return _resourceDataManagerMap.values();
  }

  public ResourceDataManager getResourceDataManager(String resourceName) {
    return _resourceDataManagerMap.get(resourceName);
  }

  public void shutDown() {
    for (ResourceDataManager resourceDataManager : getResourceDataManagers()) {
      resourceDataManager.shutDown();
    }
    _isStarted = false;
  }

}
