package com.linkedin.pinot.core.data.manager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 * 
 * @author xiafu
 *
 */
public class InstanceDataManager implements DataManager {

  private static final InstanceDataManager INSTANCE_DATA_MANAGER = new InstanceDataManager();
  public static final Logger LOGGER = LoggerFactory.getLogger(InstanceDataManager.class);
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, ResourceDataManager> _resourceDataManagerMap = new HashMap<String, ResourceDataManager>();
  private boolean _isStarted = false;

  public InstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
  }

  public static InstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public synchronized void init(InstanceDataManagerConfig instanceDataManagerConfig) throws ConfigurationException {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    for (String resourceName : instanceDataManagerConfig.getResourceNames()) {
      ResourceDataManagerConfig resourceDataManagerConfig =
          _instanceDataManagerConfig.getResourceDataManagerConfig(resourceName);
      ResourceDataManager resourceDataManager = new ResourceDataManager(resourceName, resourceDataManagerConfig);
      resourceDataManager.init();
      _resourceDataManagerMap.put(resourceName, resourceDataManager);
    }
  }

  @Override
  public synchronized void init(Configuration dataManagerConfig) {
    try {
      _instanceDataManagerConfig = new InstanceDataManagerConfig(dataManagerConfig);
    } catch (Exception e) {
      _instanceDataManagerConfig = null;
      e.printStackTrace();
    }
    for (String resourceName : _instanceDataManagerConfig.getResourceNames()) {
      ResourceDataManagerConfig resourceDataManagerConfig =
          _instanceDataManagerConfig.getResourceDataManagerConfig(resourceName);
      ResourceDataManager resourceDataManager = new ResourceDataManager(resourceName, resourceDataManagerConfig);
      try {
        resourceDataManager.init();
      } catch (ConfigurationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      _resourceDataManagerMap.put(resourceName, resourceDataManager);
    }
  }

  public synchronized void start() {
    for (ResourceDataManager resourceDataManager : _resourceDataManagerMap.values()) {
      resourceDataManager.start();
    }
    _isStarted = true;
    LOGGER.info("InstanceDataManager is started!");
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

  public synchronized void shutDown() {

    if (isStarted()) {
      for (ResourceDataManager resourceDataManager : getResourceDataManagers()) {
        resourceDataManager.shutDown();
      }
      _isStarted = false;
      LOGGER.info("InstanceDataManager is shutDown!");
    } else {
      LOGGER.warn("InstanceDataManager is already shutDown, won't do anything!");
    }
  }

  @Override
  public void addSegment(SegmentMetadata segmentMetadata) {
    String resourceName = segmentMetadata.getResourceName();
    if (_resourceDataManagerMap.containsKey(resourceName)) {
      _resourceDataManagerMap.get(resourceName).addSegment(segmentMetadata);
    } else {
      LOGGER.error("InstanceDataManager doesn't contain the assigned resource for segment : "
          + segmentMetadata.getName());
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSegment(String segmentName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void refreshSegment(String oldSegmentName, SegmentMetadata newSegmentMetadata) {
    throw new UnsupportedOperationException();
  }

}
