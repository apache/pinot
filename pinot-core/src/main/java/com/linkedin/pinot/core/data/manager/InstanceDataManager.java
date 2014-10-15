package com.linkedin.pinot.core.data.manager;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
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
  private SegmentMetadataLoader _segmentMetadataLoader;

  public InstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
  }

  public static InstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public synchronized void init(InstanceDataManagerConfig instanceDataManagerConfig) throws ConfigurationException,
      InstantiationException, IllegalAccessException, ClassNotFoundException {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    for (String resourceName : _instanceDataManagerConfig.getResourceNames()) {
      ResourceDataManagerConfig resourceDataManagerConfig =
          _instanceDataManagerConfig.getResourceDataManagerConfig(resourceName);
      ResourceDataManager resourceDataManager =
          ResourceDataManagerProvider.getResourceDataManager(resourceDataManagerConfig);
      _resourceDataManagerMap.put(resourceName, resourceDataManager);
    }
    _segmentMetadataLoader = getSegmentMetadataLoader(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
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
      ResourceDataManager resourceDataManager =
          ResourceDataManagerProvider.getResourceDataManager(resourceDataManagerConfig);
      _resourceDataManagerMap.put(resourceName, resourceDataManager);
    }
    try {
      _segmentMetadataLoader = getSegmentMetadataLoader(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
      LOGGER.info("Loaded SegmentMetadataLoader for class name : "
          + _instanceDataManagerConfig.getSegmentMetadataLoaderClass());
    } catch (Exception e) {
      LOGGER.error("Cannot initialize SegmentMetadataLoader for class name : "
          + _instanceDataManagerConfig.getSegmentMetadataLoaderClass());
      e.printStackTrace();
    }
  }

  private SegmentMetadataLoader getSegmentMetadataLoader(String segmentMetadataLoaderClassName)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return (SegmentMetadataLoader) Class.forName(segmentMetadataLoaderClassName).newInstance();

  }

  @Override
  public synchronized void start() {
    for (ResourceDataManager resourceDataManager : _resourceDataManagerMap.values()) {
      resourceDataManager.start();
    }
    try {
      bootstrapSegmentsFromSegmentDir();
    } catch (Exception e) {
      LOGGER.error("Error in bootstrap segment from dir : "
          + _instanceDataManagerConfig.getInstanceBootstrapSegmentDir());
      e.printStackTrace();
    }

    _isStarted = true;
    LOGGER.info("InstanceDataManager is started! " + getServerInfo());
  }

  private String getServerInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n[InstanceDataManager Info] : ");
    for (String resourceName : _resourceDataManagerMap.keySet()) {
      sb.append("\n\t{\n\t\tResource : [" + resourceName + "];\n\t\tSegments : [");
      boolean isFirstSegment = true;
      for (SegmentDataManager segmentDataManager : _resourceDataManagerMap.get(resourceName).getAllSegments()) {
        if (isFirstSegment) {
          sb.append(segmentDataManager.getSegment().getSegmentName());
          isFirstSegment = false;
        } else {
          sb.append(", " + segmentDataManager.getSegment().getSegmentName());
        }
      }
      sb.append("]\n\t}");
    }
    return sb.toString();
  }

  private void bootstrapSegmentsFromSegmentDir() throws Exception {
    if (_instanceDataManagerConfig.getInstanceBootstrapSegmentDir() != null) {
      File bootstrapSegmentDir = new File(_instanceDataManagerConfig.getInstanceBootstrapSegmentDir());
      if (bootstrapSegmentDir.exists()) {
        for (File segment : bootstrapSegmentDir.listFiles()) {
          addSegment(_segmentMetadataLoader.load(segment));
          LOGGER.info("Bootstrapped segment from directory : " + segment.getAbsolutePath());
        }
      } else {
        LOGGER.info("Bootstrap segment directory : " + _instanceDataManagerConfig.getInstanceBootstrapSegmentDir()
            + " doesn't exist.");
      }
    } else {
      LOGGER.info("Config of bootstrap segment directory hasn't been set.");
    }

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

  @Override
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
  public synchronized void addSegment(SegmentMetadata segmentMetadata) throws Exception {
    String resourceName = segmentMetadata.getResourceName();
    System.out.println(segmentMetadata.getName());
    if (_resourceDataManagerMap.containsKey(resourceName)) {
      _resourceDataManagerMap.get(resourceName).addSegment(segmentMetadata);
      LOGGER.info("Added a segment : " + segmentMetadata.getName() + " to resource : " + resourceName);
    } else {
      LOGGER.error("InstanceDataManager doesn't contain the assigned resource for segment : "
          + segmentMetadata.getName());
    }
  }

  @Override
  public synchronized void removeSegment(String segmentName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void refreshSegment(String oldSegmentName, SegmentMetadata newSegmentMetadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSegmentDataDirectory() {
    return _instanceDataManagerConfig.getInstanceDataDir();
  }

  @Override
  public String getSegmentFileDirectory() {
    return _instanceDataManagerConfig.getInstanceSegmentTarDir();
  }

}
