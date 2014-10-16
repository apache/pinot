package com.linkedin.pinot.core.data.manager;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.data.manager.config.HelixInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 * 
 * @author xiafu
 *
 */
public class HelixInstanceDataManager extends InstanceDataManager {

  private static final HelixInstanceDataManager INSTANCE_DATA_MANAGER = new HelixInstanceDataManager();
  public static final Logger LOGGER = Logger.getLogger(HelixInstanceDataManager.class);
  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, ResourceDataManager> _resourceDataManagerMap = new HashMap<String, ResourceDataManager>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;

  public HelixInstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
  }

  public static HelixInstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public synchronized void init(HelixInstanceDataManagerConfig instanceDataManagerConfig)
      throws ConfigurationException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    _segmentMetadataLoader = getSegmentMetadataLoader(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
  }

  @Override
  public synchronized void init(Configuration dataManagerConfig) {
    try {
      _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(dataManagerConfig);
      File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
      if (!instanceDataDir.exists()) {
        instanceDataDir.mkdirs();
      }
      File instanceSegmentTarDir = new File(_instanceDataManagerConfig.getInstanceSegmentTarDir());
      if (!instanceSegmentTarDir.exists()) {
        instanceSegmentTarDir.mkdirs();
      }
      System.out.println(_instanceDataManagerConfig.getInstanceBootstrapSegmentDir());
      System.out.println(_instanceDataManagerConfig.getInstanceDataDir());
      System.out.println(_instanceDataManagerConfig.getInstanceId());
      System.out.println(_instanceDataManagerConfig.getInstanceSegmentTarDir());
      System.out.println(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
      System.out.println(_instanceDataManagerConfig.getReadMode());
    } catch (Exception e) {
      _instanceDataManagerConfig = null;
      e.printStackTrace();
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

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public synchronized void addResourceDataManager(String resourceName, ResourceDataManager resourceDataManager) {
    _resourceDataManagerMap.put(resourceName, resourceDataManager);
  }

  @Override
  public Collection<ResourceDataManager> getResourceDataManagers() {
    return _resourceDataManagerMap.values();
  }

  @Override
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
    if (!_resourceDataManagerMap.containsKey(resourceName)) {
      addResourceIfNeed(resourceName);
    }
    _resourceDataManagerMap.get(resourceName).addSegment(segmentMetadata);
    LOGGER.info("Successfuly added a segment : " + segmentMetadata.getName() + " in HelixInstanceDataManager");

  }

  public synchronized void addResourceIfNeed(String resourceName) throws ConfigurationException {
    ResourceDataManagerConfig resourceDataManagerConfig = getDefaultHelixResourceDataManagerConfig(resourceName);
    ResourceDataManager resourceDataManager =
        ResourceDataManagerProvider.getResourceDataManager(resourceDataManagerConfig);
    addResourceDataManager(resourceName, resourceDataManager);
  }

  private ResourceDataManagerConfig getDefaultHelixResourceDataManagerConfig(String resourceName)
      throws ConfigurationException {
    return ResourceDataManagerConfig.getDefaultHelixOfflineResourceDataManagerConfig(_instanceDataManagerConfig,
        resourceName);
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
