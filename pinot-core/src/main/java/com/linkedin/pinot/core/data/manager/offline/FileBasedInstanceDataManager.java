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
package com.linkedin.pinot.core.data.manager.offline;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.DataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 *
 * @author xiafu
 *
 */
public class FileBasedInstanceDataManager implements InstanceDataManager {

  private static final FileBasedInstanceDataManager INSTANCE_DATA_MANAGER = new FileBasedInstanceDataManager();
  public static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceDataManager.class);
  private FileBasedInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, ResourceDataManager> _resourceDataManagerMap = new HashMap<String, ResourceDataManager>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;

  public FileBasedInstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
  }

  public static FileBasedInstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public synchronized void init(FileBasedInstanceDataManagerConfig instanceDataManagerConfig) throws ConfigurationException,
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
      _instanceDataManagerConfig = new FileBasedInstanceDataManagerConfig(dataManagerConfig);
    } catch (Exception e) {
      _instanceDataManagerConfig = null;
      LOGGER.error("Error during InstanceDataManager initialization", e);
      throw new RuntimeException(e);
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
          + _instanceDataManagerConfig.getSegmentMetadataLoaderClass(), e);
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
    LOGGER.info("Trying to add segment : " + segmentMetadata.getName());
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

  @Override
  public SegmentMetadataLoader getSegmentMetadataLoader() {
    return _segmentMetadataLoader;
  }

  @Override
  public SegmentMetadata getSegmentMetadata(String resource, String segmentName) {
    if (_resourceDataManagerMap.containsKey(resource)) {
      if (_resourceDataManagerMap.get(resource).getSegment(segmentName) != null) {
        return _resourceDataManagerMap.get(resource).getSegment(segmentName).getSegment().getSegmentMetadata();
      }
    }
    return null;
  }

  @Override
  public void addSegment(SegmentZKMetadata segmentZKMetadata) throws Exception {
    throw new UnsupportedOperationException("Not support addSegment(SegmentZKMetadata segmentZKMetadata) in FileBasedInstanceDataManager yet!");
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, DataResourceZKMetadata dataResourceZKMetadata, InstanceZKMetadata instanceZKMetadata,
      SegmentZKMetadata segmentZKMetadata) throws Exception {
    throw new UnsupportedOperationException("Not support addSegment(...) in FileBasedInstanceDataManager yet!");
  }

}
