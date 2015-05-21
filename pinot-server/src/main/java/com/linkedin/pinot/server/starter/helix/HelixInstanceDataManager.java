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
package com.linkedin.pinot.server.starter.helix;

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

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 *
 * @author xiafu
 *
 */
public class HelixInstanceDataManager implements InstanceDataManager {

  public static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);
  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, TableDataManager> _resourceDataManagerMap = new HashMap<String, TableDataManager>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;
  private final Object _globalLock = new Object();

  public HelixInstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
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
      LOGGER.info("InstanceDataManager Config:" + _instanceDataManagerConfig.toString());
      File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
      if (!instanceDataDir.exists()) {
        instanceDataDir.mkdirs();
      }
      File instanceSegmentTarDir = new File(_instanceDataManagerConfig.getInstanceSegmentTarDir());
      if (!instanceSegmentTarDir.exists()) {
        instanceSegmentTarDir.mkdirs();
      }
      try {
        _segmentMetadataLoader = getSegmentMetadataLoader(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
        LOGGER.info("Loaded SegmentMetadataLoader for class name : "
            + _instanceDataManagerConfig.getSegmentMetadataLoaderClass());
      } catch (Exception e) {
        LOGGER
            .error(
                "Cannot initialize SegmentMetadataLoader for class name : "
                    + _instanceDataManagerConfig.getSegmentMetadataLoaderClass() + "\nStackTrace is : "
                    + e.getMessage(), e);
      }
    } catch (Exception e) {
      _instanceDataManagerConfig = null;
      LOGGER.error("Error in initializing HelixDataManager, StackTrace is : " + e.getMessage(), e);
    }

  }

  private SegmentMetadataLoader getSegmentMetadataLoader(String segmentMetadataLoaderClassName)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return (SegmentMetadataLoader) Class.forName(segmentMetadataLoaderClassName).newInstance();

  }

  @Override
  public synchronized void start() {
    for (TableDataManager resourceDataManager : _resourceDataManagerMap.values()) {
      resourceDataManager.start();
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

  /* private void bootstrapSegmentsFromLocal() throws Exception {
     if (_instanceDataManagerConfig.getInstanceDataDir() != null) {
       File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
       if (instanceDataDir.exists() && instanceDataDir.isDirectory()) {
         for (File resourceDir : instanceDataDir.listFiles()) {
           if (resourceDir.isDirectory()) {
             LOGGER.info("Trying to bootstrap segment for resource: " + resourceDir.getName());
             for (File segment : resourceDir.listFiles()) {
               if (segment.isDirectory()) {
                 LOGGER.info("Trying to bootstrap segment from directory : " + segment.getAbsolutePath());
                 addSegment(_segmentMetadataLoader.load(segment));
               }
             }
           }
         }
       } else {
         LOGGER.info("Bootstrap segment directory : " + _instanceDataManagerConfig.getInstanceDataDir()
             + " doesn't exist.");
       }
     } else {
       LOGGER.info("Config of bootstrap segment directory hasn't been set.");
     }
   }*/

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  public synchronized void addResourceDataManager(String resourceName, TableDataManager resourceDataManager) {
    _resourceDataManagerMap.put(resourceName, resourceDataManager);
  }

  public Collection<TableDataManager> getResourceDataManagers() {
    return _resourceDataManagerMap.values();
  }

  @Override
  public TableDataManager getResourceDataManager(String resourceName) {
    return _resourceDataManagerMap.get(resourceName);
  }

  @Override
  public synchronized void shutDown() {

    if (isStarted()) {
      for (TableDataManager resourceDataManager : getResourceDataManagers()) {
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
    if (segmentMetadata == null || segmentMetadata.getResourceName() == null) {
      throw new RuntimeException("Error: adding invalid SegmentMetadata!");
    }
    LOGGER.info("Trying to add segment with name: " + segmentMetadata.getName());
    LOGGER.debug("Trying to add segment with Metadata: " + segmentMetadata.toString());
    String resourceName = segmentMetadata.getResourceName();
    if (segmentMetadata.getIndexType().equalsIgnoreCase("realtime")) {
      resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    } else {
      resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    if (!_resourceDataManagerMap.containsKey(resourceName)) {
      LOGGER.info("Trying to add ResourceDataManager for resource name: " + resourceName);
      synchronized (_globalLock) {
        if (!_resourceDataManagerMap.containsKey(resourceName)) {
          addResourceIfNeed(null, resourceName);
        }
      }
    }
    _resourceDataManagerMap.get(resourceName).addSegment(segmentMetadata);
    LOGGER.info("Successfuly added a segment : " + segmentMetadata.getName() + " in HelixInstanceDataManager");
  }

  @Override
  public synchronized void addSegment(SegmentZKMetadata segmentZKMetadata) throws Exception {
    if (segmentZKMetadata == null || segmentZKMetadata.getResourceName() == null) {
      throw new RuntimeException("Error: adding invalid SegmentMetadata!");
    }
    LOGGER.info("Trying to add segment with name: " + segmentZKMetadata.getSegmentName());
    LOGGER.debug("Trying to add segment with Metadata: " + segmentZKMetadata.toString());
    String resourceName = segmentZKMetadata.getResourceName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    } else if (segmentZKMetadata instanceof OfflineSegmentZKMetadata) {
      resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    if (!_resourceDataManagerMap.containsKey(resourceName)) {
      LOGGER.info("Trying to add ResourceDataManager for resource name: " + resourceName);
      synchronized (_globalLock) {
        if (!_resourceDataManagerMap.containsKey(resourceName)) {
          addResourceIfNeed(null, resourceName);
        }
      }
    }
    _resourceDataManagerMap.get(resourceName).addSegment(segmentZKMetadata);
    LOGGER.info("Successfuly added a segment : " + segmentZKMetadata.getSegmentName() + " in HelixInstanceDataManager");
  }

  @Override
  public synchronized void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    if (segmentZKMetadata == null || segmentZKMetadata.getResourceName() == null) {
      throw new RuntimeException("Error: adding invalid SegmentMetadata!");
    }
    LOGGER.info("Trying to add segment with name: " + segmentZKMetadata.getSegmentName());
    LOGGER.debug("Trying to add segment with Metadata: " + segmentZKMetadata.toString());
    String resourceName = segmentZKMetadata.getResourceName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    } else if (segmentZKMetadata instanceof OfflineSegmentZKMetadata) {
      resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    if (!_resourceDataManagerMap.containsKey(resourceName)) {
      LOGGER.info("Trying to add ResourceDataManager for resource name: " + resourceName);
      synchronized (_globalLock) {
        if (!_resourceDataManagerMap.containsKey(resourceName)) {
          addResourceIfNeed(tableConfig, resourceName);
        }
      }
    }
    _resourceDataManagerMap.get(resourceName).addSegment(propertyStore, tableConfig, instanceZKMetadata,
        segmentZKMetadata);
    LOGGER.info("Successfuly added a segment : " + segmentZKMetadata.getSegmentName() + " in HelixInstanceDataManager");

  }

  public synchronized void addResourceIfNeed(AbstractTableConfig tableConfig, String resourceName)
      throws ConfigurationException {
    TableDataManagerConfig resourceDataManagerConfig = getDefaultHelixResourceDataManagerConfig(resourceName);
    if (tableConfig != null) {
      resourceDataManagerConfig.overrideConfigs(tableConfig);
    }
    TableDataManager resourceDataManager = TableDataManagerProvider.getResourceDataManager(resourceDataManagerConfig);
    resourceDataManager.start();
    addResourceDataManager(resourceName, resourceDataManager);
  }

  private TableDataManagerConfig getDefaultHelixResourceDataManagerConfig(String resourceName)
      throws ConfigurationException {
    return TableDataManagerConfig.getDefaultHelixResourceDataManagerConfig(_instanceDataManagerConfig, resourceName);
  }

  @Override
  public synchronized void removeSegment(String segmentName) {
    for (TableDataManager resourceDataManager : _resourceDataManagerMap.values()) {
      resourceDataManager.removeSegment(segmentName);
    }
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

}
