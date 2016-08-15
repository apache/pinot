/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
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


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 *
 *
 */
public class HelixInstanceDataManager implements InstanceDataManager {

  public static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);
  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, TableDataManager> _tableDataManagerMap = new HashMap<String, TableDataManager>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;
  private final Object _globalLock = new Object();

  public HelixInstanceDataManager() {
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
    for (TableDataManager tableDataManager : _tableDataManagerMap.values()) {
      tableDataManager.start();
    }

    _isStarted = true;
//    LOGGER.info("InstanceDataManager is started! " + getServerInfo());
    LOGGER.info("{} started!", this.getClass().getName());
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  public synchronized void addTableDataManager(String tableName, TableDataManager tableDataManager) {
    _tableDataManagerMap.put(tableName, tableDataManager);
  }

  public Collection<TableDataManager> getTableDataManagers() {
    return _tableDataManagerMap.values();
  }

  @Override
  public TableDataManager getTableDataManager(String tableName) {
    return _tableDataManagerMap.get(tableName);
  }

  @Override
  public synchronized void shutDown() {

    if (isStarted()) {
      for (TableDataManager tableDataManager : getTableDataManagers()) {
        tableDataManager.shutDown();
      }
      _isStarted = false;
      LOGGER.info("InstanceDataManager is shutDown!");
    } else {
      LOGGER.warn("InstanceDataManager is already shutDown, won't do anything!");
    }
  }

  // Called for offline segments only
  @Override
  public synchronized void addSegment(SegmentMetadata segmentMetadata, AbstractTableConfig tableConfig, Schema schema)
      throws Exception {
    if (segmentMetadata == null || segmentMetadata.getTableName() == null) {
      throw new RuntimeException("Error: adding invalid SegmentMetadata!");
    }
    LOGGER.info("Trying to add segment {} of table {}", segmentMetadata.getName(), segmentMetadata.getTableName());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Trying to add segment with Metadata: " + segmentMetadata.toString());
    }
    String tableName = segmentMetadata.getTableName();
    if (segmentMetadata.getIndexType().equalsIgnoreCase("realtime")) {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    } else {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    }
    if (!_tableDataManagerMap.containsKey(tableName)) {
      LOGGER.info("Trying to add TableDataManager for table name: " + tableName);
      synchronized (_globalLock) {
        if (!_tableDataManagerMap.containsKey(tableName)) {
          addTableIfNeed(tableConfig, tableName);
        }
      }
    }
    _tableDataManagerMap.get(tableName).addSegment(segmentMetadata, schema);
    LOGGER.info("Successfully added a segment {} of table {}", segmentMetadata.getName(), segmentMetadata.getTableName());
  }

  // Called for real-time segments only
  @Override
  public synchronized void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    if (segmentZKMetadata == null || segmentZKMetadata.getTableName() == null) {
      throw new RuntimeException("Error: adding invalid SegmentMetadata!");
    }
    LOGGER.info("Trying to add segment with name: " + segmentZKMetadata.getSegmentName());
    LOGGER.debug("Trying to add segment with Metadata: " + segmentZKMetadata.toString());
    String tableName = segmentZKMetadata.getTableName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    } else if (segmentZKMetadata instanceof OfflineSegmentZKMetadata) {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    }
    if (!_tableDataManagerMap.containsKey(tableName)) {
      LOGGER.info("Trying to add TableDataManager for table name: " + tableName);
      synchronized (_globalLock) {
        if (!_tableDataManagerMap.containsKey(tableName)) {
          addTableIfNeed(tableConfig, tableName);
        }
      }
    }
    _tableDataManagerMap.get(tableName).addSegment(propertyStore, tableConfig, instanceZKMetadata,
        segmentZKMetadata);
    LOGGER.info("Successfully added a segment : " + segmentZKMetadata.getSegmentName() + " in HelixInstanceDataManager");

  }

  public synchronized void addTableIfNeed(AbstractTableConfig tableConfig, String tableName)
      throws ConfigurationException {
    TableDataManagerConfig tableDataManagerConfig = getDefaultHelixTableDataManagerConfig(tableName);
    if (tableConfig != null) {
      tableDataManagerConfig.overrideConfigs(tableName, tableConfig);
    }
    TableDataManager tableDataManager = TableDataManagerProvider.getTableDataManager(tableDataManagerConfig);
    tableDataManager.start();
    addTableDataManager(tableName, tableDataManager);
  }

  private TableDataManagerConfig getDefaultHelixTableDataManagerConfig(String tableName)
      throws ConfigurationException {
    return TableDataManagerConfig.getDefaultHelixTableDataManagerConfig(_instanceDataManagerConfig, tableName);
  }

  @Override
  public synchronized void removeSegment(String segmentName) {
    for (TableDataManager tableDataManager : _tableDataManagerMap.values()) {
      tableDataManager.removeSegment(segmentName);
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
  public SegmentMetadata getSegmentMetadata(String table, String segmentName) {
    SegmentDataManager segmentDataManager = null;
    TableDataManager tableDataManager = _tableDataManagerMap.get(table);
    try {
      if (tableDataManager != null) {
        segmentDataManager = tableDataManager.acquireSegment(segmentName);
        if (segmentDataManager != null) {
          return segmentDataManager.getSegment().getSegmentMetadata();
        }
      }
      return null;
    } finally {
      if (segmentDataManager != null) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

}
