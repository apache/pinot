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
package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
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
public class FileBasedInstanceDataManager implements InstanceDataManager {

  private static final FileBasedInstanceDataManager INSTANCE_DATA_MANAGER = new FileBasedInstanceDataManager();
  public static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceDataManager.class);
  private FileBasedInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, TableDataManager> _tableDataManagerMap = new HashMap<String, TableDataManager>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;

  public FileBasedInstanceDataManager() {
    //LOGGER.info("InstanceDataManager is a Singleton");
  }

  public static FileBasedInstanceDataManager getInstanceDataManager() {
    return INSTANCE_DATA_MANAGER;
  }

  public synchronized void init(FileBasedInstanceDataManagerConfig instanceDataManagerConfig)
      throws ConfigurationException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    for (String tableName : _instanceDataManagerConfig.getTableNames()) {
      TableDataManagerConfig tableDataManagerConfig =
          _instanceDataManagerConfig.getTableDataManagerConfig(tableName);
      TableDataManager tableDataManager = TableDataManagerProvider.getTableDataManager(tableDataManagerConfig);
      _tableDataManagerMap.put(tableName, tableDataManager);
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
      Utils.rethrowException(e);
    }
    for (String tableName : _instanceDataManagerConfig.getTableNames()) {
      TableDataManagerConfig tableDataManagerConfig =
          _instanceDataManagerConfig.getTableDataManagerConfig(tableName);
      TableDataManager tableDataManager = TableDataManagerProvider.getTableDataManager(tableDataManagerConfig);
      _tableDataManagerMap.put(tableName, tableDataManager);
    }
    try {
      _segmentMetadataLoader = getSegmentMetadataLoader(_instanceDataManagerConfig.getSegmentMetadataLoaderClass());
      LOGGER.info("Loaded SegmentMetadataLoader for class name : "
          + _instanceDataManagerConfig.getSegmentMetadataLoaderClass());
    } catch (Exception e) {
      LOGGER.error(
          "Cannot initialize SegmentMetadataLoader for class name : "
              + _instanceDataManagerConfig.getSegmentMetadataLoaderClass(), e);
      Utils.rethrowException(e);
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
    try {
      bootstrapSegmentsFromSegmentDir();
    } catch (Exception e) {
      LOGGER.error(
          "Error in bootstrap segment from dir : " + _instanceDataManagerConfig.getInstanceBootstrapSegmentDir(), e);
    }

    _isStarted = true;
    LOGGER.info("InstanceDataManager is started!");
  }

  private void bootstrapSegmentsFromSegmentDir() throws Exception {
    if (_instanceDataManagerConfig.getInstanceBootstrapSegmentDir() != null) {
      File bootstrapSegmentDir = new File(_instanceDataManagerConfig.getInstanceBootstrapSegmentDir());
      if (bootstrapSegmentDir.exists()) {
        for (File segment : bootstrapSegmentDir.listFiles()) {
          addSegment(_segmentMetadataLoader.load(segment), null, null);
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

  @Override
  public synchronized void addSegment(SegmentMetadata segmentMetadata, AbstractTableConfig tableConfig, Schema schema)
      throws Exception {
    String tableName = segmentMetadata.getTableName();
    LOGGER.info("Trying to add segment : " + segmentMetadata.getName());
    if (_tableDataManagerMap.containsKey(tableName)) {
      _tableDataManagerMap.get(tableName).addSegment(segmentMetadata, schema);
      LOGGER.info("Added a segment : " + segmentMetadata.getName() + " to table : " + tableName);
    } else {
      LOGGER.error("InstanceDataManager doesn't contain the assigned table for segment : "
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
  public SegmentMetadata getSegmentMetadata(String table, String segmentName) {
    if (_tableDataManagerMap.containsKey(table)) {
      if (_tableDataManagerMap.get(table).acquireSegment(segmentName) != null) {
        return _tableDataManagerMap.get(table).acquireSegment(segmentName).getSegment().getSegmentMetadata();
      }
    }
    return null;
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    throw new UnsupportedOperationException("Not support addSegment(...) in FileBasedInstanceDataManager yet!");
  }

}
