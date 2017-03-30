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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * InstanceDataManager is the top level DataManger, Singleton.
 *
 *
 */
// TODO: pass a reference to Helix property store during initialization. Both OFFLINE and REALTIME use case need it.
public class HelixInstanceDataManager implements InstanceDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);

  private static final String RELOAD_TEMP_DIR_SUFFIX = ".reload.tmp";

  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  private boolean _isStarted = false;
  private SegmentMetadataLoader _segmentMetadataLoader;

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

  private static SegmentMetadataLoader getSegmentMetadataLoader(String segmentMetadataLoaderClassName)
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

  @Nonnull
  @Override
  public Collection<TableDataManager> getTableDataManagers() {
    return _tableDataManagerMap.values();
  }

  @Nullable
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
  public synchronized void addSegment(@Nonnull SegmentMetadata segmentMetadata,
      @Nullable AbstractTableConfig tableConfig, @Nullable Schema schema)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    String tableName = segmentMetadata.getTableName();
    LOGGER.info("Trying to add segment: {} to OFFLINE table: {}", segmentName, tableName);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Trying to add segment with Metadata: " + segmentMetadata.toString());
    }
    if (segmentMetadata.getIndexType().equalsIgnoreCase("realtime")) {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    } else {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    }
    if (!_tableDataManagerMap.containsKey(tableName)) {
      LOGGER.info("Trying to add TableDataManager for OFFLINE table: {}", tableName);
      addTableIfNeed(tableConfig, tableName, null);
    }
    _tableDataManagerMap.get(tableName)
        .addSegment(segmentMetadata, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig), schema);
    LOGGER.info("Added segment: {} to OFFLINE table: {}", segmentName, tableName);
  }

  @Override
  public synchronized void addSegment(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull AbstractTableConfig tableConfig, @Nullable InstanceZKMetadata instanceZKMetadata,
      @Nonnull SegmentZKMetadata segmentZKMetadata, @Nonnull String serverInstance)
      throws Exception {
    String segmentName = segmentZKMetadata.getSegmentName();
    String tableName = segmentZKMetadata.getTableName();
    LOGGER.info("Trying to add segment: {} to REALTIME table: {}", segmentName, tableName);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Trying to add segment with Metadata: " + segmentZKMetadata.toString());
    }
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    } else {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    }
    if (!_tableDataManagerMap.containsKey(tableName)) {
      LOGGER.info("Trying to add TableDataManager for REALTIME table: {}", tableName);
      addTableIfNeed(tableConfig, tableName, serverInstance);
    }
    _tableDataManagerMap.get(tableName)
        .addSegment(propertyStore, tableConfig, instanceZKMetadata, segmentZKMetadata,
            new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig));
    LOGGER.info("Added segment: {} to REALTIME table: {}", segmentName, tableName);
  }

  public synchronized void addTableIfNeed(@Nullable AbstractTableConfig tableConfig, @Nonnull String tableName,
      @Nullable String serverInstance)
      throws ConfigurationException {
    TableDataManagerConfig tableDataManagerConfig = getDefaultHelixTableDataManagerConfig(tableName);
    if (tableConfig != null) {
      tableDataManagerConfig.overrideConfigs(tableConfig);
    }
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, serverInstance);
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
  public synchronized void reloadSegment(@Nonnull SegmentMetadata segmentMetadata,
      @Nonnull CommonConstants.Helix.TableType tableType, @Nullable AbstractTableConfig tableConfig,
      @Nullable Schema schema)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    String tableName = segmentMetadata.getTableName();
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    } else {
      tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    }
    LOGGER.info("Reloading segment: {} in table: {}", segmentName, tableName);

    File indexDir = new File(segmentMetadata.getIndexDir());
    Preconditions.checkState(indexDir.isDirectory());
    File parentDir = indexDir.getParentFile();

    // Clean up all temporary index directories
    // This is for failure recovery
    File[] files = parentDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(RELOAD_TEMP_DIR_SUFFIX);
      }
    });
    Preconditions.checkNotNull(files);
    for (File file : files) {
      FileUtils.deleteQuietly(file);
    }

    // Copy the original segment to a temporary segment
    File tempIndexDir = new File(parentDir, indexDir.getName() + RELOAD_TEMP_DIR_SUFFIX);
    FileUtils.copyDirectory(indexDir, tempIndexDir);

    // Try to load with the temporary segment
    IndexSegment indexSegment;
    try {
      indexSegment =
          ColumnarSegmentLoader.load(tempIndexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig),
              schema);
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to reload the segment: {} in table: {}, abort the reloading",
          segmentName, tableName, e);
      FileUtils.deleteQuietly(tempIndexDir);
      return;
    }

    // Replace the old segment in memory
    _tableDataManagerMap.get(tableName).addSegment(indexSegment);

    // Replace the original index directory with the temporary one
    FileUtils.deleteQuietly(indexDir);
    FileUtils.moveDirectory(tempIndexDir, indexDir);
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
