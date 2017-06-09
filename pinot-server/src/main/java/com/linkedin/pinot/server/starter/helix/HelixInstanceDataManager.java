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
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.config.TableConfig;
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
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  public synchronized void addSegment(@Nonnull SegmentMetadata segmentMetadata, @Nullable TableConfig tableConfig,
      @Nullable Schema schema)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    String tableName = segmentMetadata.getTableName();
    LOGGER.info("Trying to add segment: {} to OFFLINE table: {}", segmentName, tableName);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Trying to add segment with Metadata: " + segmentMetadata.toString());
    }
    if (segmentMetadata.getIndexType().equalsIgnoreCase("realtime")) {
      tableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    } else {
      tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
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
      @Nonnull TableConfig tableConfig, @Nullable InstanceZKMetadata instanceZKMetadata,
      @Nonnull SegmentZKMetadata segmentZKMetadata, @Nonnull String serverInstance)
      throws Exception {
    String segmentName = segmentZKMetadata.getSegmentName();
    String tableName = segmentZKMetadata.getTableName();
    LOGGER.info("Trying to add segment: {} to REALTIME table: {}", segmentName, tableName);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Trying to add segment with Metadata: " + segmentZKMetadata.toString());
    }
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      tableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    } else {
      tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
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

  public synchronized void addTableIfNeed(@Nullable TableConfig tableConfig, @Nonnull String tableName,
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
  public synchronized void reloadSegment(@Nonnull String tableNameWithType, @Nonnull SegmentMetadata segmentMetadata,
      @Nullable TableConfig tableConfig, @Nullable Schema schema)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Reloading segment: {} in table: {}", segmentName, tableNameWithType);

    File indexDir = new File(segmentMetadata.getIndexDir());
    Preconditions.checkState(indexDir.isDirectory(), "Index directory: %s is not a directory", indexDir);

    File parentFile = indexDir.getParentFile();
    File segmentBackupDir =
        new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    try {
      // First rename index directory to segment backup directory so that original segment have all file descriptors
      // point to the segment backup directory to ensure original segment serves queries properly

      // Rename index directory to segment backup directory (atomic)
      Preconditions.checkState(indexDir.renameTo(segmentBackupDir),
          "Failed to rename index directory: %s to segment backup directory: %s", indexDir, segmentBackupDir);

      // Copy from segment backup directory back to index directory
      FileUtils.copyDirectory(segmentBackupDir, indexDir);

      // Load from index directory
      IndexSegment indexSegment =
          ColumnarSegmentLoader.load(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig), schema);

      // Replace the old segment in memory
      _tableDataManagerMap.get(tableNameWithType).addSegment(indexSegment);

      // Rename segment backup directory to segment temporary directory (atomic)
      // The reason to first rename then delete is that, renaming is an atomic operation, but deleting is not. When we
      // rename the segment backup directory to segment temporary directory, we know the reload already succeeded, so
      // that we can safely delete the segment temporary directory
      File segmentTempDir = new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
      Preconditions.checkState(segmentBackupDir.renameTo(segmentTempDir),
          "Failed to rename segment backup directory: %s to segment temporary directory: %s", segmentBackupDir,
          segmentTempDir);
      LOGGER.info("Reloaded segment: {} in table: {}", segmentName, tableNameWithType);

      // Delete segment temporary directory
      FileUtils.deleteDirectory(segmentTempDir);
    } finally {
      LoaderUtils.reloadFailureRecovery(indexDir);
    }
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

  @Nonnull
  @Override
  public List<SegmentMetadata> getAllSegmentsMetadata(@Nonnull String tableNameWithType) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      return Collections.emptyList();
    } else {
      ImmutableList<SegmentDataManager> segmentDataManagers = null;
      try {
        segmentDataManagers = tableDataManager.acquireAllSegments();
        List<SegmentMetadata> segmentsMetadata = new ArrayList<>(segmentDataManagers.size());
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          segmentsMetadata.add(segmentDataManager.getSegment().getSegmentMetadata());
        }
        return segmentsMetadata;
      } finally {
        if (segmentDataManagers != null) {
          for (SegmentDataManager segmentDataManager : segmentDataManagers) {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      }
    }
  }

  @Nullable
  @Override
  public SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName) {
    SegmentDataManager segmentDataManager = null;
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
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
