/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.offline;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


/**
 * Dimension Table is a special type of OFFLINE table which is assigned to all servers
 * in a tenant and is used to execute a LOOKUP Transform Function. DimensionTableDataManager
 * loads the contents into a HashMap for faster access thus the size should be small
 * enough to easily fit in memory.
 *
 * DimensionTableDataManager uses Registry of Singletons pattern to store one instance per table
 * which can be accessed via {@link #getInstanceByTableName} static method.
 */
@ThreadSafe
public class DimensionTableDataManager extends OfflineTableDataManager {
  // Storing singletons per table in a HashMap
  private static final Map<String, DimensionTableDataManager> INSTANCES = new ConcurrentHashMap<>();

  private DimensionTableDataManager() {
  }

  /**
   * `createInstanceByTableName` should only be used by the {@link TableDataManagerProvider} and the returned
   * instance should be properly initialized via {@link #init} method before using.
   */
  public static DimensionTableDataManager createInstanceByTableName(String tableNameWithType) {
    return INSTANCES.computeIfAbsent(tableNameWithType, k -> new DimensionTableDataManager());
  }

  @VisibleForTesting
  public static DimensionTableDataManager registerDimensionTable(String tableNameWithType,
      DimensionTableDataManager instance) {
    return INSTANCES.computeIfAbsent(tableNameWithType, k -> instance);
  }

  public static DimensionTableDataManager getInstanceByTableName(String tableNameWithType) {
    return INSTANCES.get(tableNameWithType);
  }

  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<DimensionTableDataManager, Map> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(DimensionTableDataManager.class, Map.class, "_lookupTable");
  private volatile Map<PrimaryKey, GenericRow> _lookupTable = new HashMap<>();
  private Schema _tableSchema;
  private List<String> _primaryKeyColumns;

  @Override
  protected void doInit() {
    super.doInit();

    // dimension tables should always have schemas with primary keys
    _tableSchema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    _primaryKeyColumns = _tableSchema.getPrimaryKeyColumns();
  }

  @Override
  public void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    super.addSegment(indexDir, indexLoadingConfig);
    try {
      loadLookupTable();
      _logger.info("Successfully added segment {} and loaded lookup table: {}", indexDir.getName(), getTableName());
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error loading lookup table: %s", getTableName()), e);
    }
  }

  @Override
  public void removeSegment(String segmentName) {
    super.removeSegment(segmentName);
    try {
      loadLookupTable();
      _logger.info("Successfully removed segment {} and reloaded lookup table: {}", segmentName, getTableName());
    } catch (Exception e) {
      throw new RuntimeException(String
          .format("Error reloading lookup table after segment remove '%s' for table: '%s'", segmentName,
              getTableName()), e);
    }
  }

  /**
   * `loadLookupTable()` reads contents of the DimensionTable into _lookupTable HashMap for fast lookup.
   */
  private void loadLookupTable()
      throws Exception {
    Map<PrimaryKey, GenericRow> snapshot;
    Map<PrimaryKey, GenericRow> replacement;
    do {
      snapshot = _lookupTable;
      replacement = new HashMap<>(snapshot.size());
      populate(replacement);
    } while (!UPDATER.compareAndSet(this, snapshot, replacement));
  }

  private void populate(Map<PrimaryKey, GenericRow> map)
      throws Exception {
    List<SegmentDataManager> segmentManagers = acquireAllSegments();
    try {
      for (SegmentDataManager segmentManager : segmentManagers) {
        IndexSegment indexSegment = segmentManager.getSegment();
        try (PinotSegmentRecordReader reader = new PinotSegmentRecordReader(
            indexSegment.getSegmentMetadata().getIndexDir())) {
          while (reader.hasNext()) {
            GenericRow row = reader.next();
            map.put(row.getPrimaryKey(_primaryKeyColumns), row);
          }
        }
      }
    } finally {
      for (SegmentDataManager segmentManager : segmentManagers) {
        releaseSegment(segmentManager);
      }
    }
  }

  public GenericRow lookupRowByPrimaryKey(PrimaryKey pk) {
    return _lookupTable.get(pk);
  }

  public FieldSpec getColumnFieldSpec(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }
}
