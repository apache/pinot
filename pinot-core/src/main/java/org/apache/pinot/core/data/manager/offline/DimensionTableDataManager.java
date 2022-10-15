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
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
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

  // Storing singletons per table in a map
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

  private static final AtomicReferenceFieldUpdater<DimensionTableDataManager, DimensionTable> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(DimensionTableDataManager.class, DimensionTable.class, "_dimensionTable");

  private volatile DimensionTable _dimensionTable;

  @Override
  protected void doInit() {
    super.doInit();
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);
    _dimensionTable = new DimensionTable(schema, primaryKeyColumns);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    super.addSegment(immutableSegment);
    String segmentName = immutableSegment.getSegmentName();
    try {
      loadLookupTable();
      _logger.info("Successfully loaded lookup table: {} after adding segment: {}", _tableNameWithType, segmentName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after adding segment: %s", _tableNameWithType,
              segmentName), e);
    }
  }

  @Override
  public void removeSegment(String segmentName) {
    super.removeSegment(segmentName);
    try {
      loadLookupTable();
      _logger.info("Successfully loaded lookup table: {} after removing segment: {}", _tableNameWithType, segmentName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after removing segment: %s",
              _tableNameWithType, segmentName), e);
    }
  }

  /**
   * `loadLookupTable()` reads contents of the DimensionTable into _lookupTable HashMap for fast lookup.
   */
  private void loadLookupTable() {
    DimensionTable snapshot;
    DimensionTable replacement;
    do {
      snapshot = _dimensionTable;
      replacement = createDimensionTable();
    } while (!UPDATER.compareAndSet(this, snapshot, replacement));
  }

  private DimensionTable createDimensionTable() {
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);

    Map<PrimaryKey, GenericRow> lookupTable = new HashMap<>();
    List<SegmentDataManager> segmentManagers = acquireAllSegments();
    try {
      for (SegmentDataManager segmentManager : segmentManagers) {
        IndexSegment indexSegment = segmentManager.getSegment();
        int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
        if (numTotalDocs > 0) {
          try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
            recordReader.init(indexSegment);
            for (int i = 0; i < numTotalDocs; i++) {
              GenericRow row = new GenericRow();
              recordReader.getRecord(i, row);
              lookupTable.put(row.getPrimaryKey(primaryKeyColumns), row);
            }
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while reading records from segment: " + indexSegment.getSegmentName());
          }
        }
      }
      return new DimensionTable(schema, primaryKeyColumns, lookupTable);
    } finally {
      for (SegmentDataManager segmentManager : segmentManagers) {
        releaseSegment(segmentManager);
      }
    }
  }

  public boolean isPopulated() {
    return !_dimensionTable.isEmpty();
  }

  public GenericRow lookupRowByPrimaryKey(PrimaryKey pk) {
    return _dimensionTable.get(pk);
  }

  public FieldSpec getColumnFieldSpec(String columnName) {
    return _dimensionTable.getFieldSpecFor(columnName);
  }

  public List<String> getPrimaryKeyColumns() {
    return _dimensionTable.getPrimaryKeyColumns();
  }
}
