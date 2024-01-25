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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
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

  private final AtomicReference<DimensionTable> _dimensionTable = new AtomicReference<>();

  // Assign a token when loading the lookup table, cancel the loading when token changes because we will load it again
  // anyway
  private final AtomicInteger _loadToken = new AtomicInteger();

  private boolean _disablePreload = false;
  private boolean _errorOnDuplicatePrimaryKey = false;

  @Override
  protected void doInit() {
    super.doInit();
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, _tableNameWithType);
    if (tableConfig != null) {
      DimensionTableConfig dimensionTableConfig = tableConfig.getDimensionTableConfig();
      if (dimensionTableConfig != null) {
        _disablePreload = dimensionTableConfig.isDisablePreload();
        _errorOnDuplicatePrimaryKey = dimensionTableConfig.isErrorOnDuplicatePrimaryKey();
      }
    }

    if (_disablePreload) {
      _dimensionTable.set(
          new MemoryOptimizedDimensionTable(schema, primaryKeyColumns, Collections.emptyMap(), Collections.emptyList(),
              Collections.emptyList(), this));
    } else {
      _dimensionTable.set(new FastLookupDimensionTable(schema, primaryKeyColumns, new HashMap<>()));
    }
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown, "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);
    super.addSegment(immutableSegment);
    try {
      if (loadLookupTable()) {
        _logger.info("Successfully loaded lookup table after adding segment: {}", segmentName);
      } else {
        _logger.info("Skip loading lookup table after adding segment: {}, another loading in progress", segmentName);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after adding segment: %s", _tableNameWithType,
              segmentName), e);
    }
  }

  @Override
  public void removeSegment(String segmentName) {
    // Allow removing segment after shutdown so that we can remove the segment when the table is deleted
    if (_shutDown) {
      _logger.info("Table data manager is already shut down, skip removing segment: {}", segmentName);
      return;
    }
    super.removeSegment(segmentName);
    try {
      if (loadLookupTable()) {
        _logger.info("Successfully loaded lookup table after removing segment: {}", segmentName);
      } else {
        _logger.info("Skip loading lookup table after removing segment: {}, another loading in progress", segmentName);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after removing segment: %s",
              _tableNameWithType, segmentName), e);
    }
  }

  @Override
  protected void doShutdown() {
    releaseAndRemoveAllSegments();
    closeDimensionTable(_dimensionTable.get());
  }

  private void closeDimensionTable(DimensionTable dimensionTable) {
    try {
      dimensionTable.close();
    } catch (Exception e) {
      _logger.error("Caught exception while closing the dimension table", e);
    }
  }

  /**
   * `loadLookupTable()` reads contents of the DimensionTable into _lookupTable HashMap for fast lookup.
   */
  private boolean loadLookupTable() {
    DimensionTable dimensionTable =
        _disablePreload ? createMemOptimisedDimensionTable() : createFastLookupDimensionTable();
    if (dimensionTable != null) {
      closeDimensionTable(_dimensionTable.getAndSet(dimensionTable));
      return true;
    } else {
      return false;
    }
  }

  @Nullable
  private DimensionTable createFastLookupDimensionTable() {
    // Acquire a token in the beginning. Abort the loading and return null when the token changes because another
    // loading is in progress.
    int token = _loadToken.incrementAndGet();

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);

    Map<PrimaryKey, GenericRow> lookupTable = new HashMap<>();
    List<SegmentDataManager> segmentDataManagers = acquireAllSegments();
    try {
      for (SegmentDataManager segmentManager : segmentDataManagers) {
        IndexSegment indexSegment = segmentManager.getSegment();
        int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
        if (numTotalDocs > 0) {
          try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
            recordReader.init(indexSegment);
            for (int i = 0; i < numTotalDocs; i++) {
              if (_loadToken.get() != token) {
                // Token changed during the loading, abort the loading
                return null;
              }
              GenericRow row = new GenericRow();
              recordReader.getRecord(i, row);
              GenericRow previousRow = lookupTable.put(row.getPrimaryKey(primaryKeyColumns), row);
              if (_errorOnDuplicatePrimaryKey && previousRow != null) {
                throw new IllegalStateException(
                    "Caught exception while reading records from segment: " + indexSegment.getSegmentName()
                        + "primary key already exist for: " + row.getPrimaryKey(primaryKeyColumns));
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while reading records from segment: " + indexSegment.getSegmentName());
          }
        }
      }
      return new FastLookupDimensionTable(schema, primaryKeyColumns, lookupTable);
    } finally {
      for (SegmentDataManager segmentManager : segmentDataManagers) {
        releaseSegment(segmentManager);
      }
    }
  }

  @Nullable
  private DimensionTable createMemOptimisedDimensionTable() {
    // Acquire a token in the beginning. Abort the loading and return null when the token changes because another
    // loading is in progress.
    int token = _loadToken.incrementAndGet();

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);
    int numPrimaryKeyColumns = primaryKeyColumns.size();

    Map<PrimaryKey, LookupRecordLocation> lookupTable = new HashMap<>();
    List<SegmentDataManager> segmentDataManagers = acquireAllSegments();
    List<PinotSegmentRecordReader> recordReaders = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentManager : segmentDataManagers) {
      IndexSegment indexSegment = segmentManager.getSegment();
      int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
      if (numTotalDocs > 0) {
        try {
          PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
          recordReader.init(indexSegment);
          recordReaders.add(recordReader);
          for (int i = 0; i < numTotalDocs; i++) {
            if (_loadToken.get() != token) {
              // Token changed during the loading, abort the loading
              for (PinotSegmentRecordReader reader : recordReaders) {
                try {
                  reader.close();
                } catch (Exception e) {
                  _logger.error("Caught exception while closing record reader for segment: {}", reader.getSegmentName(),
                      e);
                }
              }
              for (SegmentDataManager dataManager : segmentDataManagers) {
                releaseSegment(dataManager);
              }
              return null;
            }
            Object[] values = new Object[numPrimaryKeyColumns];
            for (int j = 0; j < numPrimaryKeyColumns; j++) {
              values[j] = recordReader.getValue(i, primaryKeyColumns.get(j));
            }
            lookupTable.put(new PrimaryKey(values), new LookupRecordLocation(recordReader, i));
          }
        } catch (Exception e) {
          throw new RuntimeException(
              "Caught exception while reading records from segment: " + indexSegment.getSegmentName());
        }
      }
    }
    return new MemoryOptimizedDimensionTable(schema, primaryKeyColumns, lookupTable, segmentDataManagers, recordReaders,
        this);
  }

  public boolean isPopulated() {
    return !_dimensionTable.get().isEmpty();
  }

  public GenericRow lookupRowByPrimaryKey(PrimaryKey pk) {
    return _dimensionTable.get().get(pk);
  }

  public FieldSpec getColumnFieldSpec(String columnName) {
    return _dimensionTable.get().getFieldSpecFor(columnName);
  }

  public List<String> getPrimaryKeyColumns() {
    return _dimensionTable.get().getPrimaryKeyColumns();
  }
}
