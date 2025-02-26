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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
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
import org.apache.pinot.spi.utils.JsonUtils;


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
  public static final Hash.Strategy<Object[]> HASH_STRATEGY = new Hash.Strategy<>() {
    @Override
    public int hashCode(Object[] o) {
      return Arrays.hashCode(o);
    }

    @Override
    public boolean equals(Object[] a, Object[] b) {
      if (a == b) {
        return true;
      } else {
        return Arrays.equals(a, b);
      }
    }
  };

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
      List<String> valueColumns = getValueColumns(schema.getColumnNames(), primaryKeyColumns);

      Object2ObjectOpenCustomHashMap<Object[], Object[]> lookupTable =
          new Object2ObjectOpenCustomHashMap<>(HASH_STRATEGY);

      _dimensionTable.set(new FastLookupDimensionTable(schema, primaryKeyColumns, valueColumns, lookupTable));
    }
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    super.addSegment(immutableSegment);
    String segmentName = immutableSegment.getSegmentName();
    if (loadLookupTable()) {
      _logger.info("Successfully loaded lookup table after adding segment: {}", segmentName);
    } else {
      _logger.info("Skip loading lookup table after adding segment: {}, another loading in progress", segmentName);
    }
  }

  @Override
  protected void doOffloadSegment(String segmentName) {
    SegmentDataManager segmentDataManager = unregisterSegment(segmentName);
    if (segmentDataManager != null) {
      segmentDataManager.offload();
      releaseSegment(segmentDataManager);
      _logger.info("Offloaded segment: {}", segmentName);
      if (loadLookupTable()) {
        _logger.info("Successfully loaded lookup table after offloading segment: {}", segmentName);
      } else {
        _logger.info("Skip loading lookup table after offloading segment: {}, another loading in progress",
            segmentName);
      }
    } else {
      _logger.warn("Failed to find segment: {}, skipping offloading it", segmentName);
    }
  }

  @Override
  protected void doShutdown() {
    releaseAndRemoveAllSegments();
    closeDimensionTable(_dimensionTable.get());
  }

  /* This method is necessary for testing because shutdown doesn't release
   * _dimensionTable and DimensionTableDataManager instance is always kept in INSTANCES,
   * point to a possibly big eagerly-loaded data structure.
   */
  @VisibleForTesting
  public void releaseDimensionTable() {
    closeDimensionTable(_dimensionTable.getAndSet(null));
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

    List<SegmentDataManager> segmentDataManagers = acquireAllSegments();
    try {
      // count all documents to limit map re-sizings
      int totalDocs = 0;
      for (SegmentDataManager segmentManager : segmentDataManagers) {
        IndexSegment indexSegment = segmentManager.getSegment();
        totalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
      }

      Object2ObjectOpenCustomHashMap<Object[], Object[]> lookupTable =
          new Object2ObjectOpenCustomHashMap<>(totalDocs, HASH_STRATEGY);

      List<String> valueColumns = getValueColumns(schema.getColumnNames(), primaryKeyColumns);

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

              //TODO: consider inlining primary keys and values tables to reduce memory overhead
              Object[] primaryKey = recordReader.getRecordValues(i, primaryKeyColumns);
              Object[] values = recordReader.getRecordValues(i, valueColumns);

              Object[] previousValue = lookupTable.put(primaryKey, values);
              if (_errorOnDuplicatePrimaryKey && previousValue != null) {
                throw new IllegalStateException(
                    "Caught exception while reading records from segment: " + indexSegment.getSegmentName()
                        + "primary key already exist for: " + toString(primaryKey));
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while reading records from segment: " + indexSegment.getSegmentName(), e);
          }
        }
      }
      return new FastLookupDimensionTable(schema, primaryKeyColumns, valueColumns, lookupTable);
    } finally {
      for (SegmentDataManager segmentManager : segmentDataManagers) {
        releaseSegment(segmentManager);
      }
    }
  }

  private static String toString(Object[] primaryKey) {
    try {
      return JsonUtils.objectToPrettyString(primaryKey);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> getValueColumns(NavigableSet<String> columnNames, List<String> primaryKeyColumns) {
    List<String> nonPkColumns = new ArrayList<>(columnNames.size() - primaryKeyColumns.size());
    for (String columnName : columnNames) {
      if (!primaryKeyColumns.contains(columnName)) {
        nonPkColumns.add(columnName);
      }
    }
    return nonPkColumns;
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

    List<SegmentDataManager> segmentDataManagers = acquireAllSegments();
    List<PinotSegmentRecordReader> recordReaders = new ArrayList<>(segmentDataManagers.size());

    int totalDocs = 0;
    for (SegmentDataManager segmentManager : segmentDataManagers) {
      IndexSegment indexSegment = segmentManager.getSegment();
      totalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
    }

    Map<PrimaryKey, LookupRecordLocation> lookupTable = new HashMap<>(totalDocs);

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

  public boolean containsKey(PrimaryKey pk) {
    return _dimensionTable.get().containsKey(pk);
  }

  @Nullable
  public GenericRow lookupRow(PrimaryKey pk) {
    return _dimensionTable.get().getRow(pk);
  }

  @Nullable
  public Object lookupValue(PrimaryKey pk, String columnName) {
    return _dimensionTable.get().getValue(pk, columnName);
  }

  @Nullable
  public Object[] lookupValues(PrimaryKey pk, String[] columnNames) {
    return _dimensionTable.get().getValues(pk, columnNames);
  }

  @Nullable
  public FieldSpec getColumnFieldSpec(String columnName) {
    return _dimensionTable.get().getFieldSpecFor(columnName);
  }

  public List<String> getPrimaryKeyColumns() {
    return _dimensionTable.get().getPrimaryKeyColumns();
  }
}
