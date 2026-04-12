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
package org.apache.pinot.core.data.manager.ingest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Routes rows to partitions for INSERT INTO operations.
 *
 * <p>For upsert/dedup tables, rows are routed by the primary key column using the table's
 * configured partition function. For append-only tables, rows are distributed via round-robin
 * across the configured number of partitions.
 *
 * <p>This class validates that upsert/dedup tables have proper partition configuration and
 * rejects inserts if the configuration is missing.
 *
 * <p>This class is thread-safe.
 */
public class InsertPartitionRouter {

  private final int _numPartitions;
  private final boolean _isUpsertOrDedup;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final AtomicInteger _roundRobinCounter = new AtomicInteger(0);

  /**
   * Creates a partition router from the given table configuration.
   *
   * @param tableConfig the table configuration
   * @throws IllegalArgumentException if upsert/dedup tables lack partition configuration
   */
  public InsertPartitionRouter(TableConfig tableConfig) {
    _isUpsertOrDedup = isUpsertOrDedup(tableConfig);

    SegmentPartitionConfig partitionConfig = getSegmentPartitionConfig(tableConfig);
    if (partitionConfig != null && !partitionConfig.getColumnPartitionMap().isEmpty()) {
      if (partitionConfig.getColumnPartitionMap().size() > 1) {
        throw new IllegalArgumentException(
            "INSERT INTO does not support multi-column partition configs. "
                + "Found partition columns: " + partitionConfig.getColumnPartitionMap().keySet());
      }
      Map.Entry<String, ColumnPartitionConfig> entry =
          partitionConfig.getColumnPartitionMap().entrySet().iterator().next();
      _partitionColumn = entry.getKey();
      ColumnPartitionConfig colConfig = entry.getValue();
      _numPartitions = colConfig.getNumPartitions();
      _partitionFunction = PartitionFunctionFactory.getPartitionFunction(
          colConfig.getFunctionName(), _numPartitions, colConfig.getFunctionConfig());
    } else if (_isUpsertOrDedup) {
      throw new IllegalArgumentException(
          "Upsert/dedup tables require a partition configuration for INSERT INTO operations. "
              + "Configure segmentPartitionConfig in the table's indexingConfig.");
    } else {
      // Default to single partition for append-only tables without partition config
      _partitionColumn = null;
      _partitionFunction = null;
      _numPartitions = 1;
    }
  }

  /**
   * Creates a partition router with explicit configuration. Useful for testing.
   *
   * @param numPartitions the number of partitions
   * @param partitionColumn the column used for partitioning, or {@code null} for round-robin
   * @param partitionFunction the partition function, or {@code null} for round-robin
   * @param isUpsertOrDedup whether the table uses upsert or dedup
   */
  public InsertPartitionRouter(int numPartitions, @Nullable String partitionColumn,
      @Nullable PartitionFunction partitionFunction, boolean isUpsertOrDedup) {
    _numPartitions = numPartitions;
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _isUpsertOrDedup = isUpsertOrDedup;
  }

  /**
   * Routes a single row to its target partition.
   *
   * @param row the row to route
   * @return the partition id
   */
  public int getPartition(GenericRow row) {
    if (_partitionFunction != null && _partitionColumn != null) {
      Object value = row.getValue(_partitionColumn);
      String valueStr = value != null ? value.toString() : "";
      return _partitionFunction.getPartition(valueStr);
    }
    // Round-robin for tables without partition function
    return Math.floorMod(_roundRobinCounter.getAndIncrement(), _numPartitions);
  }

  /**
   * Routes a list of rows to their target partitions and returns them grouped by partition id.
   *
   * @param rows the rows to route
   * @return a map from partition id to the list of rows for that partition
   */
  public Map<Integer, List<GenericRow>> routeRows(List<GenericRow> rows) {
    Map<Integer, List<GenericRow>> partitionedRows = new HashMap<>();
    for (GenericRow row : rows) {
      int partitionId = getPartition(row);
      partitionedRows.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(row);
    }
    return partitionedRows;
  }

  /**
   * Returns the number of partitions.
   */
  public int getNumPartitions() {
    return _numPartitions;
  }

  /**
   * Returns whether the table uses upsert or dedup.
   */
  public boolean isUpsertOrDedup() {
    return _isUpsertOrDedup;
  }

  /**
   * Returns the partition column name, or {@code null} if round-robin is used.
   */
  @Nullable
  public String getPartitionColumn() {
    return _partitionColumn;
  }

  private static boolean isUpsertOrDedup(TableConfig tableConfig) {
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE) {
      return true;
    }
    return tableConfig.getDedupConfig() != null && tableConfig.getDedupConfig().isDedupEnabled();
  }

  @Nullable
  private static SegmentPartitionConfig getSegmentPartitionConfig(TableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig == null) {
      return null;
    }
    return indexingConfig.getSegmentPartitionConfig();
  }
}
