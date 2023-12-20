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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This class in initialized per column and all the data is
 * sent to it before actual indexes are created
 * the job of this class is to collect
 * unique elements
 * record cardinality
 * compute min
 * compute max
 * see if column isSorted
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractColumnStatisticsCollector implements ColumnStatistics {
  protected static final int INITIAL_HASH_SET_SIZE = 1000;
  protected final FieldSpec _fieldSpec;

  private final Map<String, String> _partitionFunctionConfig;
  private final PartitionFunction _partitionFunction;
  private final int _numPartitions;
  private final Set<Integer> _partitions;

  protected int _totalNumberOfEntries = 0;
  protected int _maxNumberOfMultiValues = 0;
  protected boolean _sorted = true;
  private Comparable _previousValue = null;

  public AbstractColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    _fieldSpec = statsCollectorConfig.getFieldSpecForColumn(column);
    Preconditions.checkArgument(_fieldSpec != null, "Failed to find column: %s", column);
    if (!_fieldSpec.isSingleValueField()) {
      _sorted = false;
    }

    String partitionFunctionName = statsCollectorConfig.getPartitionFunctionName(column);
    _numPartitions = statsCollectorConfig.getNumPartitions(column);
    _partitionFunctionConfig = statsCollectorConfig.getPartitionFunctionConfig(column);
    _partitionFunction =
        (partitionFunctionName != null) ? PartitionFunctionFactory.getPartitionFunction(partitionFunctionName,
            _numPartitions, _partitionFunctionConfig) : null;
    if (_partitionFunction != null) {
      _partitions = new HashSet<>();
    } else {
      _partitions = null;
    }
  }

  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  protected void addressSorted(Comparable entry) {
    if (_sorted) {
      _sorted = _previousValue == null || entry.compareTo(_previousValue) >= 0;
      _previousValue = entry;
    }
  }

  @Override
  public boolean isSorted() {
    return _sorted;
  }

  /**
   * Collects statistics for the given entry (entry can be either single-valued or multi-valued).
   */
  public abstract void collect(Object entry);

  public int getLengthOfShortestElement() {
    return -1;
  }

  public int getLengthOfLargestElement() {
    return -1;
  }

  public abstract void seal();

  /**
   * Returns the {@link PartitionFunction} for the column.
   * @return Partition function for the column.
   */
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  /**
   * Returns the number of partitions for this column.
   *
   * @return Number of partitions.
   */
  public int getNumPartitions() {
    return _numPartitions;
  }

  /**
   * Returns the {@link PartitionFunction}'s functionConfig for the column.
   *
   * @return Partition Function config for the column.
   */
  @Nullable
  public Map<String, String> getPartitionFunctionConfig() {
    return _partitionFunctionConfig;
  }

  /**
   * Returns the partitions within which the column values exist.
   *
   * @return List of ranges for the column values.
   */
  @Nullable
  public Set<Integer> getPartitions() {
    return _partitions;
  }

  /**
   * Updates the partition range based on the partition of the given value.
   *
   * @param value Column value.
   */
  protected void updatePartition(Object value) {
    if (_partitionFunction != null) {
      _partitions.add(_partitionFunction.getValueToPartition(value));
    }
  }

  void updateTotalNumberOfEntries(Object[] entries) {
    _totalNumberOfEntries += entries.length;
  }

  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }
}
