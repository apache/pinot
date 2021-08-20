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

import java.util.HashSet;
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
public abstract class AbstractColumnStatisticsCollector implements ColumnStatistics {
  protected static final int INITIAL_HASH_SET_SIZE = 1000;
  private Object _previousValue = null;
  protected final FieldSpec _fieldSpec;
  private boolean _isSorted = true;
  private final String _column;

  protected int _totalNumberOfEntries = 0;
  protected int _maxNumberOfMultiValues = 0;
  private PartitionFunction _partitionFunction;
  private final int _numPartitions;
  private final Set<Integer> _partitions;

  public AbstractColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    _column = column;
    _fieldSpec = statsCollectorConfig.getFieldSpecForColumn(column);

    String partitionFunctionName = statsCollectorConfig.getPartitionFunctionName(column);
    int numPartitions = statsCollectorConfig.getNumPartitions(column);
    _partitionFunction =
        (partitionFunctionName != null) ? PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions) : null;

    _numPartitions = statsCollectorConfig.getNumPartitions(column);
    if (_partitionFunction != null) {
      _partitions = new HashSet<>();
    } else {
      _partitions = null;
    }
  }

  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  void addressSorted(Object entry) {
    if (_isSorted) {
      if (_previousValue != null) {
        if (!entry.equals(_previousValue) && _previousValue != null) {
          final Comparable prevValue = (Comparable) _previousValue;
          final Comparable origin = (Comparable) entry;
          if (origin.compareTo(prevValue) < 0) {
            _isSorted = false;
          }
        }
      }
      _previousValue = entry;
    }
  }

  @Override
  public boolean isSorted() {
    return _fieldSpec.isSingleValueField() && _isSorted;
  }

  /**
   * Collects statistics for the given entry (entry can be either single-valued or multi-valued).
   */
  public abstract void collect(Object entry);

  public abstract Object getMinValue();

  public abstract Object getMaxValue();

  public abstract Object getUniqueValuesSet();

  public abstract int getCardinality();

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
      _partitions.add(_partitionFunction.getPartition(value));
    }
  }

  void updateTotalNumberOfEntries(Object[] entries) {
    _totalNumberOfEntries += entries.length;
  }

  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }
}
