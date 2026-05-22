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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


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
  protected final DataType _storedType;
  protected final FieldConfig _fieldConfig;
  protected final PartitionFunction _partitionFunction;
  protected final Set<Integer> _partitions;

  protected int _totalDocs = 0;
  protected int _totalNumberOfEntries = 0;
  protected int _maxNumberOfMultiValues = 0;
  protected boolean _sorted;
  protected Comparable _previousValue = null;

  public AbstractColumnStatisticsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    _fieldSpec = statsCollectorConfig.getFieldSpecForColumn(column);
    Preconditions.checkArgument(_fieldSpec != null, "Failed to find column: %s", column);
    _storedType = _fieldSpec.getDataType().getStoredType();
    _sorted = _fieldSpec.isSingleValueField();
    _fieldConfig = statsCollectorConfig.getFieldConfigForColumn(column);
    _partitionFunction = statsCollectorConfig.getPartitionFunction(column);
    _partitions = _partitionFunction != null ? new HashSet<>() : null;
  }

  /**
   * Collects statistics for the given entry (entry can be either single-valued or multi-valued).
   */
  public abstract void collect(Object entry);

  // Collects statistics for primitive values
  // Default implementation boxes the value for backward compatibility.
  public void collect(int value) {
    collect((Object) value);
  }

  public void collect(long value) {
    collect((Object) value);
  }

  public void collect(float value) {
    collect((Object) value);
  }

  public void collect(double value) {
    collect((Object) value);
  }

  public void collect(int[] values) {
    collect((Object) values);
  }

  public void collect(long[] values) {
    collect((Object) values);
  }

  public void collect(float[] values) {
    collect((Object) values);
  }

  public void collect(double[] values) {
    collect((Object) values);
  }

  public abstract void seal();

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public boolean isSorted() {
    return _sorted;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    return _partitions;
  }

  protected void addressSorted(Comparable value) {
    if (_sorted) {
      _sorted = _previousValue == null || value.compareTo(_previousValue) >= 0;
      _previousValue = value;
    }
  }

  protected void updateTotalNumberOfEntries(int newEntries) {
    _totalNumberOfEntries += newEntries;
  }

  protected void updateTotalNumberOfEntries(Object[] entries) {
    _totalNumberOfEntries += entries.length;
  }

  protected boolean isPartitionEnabled() {
    return _partitionFunction != null;
  }

  protected void updatePartition(String value) {
    _partitions.add(_partitionFunction.getPartition(value));
  }
}
