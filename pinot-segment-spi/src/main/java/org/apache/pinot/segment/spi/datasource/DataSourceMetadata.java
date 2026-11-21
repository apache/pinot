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
package org.apache.pinot.segment.spi.datasource;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/// The `DataSourceMetadata` class contains all the metadata for a column for query execution purpose.
public interface DataSourceMetadata {

  /// Returns the field spec of the column.
  FieldSpec getFieldSpec();

  /// Returns the data type of the column.
  default FieldSpec.DataType getDataType() {
    return getFieldSpec().getDataType();
  }

  /// Returns `true` if the column is single-valued, `false` otherwise.
  default boolean isSingleValue() {
    return getFieldSpec().isSingleValueField();
  }

  /// Returns `true` if the column is sorted, `false` otherwise.
  ///
  /// The result of this method cannot be trusted if null handling is enabled.
  boolean isSorted();

  /// Returns the number of documents of the column.
  int getNumDocs();

  /// Returns the number of values of the column. For single-value column, number of values always equals to number of
  /// documents; for multi-value column, each document (multi-value entry) may contain multiple values.
  int getNumValues();

  /// Returns the maximum number of values for each multi-value entry of the multi-value column. Returns `-1` for
  /// single-value column.
  int getMaxNumValuesPerMVEntry();

  /// Returns the minimum value of the column, or `null` if it is not available.
  @Nullable
  Comparable getMinValue();

  /// Returns the maximum value of the column, or `null` if it is not available.
  @Nullable
  Comparable getMaxValue();

  /// Returns whether the available column bounds rule out the given predicate range.
  /// A null predicate endpoint or missing column bound leaves that side unbounded.
  @SuppressWarnings({"rawtypes", "unchecked"})
  default boolean isOutsideMinMaxRange(@Nullable Comparable lowerBound, boolean lowerInclusive,
      @Nullable Comparable upperBound, boolean upperInclusive) {
    if (upperBound != null) {
      Comparable min = getMinValue();
      if (min != null && upperBound.compareTo(min) < (upperInclusive ? 0 : 1)) {
        return true;
      }
    }
    if (lowerBound != null) {
      Comparable max = getMaxValue();
      if (max != null && lowerBound.compareTo(max) > (lowerInclusive ? 0 : -1)) {
        return true;
      }
    }
    return false;
  }

  /// Returns whether all values in an IN predicate fall outside the available column bounds.
  @SuppressWarnings({"rawtypes", "unchecked"})
  default boolean isOutsideMinMaxRange(List<? extends Comparable<?>> values) {
    if (values.isEmpty()) {
      return true;
    }
    Comparable min = getMinValue();
    Comparable max = getMaxValue();
    for (Comparable value : values) {
      if ((min == null || value.compareTo(min) >= 0) && (max == null || value.compareTo(max) <= 0)) {
        return false;
      }
    }
    return true;
  }

  /// Returns the partition function of the column, or `null` if the column is not partitioned.
  @Nullable
  PartitionFunction getPartitionFunction();

  /// Returns the partitions the column belongs to, or `null` if the column is not partitioned.
  @Nullable
  Set<Integer> getPartitions();

  /// Returns the cardinality of the column, `-1` if not applicable
  int getCardinality();

  /// Returns the max row length in bytes for a var byte MV column. `-1` if not applicable.
  /// @return
  default int getMaxRowLengthInBytes() {
    return -1;
  }
}
