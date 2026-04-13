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
package org.apache.pinot.segment.spi.creator;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Statistics for a column, collected before creating indexes.
/// For MV column, the stats are on individual elements within the MV values.
@SuppressWarnings("rawtypes")
public interface ColumnStatistics extends Serializable {

  /// Returns the [FieldSpec] of the column.
  FieldSpec getFieldSpec();

  /// Returns the stored [DataType] of the column.
  default DataType getValueType() {
    return getFieldSpec().getDataType().getStoredType();
  }

  /// Returns whether the column is single-valued.
  default boolean isSingleValue() {
    return getFieldSpec().isSingleValueField();
  }

  /// Returns the minimum value, or `null` if the column is empty.
  @Nullable
  Comparable getMinValue();

  /// Returns the maximum value, or `null` if the column is empty.
  @Nullable
  Comparable getMaxValue();

  /// Returns a sorted (ascending) array of all unique values for non-empty dictionary-encoded columns, `null`
  /// otherwise.
  /// - INT: int[]
  /// - LONG: long[]
  /// - FLOAT: float[]
  /// - DOUBLE: double[]
  /// - BIG_DECIMAL: BigDecimal[]
  /// - STRING: String[]
  /// - BYTES: ByteArray[]
  @Nullable
  Object getUniqueValuesSet();

  /// Returns the cardinality of the column.
  /// - For dictionary-encoded columns, returns the exact cardinality.
  /// - For raw columns:
  ///   - When stats are collected from an input file, returns an estimated cardinality
  ///   - When stats are derived from a mutable segment, returns
  ///     [org.apache.pinot.segment.spi.Constants#UNKNOWN_CARDINALITY]
  // TODO: Consider generating estimated cardinality when converting from a mutable segment
  int getCardinality();

  /// Returns the minimum serialized byte length across all elements.
  /// - For fixed-width types, returns the fixed size
  /// - For empty column of var-length types, returns `0`
  /// Override it for var-length types.
  default int getLengthOfShortestElement() {
    return getValueType().size();
  }

  /// Returns the maximum serialized byte length across all elements.
  /// - For fixed-width types, returns the fixed size
  /// - For empty column of var-length types, returns `0`
  /// Override it for var-length types.
  default int getLengthOfLongestElement() {
    return getValueType().size();
  }

  @Deprecated
  default int getLengthOfLargestElement() {
    return getLengthOfLongestElement();
  }

  /// Returns `true` when all the elements are of the same serialized byte length.
  default boolean isFixedLength() {
    return getLengthOfShortestElement() == getLengthOfLongestElement();
  }

  /// Returns `true` when all elements of a non-empty STRING column contain only ASCII characters, `false` otherwise.
  /// Override it for STRING type.
  default boolean isAscii() {
    return false;
  }

  /// Returns `true` when all values in a SV column are sorted in ascending order, `false` otherwise.
  boolean isSorted();

  /// Returns the total number of entries.
  /// - For SV columns, equals the number of rows
  /// - For MV columns, sums the number of elements across all rows
  int getTotalNumberOfEntries();

  /// Returns the maximum number of elements within a single MV value, or `0` for SV or empty columns.
  int getMaxNumberOfMultiValues();

  /// Returns the maximum serialized byte length of a single row.
  /// - For SV columns, row length is the same as value length
  /// - For MV columns, row length is total length of all elements within the MV value
  /// - Returns `0` for empty columns
  /// Override it for MV var-length types.
  default int getMaxRowLengthInBytes() {
    if (isSingleValue()) {
      return getLengthOfLongestElement();
    } else {
      return getMaxNumberOfMultiValues() * getValueType().size();
    }
  }

  /// Returns the [PartitionFunction] for the column.
  @Nullable
  PartitionFunction getPartitionFunction();

  /// Returns the partitions within which the values exist.
  @Nullable
  Set<Integer> getPartitions();

  @Deprecated
  default int getNumPartitions() {
    PartitionFunction partitionFunction = getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getNumPartitions() : 0;
  }

  @Deprecated
  @Nullable
  default Map<String, String> getPartitionFunctionConfig() {
    PartitionFunction partitionFunction = getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getFunctionConfig() : null;
  }
}
