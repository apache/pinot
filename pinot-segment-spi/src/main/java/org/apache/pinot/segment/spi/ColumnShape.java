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
package org.apache.pinot.segment.spi;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;


/// The shape (identity, value distribution, and element layout) of a column.
public interface ColumnShape {

  /// Returns the [FieldSpec] of the column.
  FieldSpec getFieldSpec();

  /// Returns the name of the column.
  default String getColumnName() {
    return getFieldSpec().getName();
  }

  /// Returns the [FieldType] of the column.
  default FieldType getFieldType() {
    return getFieldSpec().getFieldType();
  }

  /// Returns the [DataType] of the column.
  default DataType getDataType() {
    return getFieldSpec().getDataType();
  }

  /// Returns the stored [DataType] of the column.
  default DataType getStoredType() {
    return getDataType().getStoredType();
  }

  /// Returns whether the column is single-valued.
  default boolean isSingleValue() {
    return getFieldSpec().isSingleValueField();
  }

  /// Returns the total number of rows (documents).
  int getTotalDocs();

  /// Returns the cardinality of the column, or [Constants#UNKNOWN_CARDINALITY] if unavailable.
  int getCardinality();

  /// Returns `true` when all values in a SV column are sorted in ascending order, `false` otherwise.
  boolean isSorted();

  /// Returns the minimum value, or `null` if unavailable.
  @Nullable
  Comparable<?> getMinValue();

  /// Returns the maximum value, or `null` if unavailable.
  @Nullable
  Comparable<?> getMaxValue();

  /// Returns the minimum serialized byte length across all elements.
  /// - For fixed-width types, returns the fixed size
  /// - For empty column of var-length types, returns `0`
  /// - A negative value indicates the length is unavailable (e.g., raw var-width columns from pre-1.6.0 segments)
  /// Override it for var-length types.
  default int getLengthOfShortestElement() {
    return getStoredType().size();
  }

  /// Returns the maximum serialized byte length across all elements.
  /// - For fixed-width types, returns the fixed size
  /// - For empty column of var-length types, returns `0`
  /// - A negative value indicates the length is unavailable (e.g., raw var-width columns from pre-1.6.0 segments)
  /// Override it for var-length types.
  default int getLengthOfLongestElement() {
    return getStoredType().size();
  }

  /// Returns `true` when all the elements are of the same serialized byte length.
  /// Returns `false` when either shortest or longest is unavailable.
  default boolean isFixedLength() {
    int shortest = getLengthOfShortestElement();
    return shortest >= 0 && shortest == getLengthOfLongestElement();
  }

  /// Returns `true` when all elements of a STRING column contain only ASCII characters, `false` otherwise.
  /// Override it for STRING type.
  default boolean isAscii() {
    return false;
  }

  /// Returns the total number of entries.
  /// - For SV columns, equals the number of rows
  /// - For MV columns, sums the number of elements across all rows
  int getTotalNumberOfEntries();

  /// Returns the maximum number of elements within a single MV value, or `0` for SV columns.
  int getMaxNumberOfMultiValues();

  /// Returns the maximum serialized byte length of a single row.
  /// - For SV columns, equals [#getLengthOfLongestElement]
  /// - For MV columns, equals the sum of element lengths in the longest row
  /// - Returns `0` for empty columns
  /// - A negative value indicates the length is unavailable (e.g., var-width MV columns with varying element
  ///   lengths read from [ColumnMetadata])
  /// The default formula `maxNumberOfMultiValues * storedType.size()` handles SV and fixed-width MV columns.
  /// Override it for var-width MV columns.
  default int getMaxRowLengthInBytes() {
    if (isSingleValue()) {
      return getLengthOfLongestElement();
    } else {
      return getMaxNumberOfMultiValues() * getStoredType().size();
    }
  }

  /// Returns the [PartitionFunction] for the column, or `null` if the column is not partitioned.
  @Nullable
  PartitionFunction getPartitionFunction();

  /// Returns the partitions within which the values exist, or `null` if the column is not partitioned.
  @Nullable
  Set<Integer> getPartitions();
}
