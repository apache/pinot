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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code DataSourceMetadata} class contains all the metadata for a column for query execution purpose.
 */
public interface DataSourceMetadata {

  /**
   * Returns the field spec of the column.
   */
  FieldSpec getFieldSpec();

  /**
   * Returns the data type of the column.
   */
  default FieldSpec.DataType getDataType() {
    return getFieldSpec().getDataType();
  }

  /**
   * Returns {@code true} if the column is single-valued, {@code false} otherwise.
   */
  default boolean isSingleValue() {
    return getFieldSpec().isSingleValueField();
  }

  /**
   * Returns {@code true} if the column is sorted, {@code false} otherwise.
   *
   * The result of this method cannot be trusted if null handling is enabled.
   */
  boolean isSorted();

  /**
   * Returns the number of documents of the column.
   */
  int getNumDocs();

  /**
   * Returns the number of values of the column. For single-value column, number of values always equals to number of
   * documents; for multi-value column, each document (multi-value entry) may contain multiple values.
   */
  int getNumValues();

  /**
   * Returns the maximum number of values for each multi-value entry of the multi-value column. Returns {@code -1} for
   * single-value column.
   */
  int getMaxNumValuesPerMVEntry();

  /**
   * Returns the minimum value of the column, or {@code null} if it is not available.
   */
  @Nullable
  Comparable getMinValue();

  /**
   * Returns the maximum value of the column, or {@code null} if it is not available.
   */
  @Nullable
  Comparable getMaxValue();

  /**
   * Returns the partition function of the column, or {@code null} if the column is not partitioned.
   */
  @Nullable
  PartitionFunction getPartitionFunction();

  /**
   * Returns the partitions the column belongs to, or {@code null} if the column is not partitioned.
   */
  @Nullable
  Set<Integer> getPartitions();

  /**
   * Returns the cardinality of the column, {@code -1} if not applicable
   */
  int getCardinality();

  /**
   * Returns the max row length in bytes for a var byte MV column. {@code -1} if not applicable.
   * @return
   */
  default int getMaxRowLengthInBytes() {
    return -1;
  }
}
