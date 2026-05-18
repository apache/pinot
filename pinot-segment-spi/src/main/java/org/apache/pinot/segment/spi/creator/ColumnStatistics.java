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
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnShape;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Statistics for a column, collected before creating indexes.
/// For MV column, the stats are on individual elements within the MV values.
public interface ColumnStatistics extends ColumnShape, Serializable {

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

  @Deprecated(since = "1.6.0", forRemoval = true)
  default DataType getValueType() {
    return getStoredType();
  }

  @Deprecated(since = "1.6.0", forRemoval = true)
  default int getLengthOfLargestElement() {
    return getLengthOfLongestElement();
  }

  @Deprecated(since = "1.6.0", forRemoval = true)
  default int getNumPartitions() {
    PartitionFunction partitionFunction = getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getNumPartitions() : 0;
  }

  @Deprecated(since = "1.6.0", forRemoval = true)
  @Nullable
  default Map<String, String> getPartitionFunctionConfig() {
    PartitionFunction partitionFunction = getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getFunctionConfig() : null;
  }
}
