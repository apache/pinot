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
package org.apache.pinot.common.partition.function;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// Implementation of [PartitionFunction] which partitions based on configured column values.
///
/// ```json
/// "columnPartitionMap": {
///   "subject": {
///     "functionName": "BoundedColumnValue",
///     "functionConfig": {
///       "columnValues": "Maths|English|Chemistry",
///       "columnValuesDelimiter": "|"
///     },
///     "numPartitions": 4
///   }
/// }
/// ```
///
/// With this partition config on column "subject", `partitionId` is `1` for Maths, `2` for English and `3` for
/// Chemistry. `partitionId` is `0` for all other values which may occur, therefore `numPartitions` is set to `4`.
public class BoundedColumnValuePartitionFunction implements PartitionFunction {
  private static final int DEFAULT_PARTITION_ID = 0;
  private static final String NAME = "BoundedColumnValue";
  private static final String COLUMN_VALUES = "columnValues";
  private static final String COLUMN_VALUES_DELIMITER = "columnValuesDelimiter";
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;
  private final String[] _values;

  public BoundedColumnValuePartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    _numPartitions = numPartitions;
    if (functionConfig == null) {
      // Probe-only path used by PartitionFunctionFactory startup scan; real use supplies a
      // populated config and reaches the validation below. getPartition() will throw on a
      // probe-built instance, which is fine because the registry never calls it.
      _functionConfig = null;
      _values = null;
      return;
    }
    Preconditions.checkArgument(functionConfig.size() > 0, "'functionConfig' must not be empty");
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES) != null, "columnValues must be configured");
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES_DELIMITER) != null,
        "'columnValuesDelimiter' must be configured");
    _functionConfig = functionConfig;
    _values = StringUtils.split(functionConfig.get(COLUMN_VALUES), functionConfig.get(COLUMN_VALUES_DELIMITER));
    Preconditions.checkState(numPartitions == _values.length + 1,
        "'numPartitions' must just be one greater than number of column values configured");
  }

  @Override
  public int getPartition(String value) {
    Preconditions.checkState(_values != null, "BoundedColumnValuePartitionFunction is not configured");
    for (int i = 0; i < _numPartitions - 1; i++) {
      if (_values[i].equalsIgnoreCase(value)) {
        return i + 1;
      }
    }
    return DEFAULT_PARTITION_ID;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /// Returns number of partitions configured for the column.
  ///
  /// @return Total number of partitions for the function.
  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  @Override
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    // Output is a fixed mapping in [0, numPartitions); no normalization is applied.
    return PartitionIdNormalizer.NO_OP;
  }

  @Override
  public String toString() {
    return getName();
  }
}
