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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIntNormalizer;
import org.apache.pinot.spi.annotations.PartitionFunctionType;


/**
 * Implementation of {@link PartitionFunction} which partitions based on configured column values.
 *
 * "columnPartitionMap": {
 *   "subject": {
 *     "functionName": "BoundedColumnValue",
 *     "functionConfig": {
 *       "columnValues": "Maths|English|Chemistry",
 *       "columnValuesDelimiter": "|"
 *     },
 *     "numPartitions": 4
 *   }
 * }
 * With this partition config on column "subject", partitionId would be 1 for Maths, 2 for English and 3 for Chemistry.
 * partitionId would be "0" for all other values which may occur, therefore 'numPartitions' is set to 4.
 *
 * <p>The mapping output is already in {@code [0, numPartitions)}, so the configured
 * {@link PartitionIntNormalizer} (default {@link PartitionIntNormalizer#POSITIVE_MODULO}) is a
 * no-op for in-range values; it is still applied so the field is uniform across all partition
 * functions.
 */
@PartitionFunctionType(names = "BoundedColumnValue")
public class BoundedColumnValuePartitionFunction implements PartitionFunction {
  private static final int DEFAULT_PARTITION_ID = 0;
  private static final String NAME = "BoundedColumnValue";
  private static final String COLUMN_VALUES = "columnValues";
  private static final String COLUMN_VALUES_DELIMITER = "columnValuesDelimiter";
  private static final PartitionIntNormalizer DEFAULT_NORMALIZER = PartitionIntNormalizer.POSITIVE_MODULO;
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;
  private final String[] _values;
  private final PartitionIntNormalizer _normalizer;

  public BoundedColumnValuePartitionFunction(int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(functionConfig != null && functionConfig.size() > 0,
        "'functionConfig' should be present, specified", functionConfig);
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES) != null, "columnValues must be configured");
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES_DELIMITER) != null,
        "'columnValuesDelimiter' must be configured");
    _functionConfig = functionConfig;
    _values = StringUtils.split(functionConfig.get(COLUMN_VALUES), functionConfig.get(COLUMN_VALUES_DELIMITER));
    Preconditions.checkState(numPartitions == _values.length + 1,
        "'numPartitions' must just be one greater than number of column values configured");
    _numPartitions = numPartitions;
    _normalizer = PartitionFunctionConfigs.normalizer(functionConfig, DEFAULT_NORMALIZER);
  }

  @Override
  public int getPartition(String value) {
    int rawPartition = DEFAULT_PARTITION_ID;
    for (int i = 0; i < _numPartitions - 1; i++) {
      if (_values[i].equalsIgnoreCase(value)) {
        rawPartition = i + 1;
        break;
      }
    }
    return _normalizer.getPartitionId(rawPartition, _numPartitions);
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Returns number of partitions configured for the column.
   *
   * @return Total number of partitions for the function.
   */
  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  @Override
  public String getPartitionIdNormalizer() {
    return _normalizer.name();
  }

  @Override
  public String toString() {
    return getName();
  }
}
