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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class ColumnPartitionConfig extends BaseJsonConfig {
  public static final String PARTITION_ID_NORMALIZER_POSITIVE_MODULO = "POSITIVE_MODULO";
  public static final String PARTITION_ID_NORMALIZER_ABS = "ABS";
  public static final String PARTITION_ID_NORMALIZER_MASK = "MASK";

  private final String _functionName;
  private final String _functionExpr;
  private final String _partitionIdNormalizer;
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;

  public ColumnPartitionConfig(String functionName, int numPartitions) {
    this(functionName, numPartitions, null, null, null);
  }

  public static ColumnPartitionConfig forFunctionExpr(String functionExpr, int numPartitions) {
    return forFunctionExpr(functionExpr, numPartitions, null);
  }

  public static ColumnPartitionConfig forFunctionExpr(String functionExpr, int numPartitions,
      @Nullable String partitionIdNormalizer) {
    return new ColumnPartitionConfig(null, numPartitions, null, functionExpr, partitionIdNormalizer);
  }

  public ColumnPartitionConfig(String functionName, int numPartitions, @Nullable Map<String, String> functionConfig) {
    this(functionName, numPartitions, functionConfig, null, null);
  }

  @JsonCreator
  public ColumnPartitionConfig(@JsonProperty("functionName") @Nullable String functionName,
      @JsonProperty(value = "numPartitions", required = true) int numPartitions,
      @JsonProperty("functionConfig") @Nullable Map<String, String> functionConfig,
      @JsonProperty("functionExpr") @Nullable String functionExpr,
      @JsonProperty("partitionIdNormalizer") @Nullable String partitionIdNormalizer) {
    Preconditions.checkArgument(hasText(functionName) ^ hasText(functionExpr),
        "Exactly one of 'functionName' or 'functionExpr' must be configured");
    Preconditions.checkArgument(numPartitions > 0, "'numPartitions' must be positive");
    Preconditions.checkArgument(!hasText(functionExpr) || functionConfig == null,
        "'functionConfig' cannot be configured together with 'functionExpr'");
    Preconditions.checkArgument(!hasText(partitionIdNormalizer) || hasText(functionExpr),
        "'partitionIdNormalizer' can only be configured together with 'functionExpr'");
    Preconditions.checkArgument(isValidPartitionIdNormalizer(partitionIdNormalizer),
        "Unsupported partitionIdNormalizer: %s", partitionIdNormalizer);
    _functionName = functionName;
    _functionExpr = functionExpr;
    _partitionIdNormalizer = partitionIdNormalizer;
    _numPartitions = numPartitions;
    _functionConfig = functionConfig;
  }

  /**
   * Returns the partition function name for the column.
   *
   * @return Partition function name.
   */
  @Nullable
  public String getFunctionName() {
    return _functionName;
  }

  /**
   * Returns the function expression for expression-mode partitioning.
   */
  @Nullable
  public String getFunctionExpr() {
    return _functionExpr;
  }

  /**
   * Returns the partition-id normalizer for expression-mode partitioning.
   */
  @Nullable
  public String getPartitionIdNormalizer() {
    return _partitionIdNormalizer;
  }

  /**
   * Returns the partition function configuration for the column.
   *
   * @return Partition function configuration.
   */
  @Nullable
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  /**
   * Returns the number of partitions for this column.
   *
   * @return Number of partitions.
   */
  public int getNumPartitions() {
    return _numPartitions;
  }

  private static boolean hasText(@Nullable String value) {
    return value != null && !value.trim().isEmpty();
  }

  private static boolean isValidPartitionIdNormalizer(@Nullable String partitionIdNormalizer) {
    if (!hasText(partitionIdNormalizer)) {
      return true;
    }
    return PARTITION_ID_NORMALIZER_POSITIVE_MODULO.equalsIgnoreCase(partitionIdNormalizer)
        || PARTITION_ID_NORMALIZER_ABS.equalsIgnoreCase(partitionIdNormalizer)
        || PARTITION_ID_NORMALIZER_MASK.equalsIgnoreCase(partitionIdNormalizer);
  }
}
