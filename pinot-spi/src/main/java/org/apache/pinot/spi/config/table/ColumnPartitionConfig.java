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


/// Partition configuration for a single column.
///
/// At least one of `functionName` or `functionExpr` must be configured. Both may be configured together; in that case
/// the framework prefers `functionExpr` and uses `functionName` only as a documentation hint or for older readers
/// that pre-date expression mode.
///
/// - **Name mode** (legacy, production-recommended): configure `functionName` (and optionally `functionConfig`).
///   Old and new nodes can all deserialize this format. The named partition functions
///   (`Murmur`, `Modulo`, `HashCode`, ...) are the production-ready path.
/// - **Expression mode** (new, flexibility escape hatch): configure `functionExpr` to compute the partition with
///   a chained scalar expression (e.g. `positiveModulo(fnv1a_32(md5(col)), numPartitions)`). This trades runtime
///   overhead for the ability to express arbitrary partition logic without writing a custom partition function. The
///   expression must produce an integral value already in `[0, numPartitions)`. **Known limitation:** expression-mode
///   configs must only be written after *all* broker/server/controller nodes have been upgraded to a version that
///   supports this feature. A node pre-dating expression mode will fail to deserialize such a config and will exclude
///   the affected table from partition-aware routing. No mixed-version safety gate is enforced at the controller
///   write path; operators are responsible for ensuring the cluster is fully upgraded before enabling expression-mode
///   partitioning.
public class ColumnPartitionConfig extends BaseJsonConfig {
  private final String _functionName;
  private final String _functionExpr;
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;

  public ColumnPartitionConfig(String functionName, int numPartitions) {
    this(functionName, numPartitions, null, null);
  }

  public static ColumnPartitionConfig forFunctionExpr(String functionExpr, int numPartitions) {
    return new ColumnPartitionConfig(null, numPartitions, null, functionExpr);
  }

  public ColumnPartitionConfig(String functionName, int numPartitions, @Nullable Map<String, String> functionConfig) {
    this(functionName, numPartitions, functionConfig, null);
  }

  @JsonCreator
  public ColumnPartitionConfig(@JsonProperty("functionName") @Nullable String functionName,
      @JsonProperty(value = "numPartitions", required = true) int numPartitions,
      @JsonProperty("functionConfig") @Nullable Map<String, String> functionConfig,
      @JsonProperty("functionExpr") @Nullable String functionExpr) {
    Preconditions.checkArgument(hasText(functionName) || hasText(functionExpr),
        "At least one of 'functionName' or 'functionExpr' must be configured");
    Preconditions.checkArgument(numPartitions > 0, "'numPartitions' must be positive");
    _functionName = functionName;
    _functionExpr = functionExpr;
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

  /// Returns the function expression for expression-mode partitioning.
  @Nullable
  public String getFunctionExpr() {
    return _functionExpr;
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
}
