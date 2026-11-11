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
package org.apache.pinot.query.routing;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.TableHintOptions;


/// The partition layout hinted on one leaf stage's table, resolved in a single place so that every consumer resolves it
/// the same way. Agreement is load-bearing rather than cosmetic: [WorkerManager] gives worker `k` of a partitioned leaf
/// the `k`-th surviving partition class of the leaf's colocated group, and [ColocationGroupAnalyzer] decides that class
/// list from these same hints, so a `partition_size` resolved two ways would make worker `k` stand for a different
/// partition on each side of a 1-to-1 exchange.
class LeafPartitionHints {
  private static final String DEFAULT_PARTITION_FUNCTION = "Murmur";

  @Nullable
  private final String _partitionKey;
  private final int _partitionSize;
  private final int _partitionParallelism;
  @Nullable
  private final String _hintedPartitionFunction;

  private LeafPartitionHints(@Nullable String partitionKey, int partitionSize, int partitionParallelism,
      @Nullable String hintedPartitionFunction) {
    _partitionKey = partitionKey;
    _partitionSize = partitionSize;
    _partitionParallelism = partitionParallelism;
    _hintedPartitionFunction = hintedPartitionFunction;
  }

  /// Resolves the partition hints of a leaf stage from its table hint options. A hint that cannot be used, including a
  /// non-numeric `partition_size` or `partition_parallelism`, is reported as [IllegalStateException] so that a caller
  /// which wants to degrade instead of failing (see [ColocationGroupAnalyzer]) has a single type to catch.
  static LeafPartitionHints resolve(Map<String, String> tableOptions) {
    // Resolved for a non-partitioned leaf too, because it also sizes the workers of its local exchange.
    int partitionParallelism = parsePositive(tableOptions, TableHintOptions.PARTITION_PARALLELISM, 1);
    String partitionKey = tableOptions.get(TableHintOptions.PARTITION_KEY);
    if (partitionKey == null) {
      // Not a partitioned leaf, so the rest of the hints say nothing about it and are deliberately left unresolved.
      return new LeafPartitionHints(null, -1, partitionParallelism, null);
    }
    int partitionSize = parsePositive(tableOptions, TableHintOptions.PARTITION_SIZE, -1);
    Preconditions.checkState(partitionSize > 0, "'%s' must be provided for partition key: %s",
        TableHintOptions.PARTITION_SIZE, partitionKey);
    return new LeafPartitionHints(partitionKey, partitionSize, partitionParallelism,
        tableOptions.get(TableHintOptions.PARTITION_FUNCTION));
  }

  /// Returns whether the given table hint options declare the table replicated across all workers. Such a leaf holds
  /// every segment on every worker and takes its worker map from its peer, so no partition hint applies to it.
  static boolean isReplicated(Map<String, String> tableOptions) {
    return Boolean.parseBoolean(tableOptions.get(TableHintOptions.IS_REPLICATED));
  }

  /// Returns the hinted partition key, or `null` when the leaf is not partitioned, in which case the partition size and
  /// function are meaningless.
  @Nullable
  String getPartitionKey() {
    return _partitionKey;
  }

  /// Returns the number of partition classes, and of workers before any reduction, i.e. the hinted `partition_size`.
  /// Positive when [#getPartitionKey()] is non-null, -1 otherwise.
  int getPartitionSize() {
    return _partitionSize;
  }

  int getPartitionParallelism() {
    return _partitionParallelism;
  }

  /// Returns the partition function to use, i.e. the hinted one or `Murmur` when the hint is absent.
  String getPartitionFunction() {
    return _hintedPartitionFunction != null ? _hintedPartitionFunction : DEFAULT_PARTITION_FUNCTION;
  }

  /// Returns the `partition_function` hint exactly as given, i.e. `null` when it is absent. Unlike
  /// [#getPartitionFunction()], which fills the default in, so comparing two leaves through this never lets an omitted
  /// hint match an explicit one.
  @Nullable
  String getHintedPartitionFunction() {
    return _hintedPartitionFunction;
  }

  private static int parsePositive(Map<String, String> tableOptions, String option, int defaultValue) {
    String value = tableOptions.get(option);
    if (value == null) {
      return defaultValue;
    }
    int parsed;
    try {
      parsed = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("'" + option + "' must be a positive integer, got: " + value);
    }
    Preconditions.checkState(parsed > 0, "'%s' must be positive, got: %s", option, parsed);
    return parsed;
  }
}
