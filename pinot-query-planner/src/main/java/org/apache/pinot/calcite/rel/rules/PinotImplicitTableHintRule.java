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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.query.routing.WorkerManager;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Enclosing
public class PinotImplicitTableHintRule extends RelRule<RelRule.Config> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotImplicitTableHintRule.class);
  private final WorkerManager _workerManager;

  private PinotImplicitTableHintRule(Config config) {
    super(config);
    _workerManager = config.getWorkerManager();
  }

  public static PinotImplicitTableHintRule withWorkerManager(WorkerManager workerManager) {
    return new PinotImplicitTableHintRule(ImmutablePinotImplicitTableHintRule.Config.builder()
        .operandSupplier(b0 -> b0.operand(TableScan.class).anyInputs())
        .workerManager(workerManager)
        .build()
    );
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    TableScan tableScan = call.rel(0);
    return getHintOptionsToRewrite(tableScan) != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan tableScan = call.rel(0);

    String tableName = RelToPlanNodeConverter.getTableNameFromTableScan(tableScan);
    TableOptions implicitTableOptions = _workerManager.inferTableOptions(tableName);
    if (implicitTableOptions == null) {
      return;
    }

    Map<String, String> explicitOptions = getHintOptionsToRewrite(tableScan);
    assert explicitOptions != null;
    TableOptions tableOptions = calculateTableOptions(explicitOptions, implicitTableOptions, tableScan);
    RelNode newRel = withNewTableOptions(tableScan, tableOptions);
    call.transformTo(newRel);
  }

  /**
   * Returns the kv options of the explicit table options hint to be rewritten with the inferred partition options
   * (empty if the scan does not have an explicit table options hint), or {@code null} when no rewrite is required —
   * either the explicit hint already provides the complete partition config (key, function and size), or the table is
   * hinted as replicated across all workers (each worker scans all the segments, so partition options are irrelevant).
   */
  @Nullable
  private static Map<String, String> getHintOptionsToRewrite(TableScan tableScan) {
    RelHint hint = PinotHintStrategyTable.getHint(tableScan, PinotHintOptions.TABLE_HINT_OPTIONS);
    if (hint == null || hint.kvOptions == null) {
      return Map.of();
    }
    Map<String, String> kvOptions = hint.kvOptions;
    if (Boolean.parseBoolean(kvOptions.get(PinotHintOptions.TableHintOptions.IS_REPLICATED))) {
      return null;
    }
    if (kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_KEY)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_SIZE)) {
      return null;
    }
    return kvOptions;
  }

  /**
   * Returns a new node which is a copy of the given table scan with the new table options hint.
   */
  private static RelNode withNewTableOptions(TableScan tableScan, TableOptions tableOptions) {
    ArrayList<RelHint> newHints = new ArrayList<>(tableScan.getHints());

    newHints.removeIf(relHint -> relHint.hintName.equals(PinotHintOptions.TABLE_HINT_OPTIONS));

    RelHint.Builder builder = RelHint.builder(PinotHintOptions.TABLE_HINT_OPTIONS)
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_KEY, tableOptions.getPartitionKey())
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, tableOptions.getPartitionFunction())
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_SIZE, String.valueOf(tableOptions.getPartitionSize()));

    if (tableOptions.getPartitionParallelism() != null) {
      builder.hintOption(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM,
          String.valueOf(tableOptions.getPartitionParallelism()));
    }

    if (tableOptions.isReplicated() != null) {
      builder.hintOption(PinotHintOptions.TableHintOptions.IS_REPLICATED,
          String.valueOf(tableOptions.isReplicated()));
    }

    newHints.add(builder.build());

    return tableScan.withHints(newHints);
  }

  /**
   * Creates a new table options hint based on the given table partition info and the explicitly supplied options, if
   * any.
   *
   * Any explicitly supplied option will override the implicit one obtained from the table partition info.
   */
  private static TableOptions calculateTableOptions(Map<String, String> kvOptions,
      TableOptions implicitTableOptions, TableScan tableScan) {
    if (kvOptions.isEmpty()) {
      return implicitTableOptions;
    }

    ImmutableTableOptions newTableOptions = ImmutableTableOptions.copyOf(implicitTableOptions);
    newTableOptions = overridePartitionKey(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionFunction(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionSize(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionParallelism(newTableOptions, tableScan, kvOptions);
    newTableOptions = carryOverIsReplicated(newTableOptions, kvOptions);

    return newTableOptions;
  }

  /**
   * Returns a table options hint with the replicated flag carried over from the explicit hint, if any. Note that this
   * rule does not match when the table is hinted as replicated across all workers, so this only ever carries over an
   * explicit {@code is_replicated='false'}.
   */
  private static ImmutableTableOptions carryOverIsReplicated(ImmutableTableOptions base,
      Map<String, String> kvOptions) {
    String isReplicated = kvOptions.get(PinotHintOptions.TableHintOptions.IS_REPLICATED);
    if (isReplicated == null) {
      return base;
    }
    return base.withIsReplicated(Boolean.parseBoolean(isReplicated));
  }

  /**
   * Returns a table options hint with the partition key overridden by the hint, if any.
   */
  private static ImmutableTableOptions overridePartitionKey(ImmutableTableOptions base, TableScan tableScan,
      Map<String, String> kvOptions) {
    String partitionKey = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY);
    if (partitionKey == null || partitionKey.equals(base.getPartitionKey())) {
      return base;
    }
    LOGGER.debug("Override implicit table hint for {} with explicit partition key: {}", tableScan, partitionKey);
    return base.withPartitionKey(partitionKey);
  }

  /**
   * Returns a table options hint with the partition function overridden by the hint, if any.
   */
  private static ImmutableTableOptions overridePartitionFunction(ImmutableTableOptions base,
      TableScan tableScan, Map<String, String> kvOptions) {
    String partitionFunction = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION);
    if (partitionFunction == null || partitionFunction.equals(base.getPartitionFunction())) {
      return base;
    }
    LOGGER.debug("Override implicit table hint for {} with explicit partition function: {}", tableScan,
        partitionFunction);
    return base.withPartitionFunction(partitionFunction);
  }

  /**
   * Returns a table options hint with the partition parallelism overridden by the hint, if any.
   */
  private static ImmutableTableOptions overridePartitionParallelism(ImmutableTableOptions base,
      TableScan tableScan, Map<String, String> kvOptions) {
    String partitionParallelismStr = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM);
    if (partitionParallelismStr == null) {
      return base;
    }
    try {
      int partitionParallelism = Integer.parseInt(partitionParallelismStr);
      LOGGER.debug("Override implicit table hint for {} with explicit partition parallelism: {}", tableScan,
          partitionParallelism);
      return base.withPartitionParallelism(partitionParallelism);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid partition parallelism: " + partitionParallelismStr + " for table: " + tableScan);
    }
  }

  /**
   * Returns a table options hint with the partition size overridden by the hint, if any.
   */
  private static ImmutableTableOptions overridePartitionSize(ImmutableTableOptions base,
      TableScan tableScan, Map<String, String> kvOptions) {
    String partitionSizeStr = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
    if (partitionSizeStr == null) {
      return base;
    }
    try {
      int explicitPartitionSize = Integer.parseInt(partitionSizeStr);
      if (explicitPartitionSize == base.getPartitionSize()) {
        return base;
      }
      LOGGER.debug("Override implicit table hint for {} with explicit partition size: {}", tableScan,
          explicitPartitionSize);
      return base.withPartitionSize(explicitPartitionSize);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid partition size: " + partitionSizeStr + " for table: " + tableScan);
    }
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    @Nullable
    WorkerManager getWorkerManager();

    @Override
    default PinotImplicitTableHintRule toRule() {
      return new PinotImplicitTableHintRule(this);
    }
  }
}
