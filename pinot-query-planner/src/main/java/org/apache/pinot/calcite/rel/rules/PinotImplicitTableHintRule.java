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

    // we don't want to apply this rule if the explicit hint is complete
    return !isHintComplete(getTableOptionHint(tableScan));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan tableScan = call.rel(0);

    String tableName = RelToPlanNodeConverter.getTableNameFromTableScan(tableScan);
    @Nullable
    TableOptions implicitTableOptions = _workerManager.inferTableOptions(tableName);
    if (implicitTableOptions == null) {
      return;
    }

    @Nullable
    RelHint explicitHint = getTableOptionHint(tableScan);
    TableOptions tableOptions = calculateTableOptions(explicitHint, implicitTableOptions, tableScan);
    RelNode newRel = withNewTableOptions(tableScan, tableOptions);
    call.transformTo(newRel);
  }

  /**
   * Determines is the provided hint is complete.
   * A hint is considered complete if it provides explicit config for key, function and partition size.
   */
  private boolean isHintComplete(@Nullable RelHint hint) {
    if (hint == null || hint.kvOptions == null) {
      return false;
    }
    Map<String, String> kvOptions = hint.kvOptions;
    return kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_KEY)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
  }

  /**
   * Get the table option hint from the table scan, if any.
   */
  @Nullable
  private static RelHint getTableOptionHint(TableScan tableScan) {
    return PinotHintStrategyTable.getHint(tableScan, PinotHintOptions.TABLE_HINT_OPTIONS);
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

    newHints.add(builder.build());

    return tableScan.withHints(newHints);
  }

  /**
   * Creates a new table options hint based on the given table partition info and the explicit hint, if any.
   *
   * Any explicit hint will override the implicit hint obtained from the table partition info.
   */
  private static TableOptions calculateTableOptions(
      @Nullable RelHint relHint, TableOptions implicitTableOptions, TableScan tableScan) {
    if (relHint == null) {
      return implicitTableOptions;
    }

    // there is a hint, check fill default data and obtain the partition parallelism if supplied
    Map<String, String> kvOptions = relHint.kvOptions;

    ImmutableTableOptions newTableOptions = ImmutableTableOptions.copyOf(implicitTableOptions);
    newTableOptions = overridePartitionKey(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionFunction(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionSize(newTableOptions, tableScan, kvOptions);
    newTableOptions = overridePartitionParallelism(newTableOptions, tableScan, kvOptions);

    return newTableOptions;
  }

  /**
   * Returns a table options hint with the partition key overridden by the hint, if any.
   */
  private static ImmutableTableOptions overridePartitionKey(ImmutableTableOptions base, TableScan tableScan,
      Map<String, String> kvOptions) {
    String partitionKey = kvOptions.get(kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY));
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
    String partitionFunction = kvOptions.get(kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION));
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
