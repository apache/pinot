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
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Enclosing
public class PinotImplicitTableHintRule extends RelRule<RelRule.Config> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotImplicitTableHintRule.class);
  private final PartitionTableFinder _partitionTableFinder;

  private PinotImplicitTableHintRule(Config config) {
    super(config);
    _partitionTableFinder = config.getTablePartitionTableFinder();
  }

  public static PinotImplicitTableHintRule withPartitionTableFinder(PartitionTableFinder tablePartitionTableFinder) {
    return new PinotImplicitTableHintRule(ImmutablePinotImplicitTableHintRule.Config.builder()
        .operandSupplier(b0 -> b0.operand(LogicalTableScan.class).anyInputs())
        .tablePartitionTableFinder(tablePartitionTableFinder)
        .build()
    );
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalTableScan tableScan = call.rel(0);

    RelHint explicitHint = getTableOptionHint(tableScan);

    if (explicitHint == null) {
      return true;
    }
    // we don't want to apply this rule if the explicit hint is complete
    Map<String, String> kvOptions = explicitHint.kvOptions;
    return kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_KEY)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION)
        && kvOptions.containsKey(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTableScan tableScan = call.rel(0);

    TablePartitionInfo tablePartitionInfo = getTablePartitionInfo(tableScan);
    if (tablePartitionInfo == null) {
      LOGGER.debug("Table partition info not found for table: {}", tableScan);
      return;
    }

    @Nullable
    RelHint explicitHint = getTableOptionHint(tableScan);
    TableOptions tableOptions = calculateTableOptions(explicitHint, tablePartitionInfo, tableScan);
    RelNode newRel = withNewTableOptions(tableScan, tableOptions);
    call.transformTo(newRel);
  }

  /**
   * Get the table option hint from the table scan, if any.
   */
  @Nullable
  private static RelHint getTableOptionHint(LogicalTableScan tableScan) {
    return tableScan.getHints().stream()
        .filter(relHint -> relHint.hintName.equals(PinotHintOptions.TABLE_HINT_OPTIONS))
        .findAny()
        .orElse(null);
  }

  /**
   * Returns a new node which is a copy of the given table scan with the new table options hint.
   */
  private static RelNode withNewTableOptions(LogicalTableScan tableScan, TableOptions tableOptions) {
    ArrayList<RelHint> newHints = new ArrayList<>(tableScan.getHints());

    newHints.removeIf(relHint -> relHint.hintName.equals(PinotHintOptions.TABLE_HINT_OPTIONS));

    RelHint tableOptionsHint = RelHint.builder(PinotHintOptions.TABLE_HINT_OPTIONS)
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_KEY, tableOptions.getPartitionKey())
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, tableOptions.getPartitionFunction())
        .hintOption(PinotHintOptions.TableHintOptions.PARTITION_SIZE, String.valueOf(tableOptions.getPartitionSize()))
        .build();

    newHints.add(tableOptionsHint);

    return tableScan.withHints(newHints);
  }

  /**
   * Creates a new table options hint based on the given table partition info and the explicit hint, if any.
   *
   * Any explicit hint will override the implicit hint obtained from the table partition info.
   */
  private static TableOptions calculateTableOptions(
      @Nullable RelHint relHint, TablePartitionInfo tablePartitionInfo, LogicalTableScan tableScan) {
    if (relHint == null) {
      return ImmutablePinotImplicitTableHintRule.TableOptions.builder()
          .partitionKey(tablePartitionInfo.getPartitionColumn())
          .partitionFunction(tablePartitionInfo.getPartitionFunctionName())
          .partitionSize(tablePartitionInfo.getNumPartitions()).partitionParallelism(1)
          .build();
    }

    // there is a hint, check fill default data and obtain the partition parallelism if supplied
    Map<String, String> kvOptions = relHint.kvOptions;
    return ImmutablePinotImplicitTableHintRule.TableOptions.builder()
        .partitionKey(getPartitionKey(tablePartitionInfo, tableScan, kvOptions))
        .partitionFunction(getPartitionFunction(tablePartitionInfo, tableScan, kvOptions))
        .partitionSize(getPartitionSize(tablePartitionInfo, tableScan, kvOptions))
        .partitionParallelism(getPartitionParallelism(tableScan, kvOptions))
        .build();
  }

  /**
   * Get the partition key from the hint, if any, otherwise use the partition column from the table partition info.
   */
  private static String getPartitionKey(TablePartitionInfo tablePartitionInfo, LogicalTableScan tableScan,
      Map<String, String> kvOptions) {
    String partitionKey = kvOptions.get(kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY));
    if (partitionKey != null && !partitionKey.equals(tablePartitionInfo.getPartitionColumn())) {
      LOGGER.debug("Override implicit table hint for {} with explicit partition key: {}", tableScan, partitionKey);
      return partitionKey;
    } else {
      return tablePartitionInfo.getPartitionColumn();
    }
  }

  /**
   * Get the partition function from the hint, if any, otherwise use the partition function from the table partition
   * info.
   */
  private static String getPartitionFunction(TablePartitionInfo tablePartitionInfo, LogicalTableScan tableScan,
      Map<String, String> kvOptions) {
    String partitionFunction = kvOptions.get(kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION));
    if (partitionFunction != null && !partitionFunction.equals(tablePartitionInfo.getPartitionFunctionName())) {
      LOGGER.debug("Override implicit table hint for {} with explicit partition function: {}",
          tableScan, partitionFunction);
      return partitionFunction;
    } else {
      return tablePartitionInfo.getPartitionFunctionName();
    }
  }

  /**
   * Get the partition parallelism from the hint, if any, otherwise use the default value of 1.
   */
  private static int getPartitionParallelism(LogicalTableScan tableScan, Map<String, String> kvOptions) {
    int partitionParallelism = 1;
    String partitionParallelismStr = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM);
    if (partitionParallelismStr != null) {
      try {
        int explicitPartitionParallelism = Integer.parseInt(partitionParallelismStr);
        if (explicitPartitionParallelism != partitionParallelism) {
          LOGGER.debug("Override implicit table hint for {} with explicit partition parallelism: {}", tableScan,
              explicitPartitionParallelism);
          partitionParallelism = explicitPartitionParallelism;
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid partition parallelism: " + partitionParallelismStr + " for table: " + tableScan);
      }
    }
    return partitionParallelism;
  }

  /**
   * Get the partition size from the hint, if any, otherwise use the partition size from the table partition info.
   */
  private static int getPartitionSize(TablePartitionInfo tablePartitionInfo, LogicalTableScan tableScan,
      Map<String, String> kvOptions) {
    int partitionSize = tablePartitionInfo.getNumPartitions();
    String partitionSizeStr = kvOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
    if (partitionSizeStr != null) {
      try {
        int explicitPartitionSize = Integer.parseInt(partitionSizeStr);
        if (explicitPartitionSize != partitionSize) {
          LOGGER.debug("Override implicit table hint for {} with explicit partition size: {}", tableScan,
              explicitPartitionSize);
          partitionSize = explicitPartitionSize;
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid partition size: " + partitionSizeStr + " for table: " + tableScan);
      }
    }
    return partitionSize;
  }

  /**
   * Get the table partition info for the given table scan, transforming Calcite's table name to Pinot's table name
   * and adding the table type if needed.
   */
  @Nullable
  private TablePartitionInfo getTablePartitionInfo(LogicalTableScan tableScan) {
    String tableName = RelToPlanNodeConverter.getTableNameFromTableScan(tableScan);

    if (TableNameBuilder.getTableTypeFromTableName(tableName) == null) {
      tableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
    }
    return _partitionTableFinder.getTablePartitionInfo(tableName);
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    PartitionTableFinder getTablePartitionTableFinder();

    @Override
    default PinotImplicitTableHintRule toRule() {
      return new PinotImplicitTableHintRule(this);
    }
  }

  /**
   * An interface to find the table partition info for a given table name.
   */
  public interface PartitionTableFinder {

    /**
     * Get the table partition info for the given table name with type suffix.
     * @return the table partition info, or null if not found
     */
    @Nullable
    TablePartitionInfo getTablePartitionInfo(String tableNameWithType);

    /**
     * A partition table finder that always returns null, meaning that the table partition info is not found.
     */
    static PartitionTableFinder disabled() {
      return (table) -> null;
    }
  }

  /**
   * An internal interface used to generate the table options hint.
   */
  @Value.Immutable
  interface TableOptions {
    String getPartitionKey();
    String getPartitionFunction();
    int getPartitionSize();
    int getPartitionParallelism();
  }
}
