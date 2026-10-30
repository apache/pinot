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

import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.pinot.calcite.rel.logical.PinotKWayMergeSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.immutables.value.Value;


/// Streams a globally limited leaf selection after its ordered sender input is established by logical planning.
/// This per-query rule is registered only after the broker verifies the merge wire capability and the query opts in.
@Value.Enclosing
public class PinotSortedLeafExchangeRule extends RelRule<PinotSortedLeafExchangeRule.Config> {
  @Nullable
  private final TableCache _tableCache;

  public PinotSortedLeafExchangeRule(@Nullable TableCache tableCache) {
    super(Config.DEFAULT);
    _tableCache = tableCache;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalSort globalSort = call.rel(0);
    PinotLogicalSortExchange exchange = call.rel(1);
    LogicalSort senderSort = call.rel(2);
    if (globalSort.fetch == null || globalSort.getCollation().getFieldCollations().isEmpty()
        || !globalSort.getCollation().equals(exchange.getCollation())
        || !globalSort.getCollation().equals(senderSort.getCollation())
        || !isSinglePhysicalSelection(senderSort.getInput())) {
      return;
    }
    // SortExchangeCopyRule supplies the sender's OFFSET + LIMIT. Preserve that sort and apply the global slice
    // only after merging. An absent global fetch must keep SortOperator's defaultResponseLimit semantics.
    call.transformTo(PinotKWayMergeSortExchange.create(senderSort, exchange.getDistribution(), exchange.getCollation(),
        exchange.getPrePartitioned(), RexExpressionUtils.getValueAsInt(globalSort.fetch),
        globalSort.offset == null ? -1 : RexExpressionUtils.getValueAsInt(globalSort.offset)));
  }

  private boolean isSinglePhysicalSelection(RelNode node) {
    node = PinotRuleUtils.unboxRel(node);
    while (node instanceof Project || node instanceof Filter) {
      node = PinotRuleUtils.unboxRel(node.getInput(0));
    }
    if (!(node instanceof TableScan) || _tableCache == null) {
      return false;
    }
    String tableName = RelToPlanNodeConverter.getTableNameFromTableScan((TableScan) node);
    if (TableNameBuilder.getTableTypeFromTableName(tableName) != null) {
      return true;
    }
    String actualTableName = _tableCache.getActualTableName(tableName);
    if (actualTableName == null || _tableCache.isLogicalTable(actualTableName)) {
      return false;
    }
    if (TableNameBuilder.getTableTypeFromTableName(actualTableName) != null) {
      return true;
    }
    boolean offline = _tableCache.getTableConfig(
        TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(actualTableName)) != null;
    boolean realtime = _tableCache.getTableConfig(
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(actualTableName)) != null;
    return offline != realtime;
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutablePinotSortedLeafExchangeRule.Config.builder()
        .operandSupplier(b0 -> b0.operand(LogicalSort.class)
            .oneInput(b1 -> b1.operand(PinotLogicalSortExchange.class)
                .oneInput(b2 -> b2.operand(LogicalSort.class).anyInputs())))
        .build();

    @Override
    default PinotSortedLeafExchangeRule toRule() {
      return new PinotSortedLeafExchangeRule(null);
    }
  }
}
