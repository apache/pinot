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
package org.apache.calcite.rel.rules;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;


public class PinotSortExchangeCopyRule extends RelRule<RelRule.Config> {

  public static final PinotSortExchangeCopyRule SORT_EXCHANGE_COPY =
      PinotSortExchangeCopyRule.Config.DEFAULT.toRule();
  private static final int DEFAULT_SORT_EXCHANGE_COPY_THRESHOLD = 10_000;
  private static final TypeFactory TYPE_FACTORY = new TypeFactory(new TypeSystem());
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  private static final RexLiteral REX_ZERO = REX_BUILDER.makeLiteral(0,
      TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

  /**
   * Creates a PinotSortExchangeCopyRule.
   */
  protected PinotSortExchangeCopyRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final SortExchange exchange = call.rel(1);
    final RelMetadataQuery metadataQuery = call.getMetadataQuery();

    if (RelMdUtil.checkInputForCollationAndLimit(
        metadataQuery,
        exchange.getInput(),
        sort.getCollation(),
        sort.offset,
        sort.fetch)) {
      // Don't rewrite anything if the input is already sorted AND the
      // input node would already return fewer than sort.offset + sort.fetch
      // rows (e.g. there is already an inner limit applied)
      return;
    }

    RelCollation collation = sort.getCollation();
    Preconditions.checkArgument(
        collation.equals(exchange.getCollation()),
        "Expected collation on exchange and sort to be the same"
    );

    final RexNode fetch;
    if (sort.fetch == null) {
      fetch = null;
    } else if (sort.offset == null) {
      fetch = sort.fetch;
    } else {
      int total = RexExpressionUtils.getValueAsInt(sort.fetch) + RexExpressionUtils.getValueAsInt(sort.offset);
      fetch = REX_BUILDER.makeLiteral(total, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    }
    // do not transform sort-exchange copy when there's no fetch limit, or fetch amount is larger than threshold
    if (!collation.getFieldCollations().isEmpty()
        && (fetch == null || RexExpressionUtils.getValueAsInt(fetch) > DEFAULT_SORT_EXCHANGE_COPY_THRESHOLD)) {
      return;
    }

    final RelNode newExchangeInput = sort.copy(sort.getTraitSet(), exchange.getInput(), collation, null, fetch);
    final RelNode exchangeCopy = exchange.copy(exchange.getTraitSet(), newExchangeInput, exchange.getDistribution());
    final RelNode sortCopy = sort.copy(sort.getTraitSet(), exchangeCopy, collation,
        sort.offset == null ? REX_ZERO : sort.offset, sort.fetch);

    call.transformTo(sortCopy);
  }

  public interface Config extends RelRule.Config {

    Config DEFAULT = ImmutableSortExchangeCopyRule.Config.of()
        .withOperandFor(LogicalSort.class, PinotLogicalSortExchange.class);

    @Override default PinotSortExchangeCopyRule toRule() {
      return new PinotSortExchangeCopyRule(this);
    }

    /** Defines an operand tree for the given classes. */

    default Config withOperandFor(Class<? extends Sort> sortClass,
        Class<? extends SortExchange> exchangeClass) {
      return withOperandSupplier(b0 ->
          b0.operand(sortClass).oneInput(b1 ->
              b1.operand(exchangeClass).anyInputs()))
          .as(Config.class);
    }
  }
}
