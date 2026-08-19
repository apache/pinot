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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;


/// Special rule for Pinot, this rule is fixed to always insert an exchange below the MATCH_RECOGNIZE node, mirroring
/// what [PinotWindowExchangeNodeInsertRule] does for `WINDOW`.
///
/// Row pattern recognition is only correct when every row of a `PARTITION BY` partition is evaluated by the same
/// worker, in `ORDER BY` order. The rule therefore inserts a [PinotLogicalSortExchange] hash-distributed on the
/// `PARTITION BY` keys, sorted on the receiver side.
///
/// **The receiver-side collation is `(partitionKeys..., orderByKeys...)`, not just the `ORDER BY` keys.** Prepending
/// the partition keys makes rows arrive clustered by partition, so the runtime operator can match and flush one
/// partition at a time and release its buffer at each partition boundary. Sorting on the `ORDER BY` keys alone would
/// interleave partitions and force the operator to buffer a `Map<partitionKey, List<row>>` for the whole input until
/// end-of-stream. An `ORDER BY` key that is also a partition key is dropped from the tail: it is constant inside a
/// partition, so re-sorting on it there is a no-op.
///
/// Sorting happens on the receiver only. Sender-side sorting is not implemented for exchanges yet (see the same
/// note in [PinotWindowExchangeNodeInsertRule]).
///
/// A query with no `PARTITION BY` is rejected, because hashing on zero keys sends the entire table to one worker.
/// Set the [QueryOptionKey#ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY] query option to opt into that plan.
///
/// TODO: Support sender-side sorting so the receiver can k-way merge already sorted streams instead of re-sorting.
public class PinotMatchExchangeNodeInsertRule extends RelOptRule {
  public static final PinotMatchExchangeNodeInsertRule INSTANCE =
      new PinotMatchExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotMatchExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(Match.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Match match = call.rel(0);
    return !PinotRuleUtils.isExchange(match.getInput());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Match match = call.rel(0);
    List<Integer> partitionKeys = match.getPartitionKeys().toList();
    if (partitionKeys.isEmpty() && !isMatchWithoutPartitionByAllowed(call)) {
      throw new QueryException(QueryErrorCode.QUERY_PLANNING,
          "MATCH_RECOGNIZE without a PARTITION BY clause is not allowed because the whole table has to be routed to "
              + "a single worker, which does not scale and can exhaust that worker's memory. Add a PARTITION BY "
              + "clause on a column with enough distinct values, or set the query option '"
              + QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY + "=true' to accept single-worker "
              + "execution.");
    }

    RelNode input = match.getInput();
    RelCollation collation = clusterByPartitionKeys(partitionKeys, match.getOrderKeys());
    Exchange exchange;
    if (collation.getFieldCollations().isEmpty()) {
      // Neither PARTITION BY nor ORDER BY: nothing to sort on, so a plain exchange onto a single worker is enough.
      exchange = PinotLogicalExchange.create(input, RelDistributions.hash(partitionKeys));
    } else {
      exchange = PinotLogicalSortExchange.create(input, RelDistributions.hash(partitionKeys), collation, false, true);
    }
    call.transformTo(match.copy(match.getTraitSet(), List.of(exchange)));
  }

  /// Builds the receiver-side collation `(partitionKeys..., orderByKeys...)` so that rows arrive grouped by
  /// partition and, within a partition, in `ORDER BY` order.
  private static RelCollation clusterByPartitionKeys(List<Integer> partitionKeys, RelCollation orderKeys) {
    if (partitionKeys.isEmpty()) {
      return orderKeys;
    }
    List<RelFieldCollation> fieldCollations = new ArrayList<>(partitionKeys.size());
    for (int partitionKey : partitionKeys) {
      // The direction is irrelevant for clustering, any total order groups the partitions together.
      fieldCollations.add(new RelFieldCollation(partitionKey));
    }
    Set<Integer> partitionKeySet = new HashSet<>(partitionKeys);
    for (RelFieldCollation fieldCollation : orderKeys.getFieldCollations()) {
      // A partition key is constant within its partition, so sorting on it again is a no-op.
      if (!partitionKeySet.contains(fieldCollation.getFieldIndex())) {
        fieldCollations.add(fieldCollation);
      }
    }
    return RelCollations.of(fieldCollations);
  }

  private static boolean isMatchWithoutPartitionByAllowed(RelOptRuleCall call) {
    PlannerContext plannerContext = getPlannerContext(call);
    return plannerContext != null && Boolean.parseBoolean(
        plannerContext.getOptions().get(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY));
  }

  @Nullable
  private static PlannerContext getPlannerContext(RelOptRuleCall call) {
    Context context = call.getPlanner().getContext();
    return context != null ? context.unwrap(PlannerContext.class) : null;
  }
}
