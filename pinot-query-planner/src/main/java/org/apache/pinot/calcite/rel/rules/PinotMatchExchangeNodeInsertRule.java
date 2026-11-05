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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.immutables.value.Value;


/// Special rule for Pinot, this rule is fixed to always insert an exchange below the MATCH_RECOGNIZE node, mirroring
/// what [PinotWindowExchangeNodeInsertRule] does for `WINDOW`.
///
/// Row pattern recognition is only correct when every row of a `PARTITION BY` partition is evaluated by the same
/// worker, in `ORDER BY` order. The rule therefore inserts an exchange hash-distributed on the `PARTITION BY` keys and
/// an explicit Sort above it, which orders each worker's input before MATCH_RECOGNIZE reads it:
///
/// ```
/// Match
///   LogicalSort(collation = (partitionKeys..., orderByKeys...), no fetch, no offset)
///     PinotLogicalSortExchange(hash[partitionKeys], same collation, sorted on neither side)
/// ```
///
/// **The collation is `(partitionKeys..., orderByKeys...)`, not just the `ORDER BY` keys.** Prepending the partition
/// keys makes rows arrive clustered by partition, so the runtime operator can match and flush one partition at a time
/// and release its buffer at each partition boundary. Sorting on the `ORDER BY` keys alone would interleave partitions
/// and force the operator to buffer a `Map<partitionKey, List<row>>` for the whole input until end-of-stream. An
/// `ORDER BY` key that is also a partition key is dropped from the tail: it is constant inside a partition, so
/// re-sorting on it there is a no-op.
///
/// The order is established by the Sort rather than by the receive side of the exchange, as for `WINDOW` and for
/// ordered aggregates. The Sort runs as a `SortOperator`, which checks for termination and samples resource usage as
/// each input block arrives, and hands its result to MATCH_RECOGNIZE in bounded blocks. Sorting on the receive side
/// would build the deprecated `SortedMailboxReceiveOperator` instead, which samples nothing while it collects and
/// returns the worker's whole input as a single block.
///
/// ## Memory
///
/// This does not make MATCH_RECOGNIZE stream. The Sort has no fetch, so it runs as a full sort: it buffers every row
/// the worker receives for this MATCH_RECOGNIZE, across all of the worker's partitions, and sorts them once, so
/// matching cannot start before the worker's entire input has arrived. The sorted rows stay reachable from the sort
/// while MATCH_RECOGNIZE works through them, so releasing each partition at its boundary does not lower that peak.
/// The peak is the same as with a receive-side sort: the choice of operator changes how the accumulation is observed
/// and handed over, not how much is accumulated. The per-partition limit
/// ([QueryOptionKey#MAX_ROWS_IN_MATCH_PARTITION]) does not bound it either. That limit applies to the rows
/// MATCH_RECOGNIZE buffers for one partition after the sort, not to the sort buffer below it.
///
/// Sender-side sorting is not implemented for exchanges yet (see the same note in [PinotWindowExchangeNodeInsertRule]).
///
/// A query with no `PARTITION BY` is rejected, because hashing on zero keys sends the entire table to one worker.
/// Set the [QueryOptionKey#ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY] query option to opt into that plan.
///
/// TODO(#19395): Support sender-side sorting so the receiver can k-way merge already sorted streams instead of
/// re-sorting.
@Value.Enclosing
public class PinotMatchExchangeNodeInsertRule extends RelRule<PinotMatchExchangeNodeInsertRule.Config> {
  public static final PinotMatchExchangeNodeInsertRule INSTANCE =
      new PinotMatchExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotMatchExchangeNodeInsertRule(RelBuilderFactory factory) {
    this(ImmutablePinotMatchExchangeNodeInsertRule.Config.builder()
        .relBuilderFactory(factory)
        .operandSupplier(b0 -> b0.operand(Match.class).anyInputs())
        .build());
  }

  private PinotMatchExchangeNodeInsertRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Match match = call.rel(0);
    RelNode input = PinotRuleUtils.unboxRel(match.getInput());
    if (input instanceof Exchange) {
      return false;
    }
    // This rule leaves a Sort over the exchange whenever there is a collation (see onMatch), so that shape also means
    // the MATCH has already been processed. Without this the rule re-fires on its own output forever.
    //
    // The shape has to match exactly. A Sort that orders by something else, that trims, or that does not sit on an
    // exchange hashed on the PARTITION BY keys belongs to another part of the plan - treating it as this rule's own
    // output would leave the MATCH without the distribution or the ordering it requires, because MatchOperator does
    // no ordering of its own.
    if (!(input instanceof Sort)) {
      return true;
    }
    Sort sort = (Sort) input;
    if (sort.fetch != null || sort.offset != null) {
      return true;
    }
    RelNode sortInput = PinotRuleUtils.unboxRel(sort.getInput());
    List<Integer> partitionKeys = match.getPartitionKeys().toList();
    return !(sortInput instanceof Exchange
        && ((Exchange) sortInput).getDistribution().equals(RelDistributions.hash(partitionKeys))
        && sort.getCollation().equals(clusterByPartitionKeys(partitionKeys, match.getOrderKeys())));
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
    RelDistribution distribution = RelDistributions.hash(partitionKeys);
    RelCollation collation = clusterByPartitionKeys(partitionKeys, match.getOrderKeys());
    RelNode matchInput;
    if (collation.getFieldCollations().isEmpty()) {
      // Neither PARTITION BY nor ORDER BY: nothing to sort on, so a plain exchange onto a single worker is enough.
      matchInput = PinotLogicalExchange.create(input, distribution);
    } else {
      // MatchOperator does no ordering of its own, so the order is established by an explicit Sort above the exchange
      // rather than by the receive side: SortOperator samples resource usage per received block and emits bounded
      // blocks, while sorting on the receive side would build the deprecated SortedMailboxReceiveOperator. The Sort
      // carries no fetch or offset, so it keeps every row and runs as a full sort.
      // PinotSortExchangeNodeInsertRule does not re-fire on it: its matches() rejects a Sort whose input is an
      // exchange. PinotSortExchangeCopyRule does not either: it declines when there is no fetch.
      PinotLogicalSortExchange exchange = PinotLogicalSortExchange.create(input, distribution, collation, false, false);
      matchInput = LogicalSort.create(exchange, collation, null, null);
    }
    call.transformTo(match.copy(match.getTraitSet(), List.of(matchInput)));
  }

  /// Builds the collation `(partitionKeys..., orderByKeys...)` of the Sort below the MATCH, so that rows reach it
  /// grouped by partition and, within a partition, in `ORDER BY` order.
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

  /// Configuration for the MATCH_RECOGNIZE exchange rule.
  @Value.Immutable
  public interface Config extends RelRule.Config {
    @Override
    default PinotMatchExchangeNodeInsertRule toRule() {
      return new PinotMatchExchangeNodeInsertRule(this);
    }
  }
}
