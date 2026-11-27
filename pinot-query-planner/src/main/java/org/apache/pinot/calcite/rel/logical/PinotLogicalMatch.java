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
package org.apache.pinot.calcite.rel.logical;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;


/// Same as [LogicalMatch], except that it also keeps the `PARTITION BY` keys in the order the query wrote them.
///
/// [Match#getPartitionKeys()] is an [ImmutableBitSet], so `asList()` always returns the keys in ascending index order
/// and the source order is gone. It matters: ONE ROW PER MATCH emits the partition columns first, in `PARTITION BY`
/// order, and `MatchOperator` fills the output row positionally, so the bit set order would report each partition
/// column's value under the other one's name, or fail with a `ClassCastException` when the two types differ.
///
/// Calcite keeps the source order only implicitly, as the names of the leading output fields, and those names can
/// only be matched against the input field names on the tree that `SqlToRelConverter` returns. Field trimming turns a
/// project that only renames into a virtual rename, so afterwards the input carries the table's column names instead
/// of the aliases the `PARTITION BY` clause refers to. [#pinPartitionKeyOrder(RelNode)] therefore recovers the order
/// straight after conversion, and [#copy] carries it through decorrelation, field trimming and the planner rules.
///
/// The only state it adds is an immutable list, so it has the thread-safety of any other rel node: it is built and
/// used by the single thread that plans the query.
public class PinotLogicalMatch extends LogicalMatch {
  private final List<Integer> _partitionKeysInSourceOrder;

  private PinotLogicalMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType rowType,
      RexNode pattern, boolean strictStart, boolean strictEnd, Map<String, RexNode> patternDefinitions,
      Map<String, RexNode> measures, RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows,
      List<Integer> partitionKeysInSourceOrder, RelCollation orderKeys, @Nullable RexNode interval) {
    super(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after,
        subsets, allRows, ImmutableBitSet.of(partitionKeysInSourceOrder), orderKeys, interval);
    _partitionKeysInSourceOrder = List.copyOf(partitionKeysInSourceOrder);
  }

  /// Creates a match whose `PARTITION BY` keys are already known in source order.
  public static PinotLogicalMatch create(RelNode input, RelDataType rowType, RexNode pattern, boolean strictStart,
      boolean strictEnd, Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures, RexNode after,
      Map<String, ? extends SortedSet<String>> subsets, boolean allRows, List<Integer> partitionKeysInSourceOrder,
      RelCollation orderKeys, @Nullable RexNode interval) {
    RelOptCluster cluster = input.getCluster();
    return new PinotLogicalMatch(cluster, cluster.traitSetOf(Convention.NONE), input, rowType, pattern, strictStart,
        strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeysInSourceOrder, orderKeys,
        interval);
  }

  /// Replaces every [LogicalMatch] in `rel` with a [PinotLogicalMatch].
  ///
  /// This must run on the tree that `SqlToRelConverter` returns, before decorrelation and field trimming, because
  /// the `PARTITION BY` order is recovered by name (see [#create(LogicalMatch)]).
  public static RelNode pinPartitionKeyOrder(RelNode rel) {
    return rel.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(LogicalMatch match) {
        RelNode visited = super.visit(match);
        return visited instanceof PinotLogicalMatch ? visited : create((LogicalMatch) visited);
      }
    });
  }

  /// Creates a match from Calcite's, recovering the `PARTITION BY` source order from the output row type.
  ///
  /// The output row type built by `SqlValidatorImpl.validateMatchRecognize` keeps the source order: its leading fields
  /// are the partition columns, named after the last component of each `PARTITION BY` identifier. The number of
  /// leading partition fields is derived from the measure count rather than from `partitionKeys.cardinality()`,
  /// because `PARTITION BY col, col` contributes two output fields but only one bit. The names are resolved against
  /// the partition key set only, case-sensitively first, because the schema may be case-insensitive and the row type
  /// carries the identifier as it was written rather than as it was resolved.
  private static PinotLogicalMatch create(LogicalMatch match) {
    ImmutableBitSet partitionKeys = match.getPartitionKeys();
    List<String> outputFieldNames = match.getRowType().getFieldNames();
    int numPartitionFields = outputFieldNames.size() - match.getMeasures().size();
    List<Integer> partitionKeysInSourceOrder;
    if (numPartitionFields <= 1) {
      // Nothing to reorder, and no name to resolve.
      partitionKeysInSourceOrder = partitionKeys.asList();
    } else {
      List<String> inputFieldNames = match.getInput().getRowType().getFieldNames();
      partitionKeysInSourceOrder = new ArrayList<>(numPartitionFields);
      for (int i = 0; i < numPartitionFields; i++) {
        partitionKeysInSourceOrder.add(resolvePartitionKey(outputFieldNames.get(i), partitionKeys, inputFieldNames));
      }
    }
    // A future Calcite change to how the Match row type is built must fail loudly rather than silently misalign rows.
    Preconditions.checkState(ImmutableBitSet.of(partitionKeysInSourceOrder).equals(partitionKeys),
        "MATCH_RECOGNIZE partition keys %s recovered from the output row type %s do not match Calcite's %s",
        partitionKeysInSourceOrder, outputFieldNames, partitionKeys);
    return new PinotLogicalMatch(match.getCluster(), match.getTraitSet(), match.getInput(), match.getRowType(),
        match.getPattern(), match.isStrictStart(), match.isStrictEnd(), match.getPatternDefinitions(),
        match.getMeasures(), match.getAfter(), match.getSubsets(), match.isAllRows(), partitionKeysInSourceOrder,
        match.getOrderKeys(), match.getInterval());
  }

  private static int resolvePartitionKey(String name, ImmutableBitSet partitionKeys, List<String> inputFieldNames) {
    int caseInsensitiveMatch = -1;
    for (int index : partitionKeys) {
      String inputFieldName = inputFieldNames.get(index);
      if (inputFieldName.equals(name)) {
        return index;
      }
      if (inputFieldName.equalsIgnoreCase(name)) {
        caseInsensitiveMatch = index;
      }
    }
    Preconditions.checkState(caseInsensitiveMatch >= 0,
        "MATCH_RECOGNIZE partition column '%s' does not resolve to any of the partition key columns %s of %s", name,
        partitionKeys, inputFieldNames);
    return caseInsensitiveMatch;
  }

  /// The input column indexes of the `PARTITION BY` keys, in `PARTITION BY` source order. A key repeated in
  /// `PARTITION BY` is repeated here too, so there is one entry per leading partition field of the output row.
  public List<Integer> getPartitionKeysInSourceOrder() {
    return _partitionKeysInSourceOrder;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PinotLogicalMatch(getCluster(), traitSet, sole(inputs), getRowType(), pattern, strictStart, strictEnd,
        patternDefinitions, measures, after, subsets, allRows, _partitionKeysInSourceOrder, orderKeys, interval);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    // The order is part of the digest, so the planner never merges two matches that differ only in it.
    return super.explainTerms(pw).itemIf("partitionOrder", _partitionKeysInSourceOrder,
        !_partitionKeysInSourceOrder.equals(partitionKeys.asList()));
  }
}
