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
package org.apache.pinot.query.planner.serde;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


public class PlanNodeSerDeTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryStagePlanSerDe(String query) {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    for (DispatchablePlanFragment dispatchablePlanFragment : dispatchableSubPlan.getQueryStages()) {
      PlanNode stagePlan = dispatchablePlanFragment.getPlanFragment().getFragmentRoot();
      PlanNode deserializedStagePlan = PlanNodeDeserializer.process(PlanNodeSerializer.process(stagePlan));
      assertEquals(stagePlan, deserializedStagePlan);
    }
  }

  @Test
  public void testPrunedUnnestNodeSerDe() {
    // Round-trips the passthrough-pruning wire fields (passthroughInputIndexes, prunedPassthrough). A non-sequential
    // index list plus WITH ORDINALITY exercise the proto repeated/bool fields and ordering.
    DataSchema dataSchema = new DataSchema(new String[]{"col0", "col2", "elem", "ord"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    UnnestNode.TableFunctionContext context =
        new UnnestNode.TableFunctionContext(true, List.of(2), 3, List.of(0, 2), true);
    UnnestNode node = new UnnestNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(),
        List.of(new RexExpression.InputRef(1)), context);

    PlanNode deserialized = PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    UnnestNode deserializedUnnest = (UnnestNode) deserialized;
    assertEquals(deserializedUnnest.getPassthroughInputIndexes(), List.of(0, 2));
    assertEquals(deserializedUnnest.isPrunedPassthrough(), true);
    assertEquals(deserializedUnnest.getOrdinalityIndex(), 3);
  }

  @Test
  public void testLegacyUnnestNodeSerDe() {
    // A non-pruned UnnestNode must round-trip with prunedPassthrough=false and an empty passthrough map (the wire
    // default an old broker produces).
    DataSchema dataSchema = new DataSchema(new String[]{"id", "arr", "elem"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT_ARRAY, ColumnDataType.INT});
    UnnestNode node = new UnnestNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(),
        new RexExpression.InputRef(1), "elem", false, null);

    UnnestNode deserialized = (UnnestNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    assertEquals(deserialized.isPrunedPassthrough(), false);
    assertEquals(deserialized.getPassthroughInputIndexes(), List.of());
  }

  /// Enriched joins have been removed, but [EnrichedJoinNode], proto field 17 and the serde are retained so a
  /// plan produced by an older-version broker still round-trips (see [EnrichedJoinNode] deprecation note). The
  /// planner no longer produces this node, so this direct round-trip is the only guard on that wire format. Because
  /// `JoinNode#equals` ignores the enriched-specific fields, assert on them explicitly rather than via equals.
  @Test
  @SuppressWarnings("deprecation")
  public void testEnrichedJoinNodeSerDe() {
    DataSchema joinResultSchema = new DataSchema(new String[]{"l0", "r0"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    DataSchema projectResultSchema = new DataSchema(new String[]{"p0"},
        new ColumnDataType[]{ColumnDataType.INT});
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = List.of(
        new EnrichedJoinNode.FilterProjectRex(new RexExpression.InputRef(0)),
        new EnrichedJoinNode.FilterProjectRex(List.of(new RexExpression.InputRef(1)), projectResultSchema));
    EnrichedJoinNode node = new EnrichedJoinNode(1, joinResultSchema, projectResultSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), JoinRelType.INNER, List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH, null,
        filterProjectRexes, 10, 5);

    EnrichedJoinNode deserialized = (EnrichedJoinNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized.getFetch(), 10);
    assertEquals(deserialized.getOffset(), 5);
    assertEquals(deserialized.getJoinResultSchema(), joinResultSchema);
    assertEquals(deserialized.getDataSchema(), projectResultSchema);
    List<EnrichedJoinNode.FilterProjectRex> roundTripped = deserialized.getFilterProjectRexes();
    assertEquals(roundTripped.size(), 2);
    assertEquals(roundTripped.get(0).getType(), EnrichedJoinNode.FilterProjectRexType.FILTER);
    assertEquals(roundTripped.get(0).getFilter(), new RexExpression.InputRef(0));
    assertEquals(roundTripped.get(1).getType(), EnrichedJoinNode.FilterProjectRexType.PROJECT);
    assertEquals(roundTripped.get(1).getProjectAndResultSchema().getProject(),
        List.of(new RexExpression.InputRef(1)));
    assertEquals(roundTripped.get(1).getProjectAndResultSchema().getSchema(), projectResultSchema);
  }

  @Test
  public void testAggregateGroupingSetsSerDe() {
    /// The grouping sets (member indexes over the union group keys, in ordinal order) must survive serialization
    /// to the worker. ROLLUP(g0, g1) over the union {g0, g1} expands to the sets (g0, g1), (g0), (). The empty
    /// grand-total set in particular must round-trip as an entry (not vanish as a proto default).
    DataSchema schema = new DataSchema(new String[]{"g0", "g1", "sum"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<List<Integer>> groupingSets = List.of(List.of(0, 1), List.of(0), List.of());
    AggregateNode node = new AggregateNode(0, schema, PlanNode.NodeHint.EMPTY, List.of(), List.of(), List.of(),
        List.of(0, 1), AggType.DIRECT, false, List.of(), 0, groupingSets);
    AggregateNode deserialized = (AggregateNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized.getGroupingSets(), groupingSets);
    assertEquals(deserialized, node);
  }

  /// Round-trips a MATCH_RECOGNIZE node whose pattern is `^ A (B{2,3} | C+?) D $`: nested alternation, a bounded
  /// quantifier, a reluctant quantifier and both anchors. Also covers the pattern-variable symbol table, a DEFINE
  /// predicate carrying a {@link RexExpression.PatternFieldRef}, MEASURES and `AFTER MATCH SKIP TO LAST C`.
  @Test
  public void testMatchNodeSerDe() {
    MatchNode node = buildMatchNode(MatchNode.AfterMatchSkipMode.TO_LAST, 2);

    MatchNode deserialized = (MatchNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    assertEquals(deserialized.getPatternString(), "^ A (B{2,3} | C+?) D $");
    assertEquals(deserialized.getAfterMatchSkipMode(), MatchNode.AfterMatchSkipMode.TO_LAST);
    assertEquals(deserialized.getAfterMatchSkipToSymbolOrdinal(), 2);
    assertEquals(deserialized.getRowsPerMatchMode(), MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
    assertEquals(deserialized.getPartitionKeys(), List.of(0));
    assertEquals(deserialized.getCollations().size(), 1);

    // The pattern variable of a DEFINE reference must survive: degrading it to a plain InputRef would silently turn
    // `B.price` into a read of the current row.
    RexExpression definition = deserialized.getPatternSymbols().get(1).getDefinition();
    RexExpression.PatternFieldRef ref =
        (RexExpression.PatternFieldRef) ((RexExpression.FunctionCall) definition).getFunctionOperands().get(0);
    assertEquals(ref.getSymbolOrdinal(), 1);
    assertEquals(ref.getIndex(), 1);
    assertEquals(ref.getAlpha(), "B");

    // Variables without a DEFINE entry match every row and must round-trip as "no definition", not as a default.
    assertNull(deserialized.getPatternSymbols().get(0).getDefinition());
  }

  /// `AFTER MATCH SKIP PAST LAST ROW` has no target variable. Ordinal 0 is a valid pattern variable, so "no target"
  /// must not be encoded as the proto3 default of the field.
  @Test
  public void testMatchNodeWithoutSkipToSymbolSerDe() {
    MatchNode node = buildMatchNode(MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL);

    MatchNode deserialized = (MatchNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    assertEquals(deserialized.getAfterMatchSkipToSymbolOrdinal(), MatchNode.NO_SKIP_TO_SYMBOL);
  }

  /// A pattern field reference that was never bound to a pattern symbol must not reach the wire: an ambiguous
  /// reference would be resolved arbitrarily by the server and produce wrong-but-type-correct results.
  @Test
  public void testUnresolvedPatternFieldRefIsRejected() {
    DataSchema dataSchema = new DataSchema(new String[]{"sym", "startPrice"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    RexExpression.PatternFieldRef unresolved =
        new RexExpression.PatternFieldRef(1, RexExpression.PatternFieldRef.UNRESOLVED_SYMBOL_ORDINAL, "A");
    MatchNode node = new MatchNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(),
        List.of(new PatternSymbol("A", null)), new RowPattern.Symbol(0),
        List.of(new MatchNode.Measure("startPrice", unresolved)), List.of(0),
        List.of(new RelFieldCollation(1)), MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);

    assertThrows(IllegalStateException.class, () -> PlanNodeSerializer.process(node));
  }

  private static MatchNode buildMatchNode(MatchNode.AfterMatchSkipMode skipMode, int skipToSymbolOrdinal) {
    // PATTERN (^ A (B{2,3} | C+?) D $) over the symbol table [A, B, C, D].
    List<PatternSymbol> patternSymbols = List.of(
        new PatternSymbol("A", null),
        new PatternSymbol("B", new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "GREATER_THAN",
            List.of(new RexExpression.PatternFieldRef(1, 1, "B"),
                new RexExpression.Literal(ColumnDataType.DOUBLE, 100.0d)))),
        new PatternSymbol("C", new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "LESS_THAN",
            List.of(new RexExpression.PatternFieldRef(1, 2, "C"),
                new RexExpression.Literal(ColumnDataType.DOUBLE, 100.0d)))),
        new PatternSymbol("D", null));
    RowPattern pattern = new RowPattern.Concat(List.of(
        RowPattern.AnchorStart.INSTANCE,
        new RowPattern.Symbol(0),
        new RowPattern.Alternate(List.of(
            new RowPattern.Quantify(new RowPattern.Symbol(1), 2, 3, true),
            new RowPattern.Quantify(new RowPattern.Symbol(2), 1, RowPattern.Quantify.UNBOUNDED, false))),
        new RowPattern.Symbol(3),
        RowPattern.AnchorEnd.INSTANCE));
    List<MatchNode.Measure> measures = List.of(
        new MatchNode.Measure("startPrice", new RexExpression.PatternFieldRef(1, 0, "A")),
        new MatchNode.Measure("matchNum",
            new RexExpression.FunctionCall(ColumnDataType.LONG, "MATCH_NUMBER", List.of())));
    DataSchema dataSchema = new DataSchema(new String[]{"sym", "startPrice", "matchNum"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.LONG});

    return new MatchNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(), patternSymbols, pattern, measures,
        List.of(0), List.of(new RelFieldCollation(1)), skipMode, skipToSymbolOrdinal,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
  }
}
