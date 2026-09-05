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
package org.apache.pinot.query.planner.explain;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests for [PlanNodeMerger], the logic that decides whether two explain plan nodes describe the same plan and
/// can be merged into one.
public class PlanNodeMergerTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"col"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
  private static final DataSchema MATCH_SCHEMA = new DataSchema(new String[]{"col", "value"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

  private static ExplainedNode leaf(String title) {
    return new ExplainedNode(0, SCHEMA, null, List.of(), title, Map.of());
  }

  private static ExplainedNode leaf(String title, Map<String, Plan.ExplainNode.AttributeValue> attributes) {
    return new ExplainedNode(0, SCHEMA, null, List.of(), title, attributes);
  }

  private static ExchangeNode exchange(Set<String> tableNames) {
    return new ExchangeNode(0, SCHEMA, List.of(leaf("Scan")),
        PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.BROADCAST_DISTRIBUTED, null, false, null,
        false, false, tableNames, ExchangeStrategy.BROADCAST_EXCHANGE, "absHashCodeMurmur3");
  }

  @Test
  public void exchangesWithSameTableNamesMerge() {
    PlanNode merged = PlanNodeMerger.mergePlans(exchange(Set.of("t1")), exchange(Set.of("t1")), false);
    assertNotNull(merged, "Exchange nodes describing the same tables must be mergeable");
  }

  @Test
  public void exchangesWithDifferentTableNamesDoNotMerge() {
    PlanNode merged = PlanNodeMerger.mergePlans(exchange(Set.of("t1")), exchange(Set.of("t2")), false);
    assertNull(merged, "Exchange nodes over different tables must not be merged");
  }

  @Test
  public void differentNodeTypesDoNotMerge() {
    ProjectNode project = new ProjectNode(0, SCHEMA, NodeHint.EMPTY, List.of(leaf("Scan")),
        List.of());
    assertNull(PlanNodeMerger.mergePlans(project, leaf("Scan"), false));
  }

  @Test
  public void equivalentMatchesMerge() {
    MatchNode first = match("baseline", leaf("Scan", new ExplainAttributeBuilder().putLong("rows", 2).build()));
    MatchNode second = match("baseline", leaf("Scan", new ExplainAttributeBuilder().putLong("rows", 3).build()));
    PlanNode merged = PlanNodeMerger.mergePlans(first, second, false);

    assertNotNull(merged, "Semantically identical MATCH_RECOGNIZE nodes must be mergeable");
    assertTrue(merged instanceof MatchNode);
    ExplainedNode mergedInput = (ExplainedNode) merged.getInputs().get(0);
    assertEquals(mergedInput.getAttributes().get("rows").getLong(), 5L,
        "The MATCH_RECOGNIZE node must retain the recursively merged input");
  }

  @Test
  public void matchAndDifferentNodeTypeDoNotMerge() {
    assertNull(PlanNodeMerger.mergePlans(match("baseline"), leaf("Scan"), false));
  }

  @Test(dataProvider = "nonEquivalentMatchProperties")
  public void differentMatchPropertiesDoNotMerge(String variation) {
    assertNull(PlanNodeMerger.mergePlans(match("baseline"), match(variation), false),
        "MATCH_RECOGNIZE nodes that differ in " + variation + " must not be merged");
  }

  @DataProvider(name = "nonEquivalentMatchProperties")
  public static Object[][] nonEquivalentMatchProperties() {
    return new Object[][]{
        {"pattern symbols"},
        {"pattern"},
        {"measures"},
        {"partition keys"},
        {"collations"},
        {"skip mode"},
        {"skip target"},
        {"rows per match"},
        {"input"}
    };
  }

  private static MatchNode match(String variation) {
    return match(variation, leaf("Scan"));
  }

  private static MatchNode match(String variation, PlanNode input) {
    List<PatternSymbol> patternSymbols = List.of(new PatternSymbol("A", null), new PatternSymbol("B", null));
    RowPattern pattern = new RowPattern.Concat(List.of(
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 1, RowPattern.Quantifier.UNBOUNDED, true),
        new RowPattern.Symbol(1)));
    List<MatchNode.Measure> measures = List.of(
        new MatchNode.Measure("value", new RexExpression.PatternFieldRef(0, 0, "A")));
    List<Integer> partitionKeys = List.of(0);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0));
    MatchNode.AfterMatchSkipMode skipMode = MatchNode.AfterMatchSkipMode.TO_FIRST;
    int skipTarget = 0;
    MatchNode.RowsPerMatchMode rowsPerMatchMode = MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH;
    switch (variation) {
      case "baseline":
        break;
      case "pattern symbols":
        patternSymbols = List.of(new PatternSymbol("A", RexExpression.Literal.FALSE), new PatternSymbol("B", null));
        break;
      case "pattern":
        pattern = new RowPattern.Concat(List.of(new RowPattern.Symbol(0), new RowPattern.Symbol(1)));
        break;
      case "measures":
        measures = List.of(new MatchNode.Measure("value", new RexExpression.PatternFieldRef(0, 1, "B")));
        break;
      case "partition keys":
        partitionKeys = List.of();
        break;
      case "collations":
        collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
        break;
      case "skip mode":
        skipMode = MatchNode.AfterMatchSkipMode.TO_LAST;
        break;
      case "skip target":
        skipTarget = 1;
        break;
      case "rows per match":
        rowsPerMatchMode = MatchNode.RowsPerMatchMode.ALL_ROWS_PER_MATCH;
        break;
      case "input":
        input = leaf("Different scan");
        break;
      default:
        throw new IllegalArgumentException("Unknown variation: " + variation);
    }

    return new MatchNode(0, MATCH_SCHEMA, NodeHint.EMPTY, List.of(input), patternSymbols, pattern, measures,
        partitionKeys, collations, skipMode, skipTarget, rowsPerMatchMode);
  }
}
