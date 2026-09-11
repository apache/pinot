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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the two MATCH_RECOGNIZE rel conversions: [RelToPlanNodeConverter] lowering Calcite's `LogicalMatch` into a
/// [MatchNode], and [PlanNodeToRelConverter] rebuilding a `LogicalMatch` from it.
public class MatchNodeConverterTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "patternQueries")
  public void testPatternIsLowered(String patternClause, String expectedPattern) {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + "PATTERN (" + patternClause + ") DEFINE A AS A.col3 > 0, B AS B.col3 > 0, C AS C.col3 > 0)");
    assertEquals(match.getPatternString(), expectedPattern);
  }

  @DataProvider(name = "patternQueries")
  public Object[][] patternQueries() {
    //@formatter:off
    return new Object[][]{
        new Object[]{"A", "A"},
        // Calcite builds a left deep binary PATTERN_CONCAT tree; it is flattened into a single n-ary node.
        new Object[]{"A B C", "A B C"},
        new Object[]{"A | B | C", "(A | B | C)"},
        new Object[]{"A (B | C)", "A (B | C)"},
        new Object[]{"A*", "A*"},
        new Object[]{"A+", "A+"},
        new Object[]{"A?", "A?"},
        new Object[]{"A{3}", "A{3}"},
        new Object[]{"A{2,}", "A{2,}"},
        new Object[]{"A{2,5}", "A{2,5}"},
        // Calcite writes -1 for the omitted minimum of `{,m}`; SQL:2016 reads it as 0.
        new Object[]{"A{,4}", "A{0,4}"},
        new Object[]{"A*?", "A*?"},
        new Object[]{"A+?", "A+?"},
        new Object[]{"A{2,5}?", "A{2,5}?"},
        // A quantifier over a concatenation has to be parenthesized when rendered back.
        new Object[]{"(A B)+", "(A B)+"},
        new Object[]{"^ A B $", "^ A B $"},
        new Object[]{"^ A", "^ A"},
        new Object[]{"A $", "A $"},
        new Object[]{"^ (A | B) $", "^ (A | B) $"},
        new Object[]{"^ A B{2,3} (B | C)+? $", "^ A B{2,3} (B | C)+? $"}
    };
    //@formatter:on
  }

  @Test
  public void testQuantifierGreediness() {
    RowPattern greedy = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + "PATTERN (A*) DEFINE A AS A.col3 > 0)").getPattern();
    RowPattern reluctant = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + "PATTERN (A*?) DEFINE A AS A.col3 > 0)").getPattern();
    assertTrue(((RowPattern.Quantifier) greedy).isGreedy());
    assertFalse(((RowPattern.Quantifier) reluctant).isGreedy());
    assertEquals(((RowPattern.Quantifier) greedy).getMinRepeat(), 0);
    assertEquals(((RowPattern.Quantifier) greedy).getMaxRepeat(), RowPattern.Quantifier.UNBOUNDED);
  }

  @Test
  public void testSymbolTableAndDefinitions() {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + "PATTERN (A B C) DEFINE C AS C.col3 > 0, A AS A.col3 > 0)");
    // Ordered by first appearance in PATTERN, not by DEFINE order, so the ordinals do not depend on the iteration
    // order of Calcite's pattern definition map. B has no DEFINE entry, so it matches every row.
    assertEquals(symbolNames(match), List.of("A", "B", "C"));
    assertNotNull(match.getPatternSymbols().get(0).getDefinition());
    assertNull(match.getPatternSymbols().get(1).getDefinition());
    assertNotNull(match.getPatternSymbols().get(2).getDefinition());
  }

  @Test
  public void testPatternFieldRefsAreBoundToSymbolOrdinals() {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts "
        + "MEASURES LAST(B.col3) AS ev PATTERN (A B) DEFINE A AS A.col3 > 0, B AS B.col3 > A.col3)");
    assertEquals(symbolNames(match), List.of("A", "B"));
    // DEFINE B references both B and A; every reference must carry the ordinal of its variable.
    List<RexExpression.PatternFieldRef> refs = collectPatternFieldRefs(match.getPatternSymbols().get(1)
        .getDefinition());
    assertEquals(refs.size(), 2);
    assertEquals(refs.get(0).getAlpha(), "B");
    assertEquals(refs.get(0).getSymbolOrdinal(), 1);
    assertEquals(refs.get(1).getAlpha(), "A");
    assertEquals(refs.get(1).getSymbolOrdinal(), 0);
    // MEASURES references are bound too.
    List<RexExpression.PatternFieldRef> measureRefs =
        collectPatternFieldRefs(match.getMeasures().get(0).getExpression());
    assertEquals(measureRefs.size(), 1);
    assertEquals(measureRefs.get(0).getSymbolOrdinal(), 1);
  }

  @Test
  public void testUnquotedPatternVariableNamesAreCaseInsensitive() {
    MatchNode match = planMatch("SELECT * FROM a AS src MATCH_RECOGNIZE (ORDER BY ts "
        + "MEASURES LAST(a.col3) AS ev AFTER MATCH SKIP TO FIRST a "
        + "PATTERN (A) DEFINE a AS a.col3 > 0)");

    assertEquals(symbolNames(match), List.of("A"));
    List<RexExpression.PatternFieldRef> definitionRefs =
        collectPatternFieldRefs(match.getPatternSymbols().get(0).getDefinition());
    assertEquals(definitionRefs.size(), 1);
    assertEquals(definitionRefs.get(0).getAlpha(), "A");
    assertEquals(definitionRefs.get(0).getSymbolOrdinal(), 0);
    List<RexExpression.PatternFieldRef> measureRefs =
        collectPatternFieldRefs(match.getMeasures().get(0).getExpression());
    assertEquals(measureRefs.size(), 1);
    assertEquals(measureRefs.get(0).getAlpha(), "A");
    assertEquals(measureRefs.get(0).getSymbolOrdinal(), 0);
    assertEquals(match.getAfterMatchSkipMode(), MatchNode.AfterMatchSkipMode.TO_FIRST);
    assertEquals(match.getAfterMatchSkipToSymbolOrdinal(), 0);
  }

  @Test
  public void testQuotedPatternVariableNamesRemainCaseSensitive() {
    MatchNode match = planMatch("SELECT * FROM a AS src MATCH_RECOGNIZE (ORDER BY ts "
        + "MEASURES LAST(\"a\".col3) AS ev AFTER MATCH SKIP TO FIRST \"a\" "
        + "PATTERN (\"a\") DEFINE \"a\" AS \"a\".col3 > 0)");
    assertEquals(symbolNames(match), List.of("a"));
    assertEquals(match.getAfterMatchSkipToSymbolOrdinal(), 0);

    RuntimeException e = expectThrows(RuntimeException.class, () -> planMatch(
        "SELECT * FROM a AS src MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
            + "AFTER MATCH SKIP TO FIRST \"A\" PATTERN (\"a\") DEFINE \"a\" AS \"a\".col3 > 0)"));
    assertTrue(e.getMessage().contains("Unknown pattern 'A'"), e.getMessage());
  }

  @Test
  public void testUnqualifiedReferenceBindsToUniversalSymbol() {
    // `col3` is not qualified by a pattern variable. Calcite has no representation for the SQL:2016 universal row
    // pattern variable and reuses the row source alias as the alpha, so it must not be mistaken for pattern A.
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + "PATTERN (A) DEFINE A AS col3 > 0)");
    assertEquals(symbolNames(match), List.of("A"));
    List<RexExpression.PatternFieldRef> refs =
        collectPatternFieldRefs(match.getPatternSymbols().get(0).getDefinition());
    assertEquals(refs.size(), 1);
    assertEquals(refs.get(0).getSymbolOrdinal(), RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL);
  }

  @Test(dataProvider = "afterMatchQueries")
  public void testAfterMatchSkipMode(String afterClause, MatchNode.AfterMatchSkipMode expectedMode,
      int expectedOrdinal) {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
        + afterClause + " PATTERN (A B) DEFINE A AS A.col3 > 0, B AS B.col3 > 0)");
    assertEquals(match.getAfterMatchSkipMode(), expectedMode);
    assertEquals(match.getAfterMatchSkipToSymbolOrdinal(), expectedOrdinal);
  }

  @DataProvider(name = "afterMatchQueries")
  public Object[][] afterMatchQueries() {
    //@formatter:off
    return new Object[][]{
        // An omitted AFTER MATCH must reach the plan as SKIP PAST LAST ROW (SQL:2016), not Calcite's SKIP TO NEXT ROW.
        new Object[]{"", MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL},
        new Object[]{
            "AFTER MATCH SKIP PAST LAST ROW", MatchNode.AfterMatchSkipMode.PAST_LAST_ROW,
            MatchNode.NO_SKIP_TO_SYMBOL
        },
        new Object[]{
            "AFTER MATCH SKIP TO NEXT ROW", MatchNode.AfterMatchSkipMode.TO_NEXT_ROW, MatchNode.NO_SKIP_TO_SYMBOL
        },
        // Ordinal 0 is a legal target and must stay distinguishable from "no target".
        new Object[]{"AFTER MATCH SKIP TO FIRST A", MatchNode.AfterMatchSkipMode.TO_FIRST, 0},
        new Object[]{"AFTER MATCH SKIP TO LAST B", MatchNode.AfterMatchSkipMode.TO_LAST, 1}
    };
    //@formatter:on
  }

  @Test
  public void testPartitionOrderAndMeasures() {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts DESC "
        + "MEASURES MATCH_NUMBER() AS mno, CLASSIFIER() AS cls PATTERN (A) DEFINE A AS A.col3 > 0)");
    assertEquals(match.getPartitionKeys().size(), 1);
    assertEquals(match.getCollations().size(), 1);
    assertEquals(match.getRowsPerMatchMode(), MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
    assertEquals(match.getMeasures().size(), 2);
    assertEquals(match.getMeasures().get(0).getName(), "mno");
    assertEquals(match.getMeasures().get(1).getName(), "cls");
    assertEquals(match.explain(), "MATCH_RECOGNIZE");
  }

  @Test
  public void testClassifierMeasureIsNullableAndCountKeepsItsOperand() {
    String matchQuery = "SELECT cls FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts "
        + "MEASURES CLASSIFIER() AS cls PATTERN (A*) DEFINE A AS A.col3 > 0)";
    try (var compiled = _queryEnvironment.compile(matchQuery)) {
      Match match = findMatch(compiled.getRelNode());
      assertNotNull(match, "Expected a match node");
      assertTrue(match.getRowType().getField("cls", true, false).getType().isNullable(),
          "CLASSIFIER() must be nullable because it is null for an empty match");
    }

    try (var compiled = _queryEnvironment.compile("SELECT COUNT(cls) FROM (" + matchQuery + ") matches")) {
      String plan = RelOptUtil.toString(compiled.getRelNode());
      Match match = findMatch(compiled.getRelNode());
      assertNotNull(match, "Expected a match node in:\n" + plan);
      assertTrue(match.getRowType().getField("cls", true, false).getType().isNullable(),
          "CLASSIFIER() must remain nullable below COUNT() in:\n" + plan);
      PinotLogicalAggregate aggregate = findLeafAggregate(compiled.getRelNode());
      assertNotNull(aggregate, "Expected a leaf aggregate in:\n" + plan);
      assertEquals(aggregate.getAggCallList().size(), 1);
      assertEquals(aggregate.getAggCallList().get(0).rexList.size()
              + aggregate.getAggCallList().get(0).getArgList().size(), 1,
          "COUNT(CLASSIFIER()) must not be simplified to COUNT(*) in:\n" + plan);
    }
  }

  /// Calcite's `Match#getPartitionKeys()` is an `ImmutableBitSet`, so `asList()` is ascending index order and the
  /// PARTITION BY source order survives only in the output row type. `MatchOperator` fills the output row
  /// positionally, so passing the bitset order through would report each partition column's value under the other
  /// one's name - or, as in the first case below where the two types differ, blow up in `TypeUtils.convertRow`.
  @Test(dataProvider = "partitionKeyOrderQueries")
  public void testPartitionKeysKeepTheirSourceOrder(String partitionBy, List<Integer> expectedKeys,
      String[] expectedNames, ColumnDataType[] expectedTypes) {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY " + partitionBy + " ORDER BY ts "
        + "MEASURES LAST(X.col3) AS lastv PATTERN (X+) DEFINE X AS X.col3 > 0)");
    assertEquals(match.getPartitionKeys(), expectedKeys);
    assertEquals(match.getDataSchema().getColumnNames(), expectedNames);
    assertEquals(match.getDataSchema().getColumnDataTypes(), expectedTypes);
  }

  @DataProvider(name = "partitionKeyOrderQueries")
  public Object[][] partitionKeyOrderQueries() {
    // Input columns of table `a`: col1 STRING = 0, col2 STRING = 1, col3 INT = 2.
    //@formatter:off
    return new Object[][]{
        // Descending index order: this is what the ImmutableBitSet loses. The two columns have different types, so a
        // regression shows up as a type error rather than only as a value swap.
        new Object[]{
            "col3, col1", List.of(2, 0), new String[]{"col3", "col1", "lastv"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT}
        },
        // Two same-typed columns in descending index order: a regression here is a silent value swap.
        new Object[]{
            "col2, col1", List.of(1, 0), new String[]{"col2", "col1", "lastv"},
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT}
        },
        // The already-ascending case, which was correct by accident before and must stay correct.
        new Object[]{
            "col1, col3", List.of(0, 2), new String[]{"col1", "col3", "lastv"},
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT}
        },
        new Object[]{
            "col1", List.of(0), new String[]{"col1", "lastv"},
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT}
        }
    };
    //@formatter:on
  }

  /// The schema is case-insensitive by default and the Match row type names the partition columns as the query wrote
  /// them rather than as they resolved, so the source order has to survive a case mismatch too.
  @Test
  public void testPartitionKeyOrderSurvivesACaseMismatch() {
    MatchNode match = planMatch("SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY COL3, COL1 ORDER BY ts "
        + "MEASURES LAST(X.col3) AS lastv PATTERN (X+) DEFINE X AS X.col3 > 0)");
    assertEquals(match.getPartitionKeys(), List.of(2, 0));
  }

  @Test
  public void testMultiValuePartitionKeyIsRejectedDuringPlanning() {
    RuntimeException e = expectThrows(RuntimeException.class, () -> planMatch(
        "SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY mcol1 ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
            + "PATTERN (A) DEFINE A AS A.col3 > 0)"));
    assertTrue(e.getMessage().contains("multi-value or ARRAY PARTITION BY columns: 'mcol1'"), e.getMessage());
  }

  @Test(dataProvider = "multiValueAggregateFunctions")
  public void testMultiValueAggregateOperandIsRejectedDuringPlanning(String function) {
    RuntimeException e = expectThrows(RuntimeException.class, () -> planMatch(
        "SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts MEASURES " + function
            + "(A.mcol2) AS value PATTERN (A) DEFINE A AS A.col3 > 0)"));
    assertTrue(e.getMessage().contains("applies aggregate '" + function + "' to a multi-value or ARRAY operand"),
        e.getMessage());
  }

  @DataProvider(name = "multiValueAggregateFunctions")
  public Object[][] multiValueAggregateFunctions() {
    return new Object[][]{
        new Object[]{"SUM"}, new Object[]{"MIN"}, new Object[]{"MAX"}, new Object[]{"AVG"}, new Object[]{"COUNT"}
    };
  }

  /// An unquoted row-source alias and pattern label differing only in cosmetic case are the same SQL name. The input
  /// is internally re-aliased so its unqualified columns retain universal-row semantics.
  @Test
  public void testAliasDifferingOnlyInCosmeticCaseStillBindsToTheUniversalSymbol() {
    MatchNode match = planMatch("SELECT * FROM a AS aa MATCH_RECOGNIZE (ORDER BY ts MEASURES LAST(col3) AS s "
        + "PATTERN (AA B+) DEFINE AA AS AA.col3 > 0, B AS B.col3 > 0)");
    List<RexExpression.PatternFieldRef> refs = collectPatternFieldRefs(match.getMeasures().get(0).getExpression());
    assertEquals(refs.size(), 1);
    assertEquals(refs.get(0).getSymbolOrdinal(), RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL);
  }

  @Test(dataProvider = "sameSpellingAliasAndPatternQueries")
  public void testSameSpellingInSeparateAliasAndPatternNamespaces(String sql, String expectedSymbol) {
    MatchNode match = planMatch(sql);

    assertEquals(symbolNames(match), List.of(expectedSymbol));
    List<RexExpression.PatternFieldRef> universalRefs =
        collectPatternFieldRefs(match.getMeasures().get(0).getExpression());
    assertEquals(universalRefs.size(), 1);
    assertEquals(universalRefs.get(0).getSymbolOrdinal(), RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL);

    List<RexExpression.PatternFieldRef> patternRefs =
        collectPatternFieldRefs(match.getMeasures().get(1).getExpression());
    assertEquals(patternRefs.size(), 1);
    assertEquals(patternRefs.get(0).getAlpha(), expectedSymbol);
    assertEquals(patternRefs.get(0).getSymbolOrdinal(), 0);
  }

  @DataProvider(name = "sameSpellingAliasAndPatternQueries")
  public Object[][] sameSpellingAliasAndPatternQueries() {
    return new Object[][]{
        new Object[]{
            // Qualified PARTITION BY / ORDER BY references still name the input after its internal re-aliasing.
            "SELECT * FROM a AS A MATCH_RECOGNIZE (PARTITION BY A.col1 ORDER BY A.ts "
                + "MEASURES LAST(col3) AS universal_s, LAST(a.col3) AS pattern_s "
                + "PATTERN (a) DEFINE a AS a.col3 > 0)",
            "A"
        },
        new Object[]{
            // Changing only cosmetic case must not change binding or query validity.
            "SELECT * FROM a AS a MATCH_RECOGNIZE (PARTITION BY a.col1 ORDER BY a.ts "
                + "MEASURES LAST(col3) AS universal_s, LAST(a.col3) AS pattern_s "
                + "PATTERN (a) DEFINE a AS a.col3 > 0)",
            "A"
        },
        new Object[]{
            // The raw names collide inside Calcite even though quoted lowercase "a" is distinct from unquoted A.
            "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY a.col1 ORDER BY a.ts "
                + "MEASURES LAST(col3) AS universal_s, LAST(\"a\".col3) AS pattern_s "
                + "PATTERN (\"a\") DEFINE \"a\" AS \"a\".col3 > 0)",
            "a"
        }
    };
  }

  @Test
  public void testQuantifierBoundAboveTheCapIsRejected() {
    int overTheCap = RelToPlanNodeConverter.MAX_PATTERN_QUANTIFIER_BOUND + 1;
    RuntimeException e = expectThrows(RuntimeException.class, () -> planMatch(
        "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno PATTERN (A{1," + overTheCap
            + "}) DEFINE A AS A.col3 > 0)"));
    assertTrue(e.getMessage().contains("exceeds the maximum supported bound of "
        + RelToPlanNodeConverter.MAX_PATTERN_QUANTIFIER_BOUND), e.getMessage());
  }

  @Test
  public void testQuantifierBoundAtTheCapIsAccepted() {
    int atTheCap = RelToPlanNodeConverter.MAX_PATTERN_QUANTIFIER_BOUND;
    MatchNode match = planMatch(
        "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno PATTERN (A{1," + atTheCap
            + "}) DEFINE A AS A.col3 > 0)");
    assertEquals(((RowPattern.Quantifier) match.getPattern()).getMaxRepeat(), atTheCap);
  }

  /// Covers [PlanNodeToRelConverter#visitMatch]: the anchors move back into `strictStart` / `strictEnd`, the pattern
  /// is rebuilt as Calcite's left deep `RexCall` tree, and `AFTER MATCH SKIP TO LAST` regains its target variable.
  ///
  /// The node is built by hand rather than planned from SQL because `RexExpressionUtils#toRexNode` cannot resolve
  /// comparison operators (their function name is `GREATER_THAN`, which is not a FUNCTION-syntax operator); that is a
  /// pre-existing limitation of this converter, shared with `FilterNode`, and unrelated to MATCH_RECOGNIZE.
  @Test
  public void testMatchNodeConvertsBackToLogicalMatch() {
    DataSchema schema = new DataSchema(new String[]{"col1", "col3"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    PlanNode input = new ExplainedNode(0, schema, null, List.of(), "Input", Map.of());
    // Calcite's Match requires at least one pattern definition, which the grammar guarantees (DEFINE is mandatory).
    List<PatternSymbol> symbols =
        List.of(new PatternSymbol("A", new RexExpression.Literal(ColumnDataType.BOOLEAN, true)),
            new PatternSymbol("B", null));
    RowPattern pattern = new RowPattern.Concat(
        List.of(RowPattern.AnchorStart.INSTANCE, new RowPattern.Symbol(0),
            new RowPattern.Quantifier(new RowPattern.Symbol(1), 2, 3, false), RowPattern.AnchorEnd.INSTANCE));
    List<MatchNode.Measure> measures =
        List.of(new MatchNode.Measure("ev", new RexExpression.PatternFieldRef(1, 1, "B")));
    MatchNode match = new MatchNode(0, schema, PlanNode.NodeHint.EMPTY, List.of(input), symbols, pattern, measures,
        List.of(0), List.of(new RelFieldCollation(1)), MatchNode.AfterMatchSkipMode.TO_LAST, 1,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);

    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder()
        .operatorTable(PinotOperatorTable.instance(true))
        .defaultSchema(CalciteSchema.createRootSchema(false).plus())
        .build());
    Match logicalMatch = findMatch(PlanNodeToRelConverter.convert(relBuilder, match));

    assertNotNull(logicalMatch, "Expected a Match rel");
    assertTrue(logicalMatch.isStrictStart());
    assertTrue(logicalMatch.isStrictEnd());
    assertFalse(logicalMatch.isAllRows());
    // The anchors are gone from the pattern itself, and `{2,3}` is reluctant because 2 != 3.
    assertEquals(logicalMatch.getPattern().toString(), "('A', PATTERN_QUANTIFIER('B', 2, 3, true))");
    assertEquals(logicalMatch.getAfter().toString(), "SKIP TO LAST('B')");
    // Only A has a DEFINE predicate; B matches every row and contributes no pattern definition.
    assertEquals(logicalMatch.getPatternDefinitions().keySet(), Set.of("A"));
    assertEquals(logicalMatch.getMeasures().keySet(), Set.of("ev"));
    assertEquals(logicalMatch.getPartitionKeys().asList(), List.of(0));
    assertEquals(logicalMatch.getOrderKeys().getFieldCollations(), List.of(new RelFieldCollation(1)));
  }

  private static List<String> symbolNames(MatchNode match) {
    return match.getPatternSymbols().stream().map(PatternSymbol::getName).collect(Collectors.toList());
  }

  private static List<RexExpression.PatternFieldRef> collectPatternFieldRefs(@Nullable RexExpression expression) {
    List<RexExpression.PatternFieldRef> refs = new ArrayList<>();
    collectPatternFieldRefs(expression, refs);
    return refs;
  }

  private static void collectPatternFieldRefs(@Nullable RexExpression expression,
      List<RexExpression.PatternFieldRef> refs) {
    if (expression instanceof RexExpression.PatternFieldRef) {
      refs.add((RexExpression.PatternFieldRef) expression);
    } else if (expression instanceof RexExpression.FunctionCall) {
      for (RexExpression operand : ((RexExpression.FunctionCall) expression).getFunctionOperands()) {
        collectPatternFieldRefs(operand, refs);
      }
    }
  }

  @Nullable
  private static Match findMatch(RelNode rel) {
    if (rel instanceof Match) {
      return (Match) rel;
    }
    for (RelNode input : rel.getInputs()) {
      Match match = findMatch(input);
      if (match != null) {
        return match;
      }
    }
    return null;
  }

  @Nullable
  private static PinotLogicalAggregate findLeafAggregate(RelNode rel) {
    if (rel instanceof PinotLogicalAggregate && ((PinotLogicalAggregate) rel).getAggType() == AggType.LEAF) {
      return (PinotLogicalAggregate) rel;
    }
    for (RelNode input : rel.getInputs()) {
      PinotLogicalAggregate aggregate = findLeafAggregate(input);
      if (aggregate != null) {
        return aggregate;
      }
    }
    return null;
  }

  /// Plans {@code sql} through the full query environment and returns the single [MatchNode] in the resulting plan.
  ///
  /// These queries exercise pattern lowering rather than distribution, so most of them omit `PARTITION BY`, which
  /// [org.apache.pinot.calcite.rel.rules.PinotMatchExchangeNodeInsertRule] rejects by default. The query option opts
  /// into the resulting single-worker plan.
  private MatchNode planMatch(String sql) {
    SqlNodeAndOptions sqlNodeAndOptions =
        new SqlNodeAndOptions(CalciteSqlParser.compileToSqlNodeAndOptions(sql).getSqlNode(), PinotSqlType.DQL,
            QueryOptionsUtils.resolveCaseInsensitiveOptions(
                Map.of(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY, "true")));
    DispatchableSubPlan subPlan = _queryEnvironment.compile(sql, sqlNodeAndOptions)
        .planQuery(RANDOM_REQUEST_ID_GEN.nextLong()).getQueryPlan();
    List<MatchNode> found = new ArrayList<>();
    for (DispatchablePlanFragment fragment : subPlan.getQueryStageMap().values()) {
      fragment.getPlanFragment().getFragmentRoot().visit(new PlanNodeVisitor.DepthFirstVisitor<Void, Void>() {
        @Override
        public Void visitMatch(MatchNode node, Void context) {
          found.add(node);
          return super.visitMatch(node, context);
        }

        @Override
        protected boolean traverseStageBoundary() {
          return false;
        }
      }, null);
    }
    assertEquals(found.size(), 1, "Expected exactly one MatchNode in the plan of: " + sql);
    return found.get(0);
  }
}
