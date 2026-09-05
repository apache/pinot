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

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.planner.plannode.WindowNode.WindowExclusion;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EquivalentStagesFinderTest extends StagesTestBase {

  private final DataSchema _dataSchema1 = new DataSchema(
      new String[]{"col1"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
  );
  private final DataSchema _dataSchema2 = new DataSchema(
      new String[]{"col2"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
  );
  private final DataSchema _matchDataSchema = new DataSchema(
      new String[]{"col1", "value"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT}
  );

  @Test
  public void justScan() {
    MailboxSendNode root = when(tableScan("T1"));
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(root);
    assertEquals(result.toString(), "[[0]]");
  }

  @Test
  public void independentJoin() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T2"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sharedJoin() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T1"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  @Test
  void sameDistributionKeepEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1"))
                .withDistributionType(RelDistribution.Type.RANDOM_DISTRIBUTED),
            exchange(2, tableScan("T1"))
                .withDistributionType(RelDistribution.Type.RANDOM_DISTRIBUTED)
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1, 2]]");
  }

  @Test
  void differentDistributionBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1"))
                .withDistributionType(RelDistribution.Type.RANDOM_DISTRIBUTED),
            exchange(2, tableScan("T1"))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED)
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sameHintsDontBreakEquivalence() {
    when(
        join(
            exchange(
                1,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            ),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  @Test
  public void differentHintsBreakEquivalence() {
    when(
        join(
            exchange(
                1,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            ),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value2"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentHintsOneNullBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value2"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sameWindowKeepEquivalence() {
    when(
        join(
            exchange(1, window(tableScan("T1"), lastValueRespectingNulls())),
            exchange(2, window(tableScan("T1"), lastValueRespectingNulls()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  /// Two windows that both ignore nulls are still equivalent: the check must compare the flag, not reject it.
  @Test
  public void sameIgnoreNullsKeepEquivalence() {
    when(
        join(
            exchange(1, window(tableScan("T1"), lastValueIgnoringNulls())),
            exchange(2, window(tableScan("T1"), lastValueIgnoringNulls()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  /// Same for a non-default frame exclusion.
  @Test
  public void sameWindowExclusionKeepEquivalence() {
    when(
        join(
            exchange(1, window(tableScan("T1"), lastValueRespectingNulls(), WindowExclusion.CURRENT_ROW)),
            exchange(2, window(tableScan("T1"), lastValueRespectingNulls(), WindowExclusion.CURRENT_ROW))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  /// A window that ignores nulls computes different values than one that respects them, so the two stages must not be
  /// treated as equivalent.
  @Test
  public void differentIgnoreNullsBreakEquivalence() {
    when(
        join(
            exchange(1, window(tableScan("T1"), lastValueIgnoringNulls())),
            exchange(2, window(tableScan("T1"), lastValueRespectingNulls()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  /// The frame exclusion changes which rows feed the window function, so it must break equivalence too.
  @Test
  public void differentWindowExclusionBreakEquivalence() {
    when(
        join(
            exchange(1, window(tableScan("T1"), lastValueRespectingNulls(), WindowExclusion.CURRENT_ROW)),
            exchange(2, window(tableScan("T1"), lastValueRespectingNulls(), WindowExclusion.NO_OTHERS))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sameMatchKeepsEquivalence() {
    when(
        join(
            exchange(1, match(tableScan("T1"), "baseline")),
            exchange(2, match(tableScan("T1"), "baseline"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  @Test(dataProvider = "nonEquivalentMatchProperties")
  public void differentMatchPropertiesBreakEquivalence(String variation) {
    when(
        join(
            exchange(1, match(tableScan("T1"), "baseline")),
            exchange(2, match(tableScan("T1"), variation))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]",
        "MATCH_RECOGNIZE stages that differ in " + variation + " must not be spooled together");
  }

  @Test
  public void differentMatchInputsBreakEquivalence() {
    when(
        join(
            exchange(1, match(tableScan("T1"), "baseline")),
            exchange(2, match(tableScan("T2"), "baseline"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void matchAndDifferentNodeTypeBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1").withDataSchema(_matchDataSchema)),
            exchange(2, match(tableScan("T1"), "baseline"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
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
        {"rows per match"}
    };
  }

  /// The group keys only hold the union of the grouping columns, so `ROLLUP(col1, col2)` and `CUBE(col1, col2)` agree
  /// on every other field (including the data schema) and are only told apart by the grouping sets themselves.
  @Test
  public void differentGroupingSetsBreakEquivalence() {
    when(
        join(
            exchange(1, aggregate(tableScan("T1"), List.of(List.of(0, 1), List.of(0), List.of()))),
            exchange(2, aggregate(tableScan("T1"), List.of(List.of(0, 1), List.of(0), List.of(1), List.of())))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sameGroupingSetsKeepEquivalence() {
    List<List<Integer>> rollup = List.of(List.of(0, 1), List.of(0), List.of());
    when(
        join(
            exchange(1, aggregate(tableScan("T1"), rollup)),
            exchange(2, aggregate(tableScan("T1"), rollup))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  /// ASOF joins carry their comparison in the match condition rather than in the non-equi conditions, so two joins
  /// that only differ there must not be spooled together.
  @Test
  public void differentAsofMatchConditionBreakEquivalence() {
    when(
        join(
            exchange(1, asofJoin(tableScan("T1"), tableScan("T2"), greaterThan())),
            exchange(2, asofJoin(tableScan("T1"), tableScan("T2"), greaterThanOrEqual()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sameAsofMatchConditionKeepEquivalence() {
    when(
        join(
            exchange(1, asofJoin(tableScan("T1"), tableScan("T2"), greaterThan())),
            exchange(2, asofJoin(tableScan("T1"), tableScan("T2"), greaterThan()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  private static RexExpression greaterThan() {
    return comparison(SqlKind.GREATER_THAN);
  }

  private static RexExpression greaterThanOrEqual() {
    return comparison(SqlKind.GREATER_THAN_OR_EQUAL);
  }

  private static RexExpression comparison(SqlKind kind) {
    return new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, kind.name(),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
  }

  private SimpleChildBuilder<MatchNode> match(SimpleChildBuilder<? extends PlanNode> childBuilder,
      String variation) {
    return (stageId, mySchema, myHints) -> {
      PlanNode input = childBuilder.build(stageId);
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
          patternSymbols = List.of(new PatternSymbol("A", RexExpression.Literal.FALSE),
              new PatternSymbol("B", null));
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
        default:
          throw new IllegalArgumentException("Unknown variation: " + variation);
      }

      DataSchema dataSchema = mySchema != null ? mySchema : _matchDataSchema;
      return new MatchNode(stageId, dataSchema, myHints, List.of(input), patternSymbols, pattern, measures,
          partitionKeys, collations, skipMode, skipTarget, rowsPerMatchMode);
    };
  }

  @Test
  public void differentDataSchemaBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1").withDataSchema(_dataSchema1)),
            exchange(
                2,
                tableScan("T1")
                    .withDataSchema(_dataSchema2)
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentDataSchemaOneNullBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(
                2,
                tableScan("T1")
                    .withDataSchema(_dataSchema2)
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void deepShared() {
    when(
        join(
            exchange(1,
                join(
                    exchange(3, tableScan("T1")),
                    exchange(4, tableScan("T1"))
                )
            ),
            exchange(2,
                join(
                    exchange(5, tableScan("T1")),
                    exchange(6, tableScan("T1"))
                )
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2], [3, 4, 5, 6]]");
  }

  @Test
  public void deepSharedDifferentTables() {
    when(
        join(
            exchange(1,
                join(
                    exchange(3, tableScan("T1")),
                    exchange(4, tableScan("T2"))
                )
            ),
            exchange(2,
                join(
                    exchange(5, tableScan("T1")),
                    exchange(6, tableScan("T2"))
                )
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2], [3, 5], [4, 6]]");
  }

  @Test
  void notUniqueReceiversInStage() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3, tableScan("T1"))
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 3]]");
  }
}
