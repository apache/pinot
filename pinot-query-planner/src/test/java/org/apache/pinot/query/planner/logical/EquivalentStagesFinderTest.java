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
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.WindowNode.WindowExclusion;
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

  @Test
  public void sameUnnestKeepEquivalence() {
    when(
        join(
            exchange(1, unnest(tableScan("T1"), passthrough(List.of(0, 1), true))),
            exchange(2, unnest(tableScan("T1"), passthrough(List.of(0, 1), true)))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  /// The passthrough columns decide which input columns reach the output, so two unnests that only differ there do
  /// not compute the same rows.
  @Test
  public void differentPassthroughInputIndexesBreakEquivalence() {
    when(
        join(
            exchange(1, unnest(tableScan("T1"), passthrough(List.of(0, 1), true))),
            exchange(2, unnest(tableScan("T1"), passthrough(List.of(0), true)))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentPrunedPassthroughBreakEquivalence() {
    when(
        join(
            exchange(1, unnest(tableScan("T1"), passthrough(List.of(0, 1), true))),
            exchange(2, unnest(tableScan("T1"), passthrough(List.of(0, 1), false)))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
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

  /// `rand() < 0.1` as a filter condition. `rand()` without arguments is volatile.
  private static RexExpression randomSample() {
    RexExpression.FunctionCall rand =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, "rand", List.of());
    return new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(),
        List.of(rand, new RexExpression.Literal(DataSchema.ColumnDataType.DOUBLE, 0.1)));
  }

  /// Two equal stages that call a volatile function come from two subqueries that must draw their own values, so they
  /// are not equivalent. The stages above them are not equivalent either.
  @Test
  public void stagesThatCallVolatileFunctionsAreNotEquivalent() {
    when(
        join(
            exchange(1, aggregate(exchange(2, filter(tableScan("T1"), randomSample())), List.of())),
            exchange(3, aggregate(exchange(4, filter(tableScan("T1"), randomSample())), List.of()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2], [3], [4]]");
  }

  /// `rand(seed)` is deterministic, so the same equal stages are equivalent.
  @Test
  public void stagesThatCallSeededRandAreEquivalent() {
    RexExpression.FunctionCall seededRand = new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, "rand",
        List.of(new RexExpression.Literal(DataSchema.ColumnDataType.LONG, 42L)));
    RexExpression threshold = new RexExpression.Literal(DataSchema.ColumnDataType.DOUBLE, 0.1);
    RexExpression condition = new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN,
        SqlKind.LESS_THAN.name(), List.of(seededRand, threshold));
    when(
        join(
            exchange(1, aggregate(exchange(2, filter(tableScan("T1"), condition)), List.of())),
            exchange(3, aggregate(exchange(4, filter(tableScan("T1"), condition)), List.of()))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 3], [2, 4]]");
  }

  /// A volatile call nested inside another expression also makes the stage volatile.
  @Test
  public void nestedVolatileCallsAreFound() {
    RexExpression.FunctionCall rand =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, "rand", List.of());
    RexExpression nested = new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.AND.name(),
        List.of(new RexExpression.Literal(DataSchema.ColumnDataType.BOOLEAN, 1),
            new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.GREATER_THAN.name(),
                List.of(new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, "plus",
                    List.of(rand, new RexExpression.Literal(DataSchema.ColumnDataType.DOUBLE, 1.0))),
                    new RexExpression.Literal(DataSchema.ColumnDataType.DOUBLE, 1.5)))));
    when(
        join(
            exchange(1, filter(tableScan("T1"), nested)),
            exchange(2, filter(tableScan("T1"), nested))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  /// Two scans of the same table that project different columns compute the same rows. With column mapping they are
  /// one group: the leader adds the columns that the other stage needs.
  @Test
  public void differentProjectionsAreEquivalentWithColumnMapping() {
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0)),
            exchange(2, project(tableScan("T1", "INT:a", "STRING:c"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0))
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1, 2]]");
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), false).toString(), "[[0], [1], [2]]");
  }

  /// The planner scans the columns that each consumer needs, in its own order. Columns are compared by name, so the
  /// order does not matter.
  @Test
  public void columnOrderDoesNotBreakEquivalence() {
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1)),
            exchange(2, project(tableScan("T1", "LONG:b", "INT:a"), 1, 0))
        )
    );
    // Both stages output (a, b), so they do not even need a column mask.
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), false).toString(), "[[0], [1, 2]]");
  }

  /// The distribution keys are compared by column, not by ordinal.
  @Test
  public void keysAreComparedByColumn() {
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(1)),
            exchange(2, project(tableScan("T1", "LONG:b", "STRING:c"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0))
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1, 2]]");
  }

  @Test
  public void differentKeyColumnsBreakEquivalence() {
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0)),
            exchange(2, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(1))
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1], [2]]");
  }

  /// A filter is compared by the columns it reads, so the same condition at different ordinals is equivalent.
  @Test
  public void filtersAreComparedByColumn() {
    RexExpression bIsPositive = greaterThanZero(1);
    RexExpression bIsPositiveAt0 = greaterThanZero(0);
    when(
        join(
            exchange(1, project(filter(tableScan("T1", "INT:a", "LONG:b"), bIsPositive), 0)),
            exchange(2, project(filter(tableScan("T1", "LONG:b", "INT:a"), bIsPositiveAt0), 1))
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), false).toString(), "[[0], [1, 2]]");
  }

  @Test
  public void filtersOnDifferentColumnsBreakEquivalence() {
    when(
        join(
            exchange(1, project(filter(tableScan("T1", "INT:a", "LONG:b"), greaterThanZero(1)), 0)),
            exchange(2, project(filter(tableScan("T1", "INT:a", "LONG:b"), greaterThanZero(0)), 0))
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1], [2]]");
  }

  /// A stage that reads a subset of the columns of the leader only needs a column mask, so the leader does not need
  /// a project to add columns.
  @Test
  public void aSubsetOfTheColumnsNeedsNoWidening() {
    when(
        join(
            exchange(1, tableScan("T1", "INT:a", "LONG:b"))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED),
            exchange(2, tableScan("T1", "LONG:b"))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED)
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1, 2]]");
  }

  /// The leader can only add columns when it is a table scan under projects and filters, with a project on top.
  @Test
  public void extraColumnsNeedALeaderThatCanAddThem() {
    when(
        join(
            exchange(1, tableScan("T1", "INT:a"))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED),
            exchange(2, tableScan("T1", "INT:a", "LONG:b"))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED)
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1], [2]]");
  }

  /// Receiver specs do not support every distribution, so stages with different columns and an unsupported
  /// distribution are not grouped.
  @Test
  public void differentColumnsNeedADistributionThatSpecsSupport() {
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.SINGLETON),
            exchange(2, project(tableScan("T1", "INT:a", "STRING:c"), 0, 1))
                .withDistributionType(RelDistribution.Type.SINGLETON)
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1], [2]]");
  }

  /// A stage above two equivalent stages with different columns reads the columns of the group by identity.
  @Test
  public void stagesAboveMappedStagesAreEquivalent() {
    when(
        join(
            exchange(1, project(exchange(2, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED), 0)),
            exchange(3, project(exchange(4, project(tableScan("T1", "INT:a", "STRING:c"), 0, 1))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED), 0))
        )
    );
    // Stages 1 and 3 both read column a of the group [2, 4].
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1, 3], [2, 4]]");
  }

  /// A stage that only reads deterministic columns computes the same rows as a volatile stage over the same scan, but
  /// it must not read them from a stage that draws random values.
  @Test
  public void volatileStagesAreNotReferences() {
    RexExpression rand = new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, "rand", List.of());
    when(
        join(
            exchange(1, project(tableScan("T1", "INT:a"), List.of(new RexExpression.InputRef(0), rand),
                DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED),
            exchange(2, project(tableScan("T1", "INT:a"), List.of(new RexExpression.InputRef(0)),
                DataSchema.ColumnDataType.INT))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED)
        )
    );
    assertEquals(EquivalentStagesFinder.findEquivalentStages(stage(0), true).toString(), "[[0], [1], [2]]");
  }

  private static RexExpression greaterThanZero(int column) {
    return new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.GREATER_THAN.name(),
        List.of(new RexExpression.InputRef(column), new RexExpression.Literal(DataSchema.ColumnDataType.LONG, 0L)));
  }
}
