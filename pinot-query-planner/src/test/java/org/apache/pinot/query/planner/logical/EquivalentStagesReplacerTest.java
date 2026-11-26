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
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.ReceiverSpec;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EquivalentStagesReplacerTest extends StagesTestBase {

  @Test
  public void test() {
    when(// stage 0
        exchange(1,
          join(
              exchange(2,
                join(
                  exchange(3, tableScan("T1")),
                  exchange(4, tableScan("T2"))
                )
              ),
              exchange(5,
                join(
                  exchange(6, tableScan("T1")),
                  exchange(7, tableScan("T3"))
                )
              )
          )
        )
    );

    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2], [3, 6], [4], [5], [7]]");

    MailboxSendNode rootStage = stage(0);
    EquivalentStagesReplacer.replaceEquivalentStages(rootStage, groupedStages);

    cleanup();
    SpoolBuilder readT1 = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2,
                    join(
                        readT1.newReceiver(),
                        exchange(4, tableScan("T2"))
                    )
                ),
                exchange(5,
                    join(
                        readT1.newReceiver(),
                        exchange(7, tableScan("T3"))
                    )
                )
            )
        )
    );

    assertEqualPlan(rootStage, expected);
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

    MailboxSendNode rootStage = stage(0);
    List<SpoolSubstitution> substitutions = EquivalentStagesReplacer.replaceEquivalentStages(rootStage,
        groupedStages, EquivalentStagesReplacer.OnSubstitution.NO_OP, false);
    assertEquals(substitutions.toString(), "[stage 1: 3 -> 2 REJECTED_SAME_RECEIVER_STAGE]");

    cleanup();
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3, tableScan("T1"))
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }

  /// Same plan as [#test()]. Stage 1 reads stage 2 (the probe input) only after stage 5 finishes, and both depend on
  /// the scan of T1, so the guard refuses to share it.
  @Test
  void guardRejectsSpoolThatCanBlock() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2,
                    join(
                        exchange(3, tableScan("T1")),
                        exchange(4, tableScan("T2"))
                    )
                ),
                exchange(5,
                    join(
                        exchange(6, tableScan("T1")),
                        exchange(7, tableScan("T3"))
                    )
                )
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    MailboxSendNode rootStage = stage(0);
    List<SpoolSubstitution> substitutions = EquivalentStagesReplacer.replaceEquivalentStages(rootStage,
        groupedStages, EquivalentStagesReplacer.OnSubstitution.NO_OP, true);
    assertEquals(substitutions.size(), 1);
    SpoolSubstitution substitution = substitutions.get(0);
    assertEquals(substitution.outcome(), SpoolSubstitution.Outcome.REJECTED_BY_GUARD);
    assertEquals(substitution.receiverStageId(), 5);
    assertEquals(substitution.replacedStageId(), 6);
    assertEquals(substitution.leaderStageId(), 3);
    assertEquals(substitution.reason(), "In stage 1, JOIN reads the input from stage 2 only after the input from "
        + "stage 5 finishes, and both depend on stage 3");

    cleanup();
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2,
                    join(
                        exchange(3, tableScan("T1")),
                        exchange(4, tableScan("T2"))
                    )
                ),
                exchange(5,
                    join(
                        exchange(6, tableScan("T1")),
                        exchange(7, tableScan("T3"))
                    )
                )
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }

  /// Each consumer of the scan aggregates it, so the aggregates read the whole scan before the join of stage 1 waits.
  @Test
  void guardAcceptsSpoolWhenAggregatesReadItFirst() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2, aggregate(exchange(3, tableScan("T1")), List.of())),
                exchange(4, aggregate(exchange(5, tableScan("T1")), List.of(List.of(0))))
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2], [3, 5], [4]]");

    MailboxSendNode rootStage = stage(0);
    List<SpoolSubstitution> substitutions = EquivalentStagesReplacer.replaceEquivalentStages(rootStage,
        groupedStages, EquivalentStagesReplacer.OnSubstitution.NO_OP, true);
    assertEquals(substitutions.toString(), "[stage 4: 5 -> 3 APPLIED]");

    cleanup();
    SpoolBuilder readT1 = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2, aggregate(readT1.newReceiver(), List.of())),
                exchange(4, aggregate(readT1.newReceiver(), List.of(List.of(0))))
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }

  @Test
  void groupSendingToTheSameStage() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3,
                    join(
                        exchange(4, tableScan("T1")),
                        exchange(5, tableScan("T1"))
                    )
                )
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 4, 5], [3]]");

    MailboxSendNode rootStage = stage(0);
    EquivalentStagesReplacer.replaceEquivalentStages(rootStage, groupedStages);

    cleanup();
    SpoolBuilder readT1 = new SpoolBuilder(2, tableScan("T1"));
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                readT1.newReceiver(),
                exchange(3,
                    join(
                        readT1.newReceiver(),
                        exchange(5, tableScan("T1"))
                    )
                )
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }

  /// Two leaf stages that scan the same table for different columns become one spool. The leader scans and projects
  /// the union of the columns, and each receiver gets a column mask with the columns it read before.
  @Test
  public void leaderAddsTheColumnsOfTheOtherStages() {
    when(// stage 0
        join(
            exchange(1, project(exchange(2, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0)), 1)),
            exchange(3, project(exchange(4, project(tableScan("T1", "INT:a", "STRING:c"), 0, 1))
                .withDistributionType(RelDistribution.Type.HASH_DISTRIBUTED).withKeys(List.of(0)), 1))
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0), true);
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 4], [3]]");

    MailboxSendNode leader = stage(2);
    List<SpoolSubstitution> substitutions = EquivalentStagesReplacer.replaceEquivalentStages(stage(0),
        groupedStages, EquivalentStagesReplacer.OnSubstitution.NO_OP, false);
    assertEquals(substitutions.size(), 1);
    assertEquals(substitutions.get(0).outcome(), SpoolSubstitution.Outcome.APPLIED);

    // The leader scans and outputs a, b and c.
    assertEquals(leader.getDataSchema(), new DataSchema(new String[]{"a", "b", "c"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.STRING}));
    ProjectNode project = (ProjectNode) leader.getInputs().get(0);
    assertEquals(project.getProjects(), List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1),
        new RexExpression.InputRef(2)));
    TableScanNode scan = (TableScanNode) project.getInputs().get(0);
    assertEquals(scan.getColumns(), List.of("a", "b", "c"));

    // Stage 1 reads (a, b) and stage 3 reads (a, c), both hashed by a.
    assertEquals(leader.getReceiverSpec(1),
        new ReceiverSpec(1, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), List.of(0, 1)));
    assertEquals(leader.getReceiverSpec(3),
        new ReceiverSpec(3, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), List.of(0, 2)));
    for (int receiverStageId : List.of(1, 3)) {
      MailboxReceiveNode receive = (MailboxReceiveNode) stage(receiverStageId).getInputs().get(0).getInputs().get(0);
      assertEquals(leader.getReceiverSpec(receiverStageId).receiverSchema(leader.getDataSchema()),
          receive.getDataSchema());
    }
  }

  /// Stages that output the same columns need no spec, even when their consumers read different columns.
  @Test
  public void identicalStagesNeedNoSpec() {
    when(
        join(
            exchange(1, project(exchange(2, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1)), 0)),
            exchange(3, project(exchange(4, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1)), 1))
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0), true);
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 4], [3]]");
    EquivalentStagesReplacer.replaceEquivalentStages(stage(0), groupedStages);
    assertTrue(stage(2).getReceiverSpecs().isEmpty());
    assertTrue(stage(2).hasUniformReceivers());
  }

  /// The leader passes the new table columns through every project of its chain, and computes a new expression in
  /// its top project.
  @Test
  public void leaderComputesNewExpressionsThroughItsChain() {
    RexExpression bIsPositive = new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "GREATER_THAN",
        List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.LONG, 0L)));
    RexExpression cPlusOne = new RexExpression.FunctionCall(DataSchema.ColumnDataType.INT, "plus",
        List.of(new RexExpression.InputRef(2), new RexExpression.Literal(DataSchema.ColumnDataType.INT, 1)));
    when(// stage 0
        join(
            exchange(1, project(exchange(2,
                project(filter(project(tableScan("T1", "INT:a", "LONG:b"), 0, 1), bIsPositive), 0))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED), 0)),
            exchange(3, project(exchange(4,
                project(filter(project(tableScan("T1", "INT:a", "LONG:b", "INT:c"), 0, 1, 2), bIsPositive),
                    List.of(new RexExpression.InputRef(0), cPlusOne), DataSchema.ColumnDataType.INT,
                    DataSchema.ColumnDataType.INT))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED), 1))
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0), true);
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 4], [3]]");
    EquivalentStagesReplacer.replaceEquivalentStages(stage(0), groupedStages,
        EquivalentStagesReplacer.OnSubstitution.NO_OP, false);

    MailboxSendNode leader = stage(2);
    ProjectNode top = (ProjectNode) leader.getInputs().get(0);
    FilterNode filter = (FilterNode) top.getInputs().get(0);
    ProjectNode bottom = (ProjectNode) filter.getInputs().get(0);
    TableScanNode scan = (TableScanNode) bottom.getInputs().get(0);
    assertEquals(scan.getColumns(), List.of("a", "b", "c"));
    // The bottom project passes c through, and the filter still reads b at ordinal 1.
    assertEquals(bottom.getProjects(), List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1),
        new RexExpression.InputRef(2)));
    assertEquals(filter.getCondition(), bIsPositive);
    // The top project keeps a and adds c + 1, which reads c at ordinal 2 of its input.
    assertEquals(top.getProjects(), List.of(new RexExpression.InputRef(0), cPlusOne));
    assertEquals(leader.getDataSchema().size(), 2);

    assertEquals(leader.getReceiverSpec(1),
        new ReceiverSpec(1, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), List.of(0)));
    assertEquals(leader.getReceiverSpec(3).columnMask(), null, "Stage 3 reads every column of the leader");
  }

  /// A member whose substitution is rejected keeps its own stage, so the leader does not add its columns.
  @Test
  public void rejectedMembersDoNotAddColumns() {
    when(// stage 0
        join(
            exchange(1, join(
                exchange(2, project(tableScan("T1", "INT:a", "LONG:b"), 0, 1))
                    .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED),
                exchange(3, project(tableScan("T1", "INT:a", "STRING:c"), 0, 1))
                    .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED))),
            exchange(4, project(exchange(5, project(tableScan("T1", "INT:a", "DOUBLE:d"), 0, 1))
                .withDistributionType(RelDistribution.Type.BROADCAST_DISTRIBUTED), 1))
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0), true);
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 3, 5], [4]]");
    List<SpoolSubstitution> substitutions = EquivalentStagesReplacer.replaceEquivalentStages(stage(0),
        groupedStages, EquivalentStagesReplacer.OnSubstitution.NO_OP, false);

    // Stage 1 already reads the leader, so stage 3 keeps its own stage. Stage 4 reads the leader.
    assertEquals(substitutions.stream().map(SpoolSubstitution::outcome).toList(),
        List.of(SpoolSubstitution.Outcome.REJECTED_SAME_RECEIVER_STAGE, SpoolSubstitution.Outcome.APPLIED));
    MailboxSendNode leader = stage(2);
    assertEquals(List.of(leader.getDataSchema().getColumnNames()), List.of("a", "b", "d"));
    assertEquals(leader.getReceiverSpec(1).columnMask(), List.of(0, 1));
    assertEquals(leader.getReceiverSpec(4).columnMask(), List.of(0, 2));
    assertEquals(List.of(stage(3).getDataSchema().getColumnNames()), List.of("a", "c"));
  }
}
