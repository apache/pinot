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
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
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
    assertEquals(substitution.getOutcome(), SpoolSubstitution.Outcome.REJECTED_BY_GUARD);
    assertEquals(substitution.getReceiverStageId(), 5);
    assertEquals(substitution.getReplacedStageId(), 6);
    assertEquals(substitution.getLeaderStageId(), 3);
    assertEquals(substitution.getReason(), "In stage 1, JOIN reads the input from stage 2 only after the input from "
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
}
