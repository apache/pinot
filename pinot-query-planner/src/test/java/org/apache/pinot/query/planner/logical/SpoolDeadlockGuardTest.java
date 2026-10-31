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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class SpoolDeadlockGuardTest extends StagesTestBase {

  @Test
  public void treeHasNoRisk() {
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3, tableScan("T1"))
            )
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  /// The spool feeds the probe input of stage 1 through stage 2, and the build input through stage 5.
  @Test
  public void spoolOnBothSidesOfAJoinThroughStreamingStages() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, join(spool.newReceiver(), exchange(4, tableScan("T2")))),
                exchange(5, join(spool.newReceiver(), exchange(6, tableScan("T3"))))
            )
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "JOIN", 2, 5, 3);
  }

  @Test
  public void spoolFeedsProbeDirectly() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, spool.newReceiver()),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "JOIN", 2, 4, 3);
  }

  /// The aggregate in stage 2 reads the whole spool before stage 1 starts to wait for the probe input.
  @Test
  public void aggregateOnProbePathReadsTheWholeSpool() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, aggregate(spool.newReceiver(), List.of())),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  @Test
  public void windowOnProbePathReadsTheWholeSpool() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, window(spool.newReceiver(), lastValueRespectingNulls())),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  @Test
  public void sortOnProbePathReadsTheWholeSpool() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, sort(spool.newReceiver())),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  /// A sort without collations is a streaming limit, so it does not protect the spool.
  @Test
  public void limitOnProbePathStreams() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, limit(spool.newReceiver(), 10)),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "JOIN", 2, 4, 3);
  }

  /// Stage 2 reads the spool as the build input of its own join, so the spool is read in full before stage 2 emits.
  @Test
  public void buildInputOnProbePathReadsTheWholeSpool() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, join(exchange(5, tableScan("T2")), spool.newReceiver())),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  @Test
  public void unionReadsItsInputsInOrder() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            setOp(SetOpNode.SetOpType.UNION, List.of(
                exchange(2, spool.newReceiver()),
                exchange(4, spool.newReceiver())
            ))
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "UNION", 4, 2, 3);
  }

  @Test
  public void unionWithAggregateOnLaterInput() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            setOp(SetOpNode.SetOpType.UNION, List.of(
                exchange(2, spool.newReceiver()),
                exchange(4, aggregate(spool.newReceiver(), List.of()))
            ))
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  @Test
  public void intersectReadsRightInputFirst() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            setOp(SetOpNode.SetOpType.INTERSECT, List.of(
                exchange(2, spool.newReceiver()),
                exchange(4, spool.newReceiver())
            ))
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "INTERSECT", 2, 4, 3);
  }

  /// The join alone is safe here, because the aggregate in stage 2 reads the whole spool. The pipeline breaker is not:
  /// stage 1 reads stage 4 only after the pipeline breaker reads stage 2 in full, and both need the spool.
  @Test
  public void pipelineBreakerDefersTheMainReceives() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, aggregate(spool.newReceiver(), List.of())).asPipelineBreaker(),
                exchange(4, spool.newReceiver())
            )
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "the pipeline breaker", 4, 2, 3);
  }

  /// Both receives belong to the pipeline breaker, which reads them in round-robin with blocking reads. The join alone
  /// is safe, because the aggregate in stage 2 reads the whole spool. But a blocking read on stage 2 keeps stage 4
  /// unread, and stage 4 stops the spool that stage 2 needs.
  @Test
  public void pipelineBreakerReceivesDeferEachOther() {
    SpoolBuilder spool = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode root = when(
        exchange(1,
            join(
                exchange(2, aggregate(spool.newReceiver(), List.of())).asPipelineBreaker(),
                exchange(4, spool.newReceiver()).asPipelineBreaker()
            )
        )
    );
    assertRisk(SpoolDeadlockGuard.findRisk(root), 1, "the pipeline breaker", 4, 2, 3);
  }

  /// Two joins read the same two spools with the sides swapped. Each join alone is safe: its probe spool is not
  /// needed by its own build input. Together they block: stage 2 waits for spool 4 to finish its build input while
  /// spool 4 stops because stage 6 does not read its probe input, and stage 6 waits for spool 3 in the same way.
  @Test
  public void joinsWithSwappedSidesBlockEachOther() {
    SpoolBuilder spoolA = new SpoolBuilder(3, tableScan("A"));
    SpoolBuilder spoolB = new SpoolBuilder(4, tableScan("B"));
    MailboxSendNode root = when(
        exchange(1,
            setOp(SetOpNode.SetOpType.UNION, List.of(
                exchange(2, aggregate(join(spoolA.newReceiver(), spoolB.newReceiver()), List.of())),
                exchange(6, aggregate(join(spoolB.newReceiver(), spoolA.newReceiver()), List.of()))
            ))
        )
    );
    SpoolDeadlockGuard.DeadlockRisk risk = SpoolDeadlockGuard.findRisk(root);
    assertNotNull(risk, "Expected a circular wait");
    List<SpoolDeadlockGuard.Wait> cycle = risk.cycle();
    assertEquals(cycle.size(), 2, risk.toString());
    for (SpoolDeadlockGuard.Wait wait : cycle) {
      assertEquals(wait.operator(), "JOIN", risk.toString());
      // Each wait is kept by the spool it reads as build input, which the other wait stops.
      assertEquals(wait.sharedStageId(), wait.blockingSenderStageId(), risk.toString());
    }
    assertEquals(Set.of(cycle.get(0).stageId(), cycle.get(1).stageId()), Set.of(2, 6), risk.toString());
    assertEquals(risk.toString(), "Circular wait: "
        + "In stage " + cycle.get(0).stageId() + ", JOIN reads the input from stage "
        + cycle.get(0).waitingSenderStageId() + " only after the input from stage "
        + cycle.get(0).blockingSenderStageId() + " finishes, and stage " + cycle.get(0).blockingSenderStageId()
        + " depends on stage " + cycle.get(0).sharedStageId() + ", which the next wait stops; "
        + "In stage " + cycle.get(1).stageId() + ", JOIN reads the input from stage "
        + cycle.get(1).waitingSenderStageId() + " only after the input from stage "
        + cycle.get(1).blockingSenderStageId() + " finishes, and stage " + cycle.get(1).blockingSenderStageId()
        + " depends on stage " + cycle.get(1).sharedStageId() + ", which the next wait stops");
  }

  /// Same shape as [#joinsWithSwappedSidesBlockEachOther()], but both joins read spool 3 as the build input. Spool 3
  /// is drained by both builds, so no wait stops it, and there is no cycle.
  @Test
  public void joinsWithTheSameSidesDoNotBlock() {
    SpoolBuilder spoolA = new SpoolBuilder(3, tableScan("A"));
    SpoolBuilder spoolB = new SpoolBuilder(4, tableScan("B"));
    MailboxSendNode root = when(
        exchange(1,
            setOp(SetOpNode.SetOpType.UNION, List.of(
                exchange(2, aggregate(join(spoolB.newReceiver(), spoolA.newReceiver()), List.of())),
                exchange(6, aggregate(join(spoolB.newReceiver(), spoolA.newReceiver()), List.of(List.of(0))))
            ))
        )
    );
    assertNull(SpoolDeadlockGuard.findRisk(root));
  }

  private static void assertRisk(@Nullable SpoolDeadlockGuard.DeadlockRisk risk, int stageId, String operator,
      int waitingSenderStageId, int blockingSenderStageId, int sharedStageId) {
    assertNotNull(risk, "Expected a deadlock risk");
    assertEquals(risk.cycle().size(), 1, risk.toString());
    SpoolDeadlockGuard.Wait wait = risk.cycle().get(0);
    assertEquals(wait.stageId(), stageId, risk.toString());
    assertEquals(wait.operator(), operator, risk.toString());
    assertEquals(wait.waitingSenderStageId(), waitingSenderStageId, risk.toString());
    assertEquals(wait.blockingSenderStageId(), blockingSenderStageId, risk.toString());
    assertEquals(wait.sharedStageId(), sharedStageId, risk.toString());
  }
}
