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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class WindowSortAutoTunerTest {
  @Test
  public void testRequiresTwoStrongObservationsForTheSameExchange() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> stats = stats(1, 400_000, 4, 4, 4, 256, false);

    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, stats));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, stats));
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    WindowSortAutoTuner.Session senderSession = tuner.newSession(17L);
    assertTrue(senderSession.useSenderSort(1, 2, 31, 41));
    senderSession.observe(null);
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));

    assertFalse(tuner.newSession(18L).useSenderSort(1, 2, 31, 41));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 32, 41));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 3, 31, 41));
  }

  @Test
  public void testIncompleteOrChangedEvidenceResetsQualification() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> strong = stats(1, 400_000, 4, 4, 4, 256, false);
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));

    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, stats(1, 400_000, 4, 0, 0, 0, false)));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));

    WindowSortAutoTuner missingStats = new WindowSortAutoTuner();
    assertFalse(observe(missingStats, 17L, 1, 2, 31, 41, strong));
    assertFalse(observe(missingStats, 17L, 1, 2, 31, 41, null));
    assertFalse(observe(missingStats, 17L, 1, 2, 31, 41, strong));
    assertFalse(missingStats.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertTrue(missingStats.newSession(17L).shouldProfile(1, 2, 31, 41));

    WindowSortAutoTuner cold = new WindowSortAutoTuner();
    assertFalse(observe(cold, 17L, 1, 2, 31, 41, stats(1, 399_999, 4, 4, 4, 256, false)));
    assertFalse(observe(cold, 17L, 1, 2, 31, 41, stats(1, 400_000, 3, 3, 3, 256, false)));
    assertFalse(observe(cold, 17L, 1, 2, 31, 41, stats(1, 400_000, 4, 4, 3, 256, false)));
    assertFalse(cold.newSession(17L).useSenderSort(1, 2, 31, 41));
  }

  @Test
  public void testNegativeProfileCooldownAndReprobe() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> presorted = stats(1, 400_000, 4, 4, 0, 256, false);
    List<MultiStageQueryStats.StageStats.Closed> candidate = stats(1, 400_000, 4, 4, 4, 256, false);
    WindowSortAutoTuner.Session first = tuner.newSession(17L);
    assertFalse(first.useSenderSort(1, 2, 31, 41));
    assertTrue(first.shouldProfile(1, 2, 31, 41));
    first.observe(presorted);

    for (int i = 0; i < 7; i++) {
      WindowSortAutoTuner.Session skipped = tuner.newSession(17L);
      assertFalse(skipped.useSenderSort(1, 2, 31, 41));
      assertFalse(skipped.shouldProfile(1, 2, 31, 41));
      skipped.observe(candidate);
    }
    WindowSortAutoTuner.Session probe = tuner.newSession(17L);
    assertFalse(probe.useSenderSort(1, 2, 31, 41));
    assertTrue(probe.shouldProfile(1, 2, 31, 41));
    WindowSortAutoTuner.Session follower = tuner.newSession(17L);
    assertFalse(follower.useSenderSort(1, 2, 31, 41));
    assertFalse(follower.shouldProfile(1, 2, 31, 41));
    follower.observe(candidate);
    probe.observe(candidate);
    WindowSortAutoTuner.Session second = tuner.newSession(17L);
    assertFalse(second.useSenderSort(1, 2, 31, 41));
    assertTrue(second.shouldProfile(1, 2, 31, 41));
    second.observe(candidate);
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));

    WindowSortAutoTuner tiny = new WindowSortAutoTuner();
    assertFalse(observe(tiny, 17L, 1, 2, 31, 41, stats(1, 1_000, 4, 0, 0, 40, false)));
    assertFalse(tiny.newSession(17L).shouldProfile(1, 2, 31, 41));

    WindowSortAutoTuner skewed = new WindowSortAutoTuner();
    assertFalse(observe(skewed, 17L, 1, 2, 31, 41, stats(1, 400_000, 4, 1, 1, 68, false)));
    assertFalse(skewed.newSession(17L).shouldProfile(1, 2, 31, 41));

    WindowSortAutoTuner missingSample = new WindowSortAutoTuner();
    assertFalse(observe(missingSample, 17L, 1, 2, 31, 41, stats(1, 1_000, 4, 0, 0, 0, false)));
    assertFalse(missingSample.newSession(17L).shouldProfile(1, 2, 31, 41));
    assertFalse(observe(missingSample, 18L, 1, 2, 31, 41, stats(1, 400_000, 4, 0, 0, 0, false)));
    assertTrue(missingSample.newSession(18L).shouldProfile(1, 2, 31, 41));
  }

  @Test
  public void testTwoConsecutiveSlowSenderRunsDisableChoice() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> candidate = stats(1, 400_000, 4, 4, 4, 256, false);
    WindowSortAutoTuner.Session first = tuner.newSession(17L);
    assertFalse(first.useSenderSort(1, 2, 31, 41));
    first.observe(candidate, 1_000);
    WindowSortAutoTuner.Session second = tuner.newSession(17L);
    assertFalse(second.useSenderSort(1, 2, 31, 41));
    second.observe(candidate, 1_100);

    WindowSortAutoTuner.Session slow = tuner.newSession(17L);
    assertTrue(slow.useSenderSort(1, 2, 31, 41));
    slow.observe(candidate, 1_151);
    WindowSortAutoTuner.Session recovered = tuner.newSession(17L);
    assertTrue(recovered.useSenderSort(1, 2, 31, 41));
    recovered.observe(candidate, 1_140);
    WindowSortAutoTuner.Session inFlight = tuner.newSession(17L);
    assertTrue(inFlight.useSenderSort(1, 2, 31, 41));
    WindowSortAutoTuner.Session slowAgain = tuner.newSession(17L);
    assertTrue(slowAgain.useSenderSort(1, 2, 31, 41));
    slowAgain.observe(candidate, 1_160);
    WindowSortAutoTuner.Session slowTwice = tuner.newSession(17L);
    assertTrue(slowTwice.useSenderSort(1, 2, 31, 41));
    slowTwice.observe(candidate, 1_170);

    inFlight.observe(candidate, 900);
    WindowSortAutoTuner.Session disabled = tuner.newSession(17L);
    assertFalse(disabled.useSenderSort(1, 2, 31, 41));
    assertFalse(disabled.shouldProfile(1, 2, 31, 41));
    disabled.observe(candidate, 500);
    assertFalse(tuner.newSession(17L).shouldProfile(1, 2, 31, 41));
  }

  @Test
  public void testAmbiguousReceiverStatsDoNotTrainAnyExchange() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> stats = stats(1, 400_000, 4, 4, 4, 256, false);
    for (int i = 0; i < 2; i++) {
      WindowSortAutoTuner.Session session = tuner.newSession(17L);
      assertFalse(session.useSenderSort(1, 2, 31, 41));
      assertFalse(session.useSenderSort(1, 3, 32, 42));
      session.observe(stats);
    }
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 3, 32, 42));

    List<MultiStageQueryStats.StageStats.Closed> twoReceives = stats(1, 400_000, 4, 4, 4, 256, true);
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, twoReceives));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, twoReceives));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertTrue(tuner.newSession(17L).shouldProfile(1, 2, 31, 41));
  }

  @Test
  public void testPeriodicReceiverResampleCanRevokeSenderChoice() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> strong = stats(1, 400_000, 4, 4, 4, 256, false);
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));

    for (int i = 0; i < 7; i++) {
      assertTrue(observe(tuner, 17L, 1, 2, 31, 41, null));
    }
    WindowSortAutoTuner.Session inFlightProbe = tuner.newSession(17L);
    assertFalse(inFlightProbe.useSenderSort(1, 2, 31, 41));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41),
        "An in-flight receiver probe must not leave the sender plan ready");
    inFlightProbe.observe(strong);
    assertTrue(observe(tuner, 17L, 1, 2, 31, 41, null));

    for (int i = 0; i < 6; i++) {
      assertTrue(observe(tuner, 17L, 1, 2, 31, 41, null));
    }
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, stats(1, 400_000, 4, 4, 0, 256, false)));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertFalse(tuner.newSession(17L).shouldProfile(1, 2, 31, 41));
  }

  @Test
  public void testAbandonedReceiverProbeKeepsFallback() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> strong = stats(1, 400_000, 4, 4, 4, 256, false);
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    for (int i = 0; i < 7; i++) {
      assertTrue(observe(tuner, 17L, 1, 2, 31, 41, null));
    }
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41),
        "A later receiver query must not rearm an abandoned probe");
  }

  @Test
  public void testTwoExchangesInOneQueryCanChooseDifferentPlans() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> stats = new ArrayList<>(Collections.nCopies(4, null));
    stats.set(1, stats(1, 400_000, 4, 4, 4, 256, false).get(1));
    stats.set(3, stats(3, 400_000, 4, 4, 0, 256, false).get(3));
    for (int i = 0; i < 2; i++) {
      WindowSortAutoTuner.Session session = tuner.newSession(17L);
      assertFalse(session.useSenderSort(1, 2, 31, 41));
      assertFalse(session.useSenderSort(3, 4, 32, 42));
      session.observe(stats, 1_000);
    }
    WindowSortAutoTuner.Session session = tuner.newSession(17L);
    assertTrue(session.useSenderSort(1, 2, 31, 41));
    assertFalse(session.useSenderSort(3, 4, 32, 42));
    session.observe(stats, 2_000);
    WindowSortAutoTuner.Session next = tuner.newSession(17L);
    assertTrue(next.useSenderSort(1, 2, 31, 41));
    assertFalse(next.useSenderSort(3, 4, 32, 42));
    next.observe(stats, 2_000);
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41),
        "Whole-query latency must not demote one exchange in a plan with multiple AUTO exchanges");
  }

  @Test
  public void testOnlyTheSelectedProbeCanRearmAfterConcurrentReceiverQueries() {
    WindowSortAutoTuner tuner = new WindowSortAutoTuner();
    List<MultiStageQueryStats.StageStats.Closed> strong = stats(1, 400_000, 4, 4, 4, 256, false);
    WindowSortAutoTuner.Session oldCold = tuner.newSession(17L);
    assertFalse(oldCold.useSenderSort(1, 2, 31, 41));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    for (int i = 0; i < 7; i++) {
      assertTrue(observe(tuner, 17L, 1, 2, 31, 41, null));
    }
    WindowSortAutoTuner.Session probe = tuner.newSession(17L);
    assertFalse(probe.useSenderSort(1, 2, 31, 41));
    oldCold.observe(strong);
    assertFalse(observe(tuner, 17L, 1, 2, 31, 41, strong));
    assertFalse(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
    probe.observe(strong);
    assertTrue(tuner.newSession(17L).useSenderSort(1, 2, 31, 41));
  }

  private static boolean observe(WindowSortAutoTuner tuner, long queryHash, int receiverStageId, int senderStageId,
      int inputHash, int collationHash, List<MultiStageQueryStats.StageStats.Closed> stats) {
    WindowSortAutoTuner.Session session = tuner.newSession(queryHash);
    boolean decision = session.useSenderSort(receiverStageId, senderStageId, inputHash, collationHash);
    session.observe(stats);
    return decision;
  }

  private static List<MultiStageQueryStats.StageStats.Closed> stats(int receiverStageId, long rows, int fanIn,
      int sampledStreams, int candidateStreams, long sampledRows, boolean secondReceive) {
    StatMap<BaseMailboxReceiveOperator.StatKey> receive = new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    receive.merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, rows);
    receive.merge(BaseMailboxReceiveOperator.StatKey.FAN_IN, fanIn);
    receive.merge(BaseMailboxReceiveOperator.StatKey.AUTO_SAMPLE_STREAMS, sampledStreams);
    receive.merge(BaseMailboxReceiveOperator.StatKey.AUTO_CANDIDATE_STREAMS, candidateStreams);
    receive.merge(BaseMailboxReceiveOperator.StatKey.AUTO_SAMPLED_ROWS, sampledRows);

    List<OperatorTypeDescriptor> types = new ArrayList<>();
    List<StatMap<?>> maps = new ArrayList<>();
    types.add(MultiStageOperator.Type.MAILBOX_RECEIVE);
    maps.add(receive);
    if (secondReceive) {
      types.add(MultiStageOperator.Type.MAILBOX_RECEIVE);
      maps.add(new StatMap<>(receive));
    }
    List<MultiStageQueryStats.StageStats.Closed> stages =
        new ArrayList<>(Collections.nCopies(receiverStageId + 1, null));
    stages.set(receiverStageId, new MultiStageQueryStats.StageStats.Closed(types, maps));
    return stages;
  }
}
