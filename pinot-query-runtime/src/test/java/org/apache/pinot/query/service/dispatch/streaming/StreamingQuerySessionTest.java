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
package org.apache.pinot.query.service.dispatch.streaming;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link StreamingQuerySession}. Covers the broker-side accumulator, early completion (returns true
 * as soon as all expected opchains have reported, without waiting for the timeout), timeout fall-through, and
 * fan-out cancel behaviour.
 */
public class StreamingQuerySessionTest {

  /**
   * Early completion: when all expected opchains report before the wait window, awaitCompletion returns true
   * immediately rather than burning the full timeout.
   */
  @Test
  public void testEarlyCompletion()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 3);
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    session.recordOpChainComplete(buildOpChainComplete(0, 1, 1, 10));
    session.recordOpChainComplete(buildOpChainComplete(0, 2, 1, 15));

    long start = System.nanoTime();
    boolean done = session.awaitCompletion(10, TimeUnit.SECONDS);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    Assert.assertTrue(done, "expected early completion");
    Assert.assertTrue(elapsedMs < 1000, "expected immediate return, took " + elapsedMs + "ms");
    Assert.assertEquals(session.getOutstandingCount(), 0L);
  }

  /**
   * Timeout fall-through: awaitCompletion returns false when the timeout fires before all opchains have reported,
   * and the per-stage coverage shows the missing opchains.
   */
  @Test
  public void testTimeoutFallThrough()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 3);
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    // Only 1 of 3 reports.
    boolean done = session.awaitCompletion(50, TimeUnit.MILLISECONDS);
    Assert.assertFalse(done, "expected timeout, not early completion");
    Assert.assertEquals(session.getOutstandingCount(), 2L);

    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().getOrDefault(0, 0), 1);
    Assert.assertEquals((int) coverage.getMergeFailedByStage().getOrDefault(0, 0), 0);
  }

  /**
   * Cross-worker stats sum: two opchains for the same stage merge into one accumulator entry by tree-shape match.
   */
  @Test
  public void testStatsAccumulationAcrossWorkers()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    session.recordOpChainComplete(buildOpChainComplete(0, 1, 1, 7));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS));
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().get(0), 2);
    @SuppressWarnings("unchecked")
    StatMap<AggregateOperator.StatKey> merged =
        (StatMap<AggregateOperator.StatKey>) coverage.getStageAccumulator().get(0).getStatMap();
    Assert.assertEquals(merged.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 12);
  }

  /**
   * Regression for the snapshot data race: {@link StreamingQuerySession#snapshotCoverage()} must hand back a snapshot
   * isolated from the live accumulator, and a late {@link StreamingQuerySession#recordOpChainComplete} arriving after
   * the broker stopped waiting must NOT mutate it. Before the fix, snapshotCoverage shallow-copied the map, so the
   * returned {@code StageStatsTreeNode}/{@code StatMap} were aliased to the live ones; a post-snapshot merge (which
   * happens on the success-partial path, where open streams are not cancelled) mutated the very {@code StatMap} the
   * broker was concurrently flattening/serializing — a real data race. This asserts the observable consequence
   * deterministically: the snapshot value stays put and the late report is ignored.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshotIsolatedFromLateReports()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);
    // Worker 0 reports stage 0 with 5 rows. The broker then stops waiting (e.g. drain window expired) and snapshots.
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    StreamingQuerySession.Coverage snapshot = session.snapshotCoverage();
    StatMap<AggregateOperator.StatKey> snapshotStat =
        (StatMap<AggregateOperator.StatKey>) snapshot.getStageAccumulator().get(0).getStatMap();
    Assert.assertEquals(snapshotStat.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 5);

    // A late worker reports another 7 rows for the same stage AFTER the snapshot was taken. With the old shallow copy
    // + no finalize guard this merged into the snapshot's StatMap (5 -> 12).
    session.recordOpChainComplete(buildOpChainComplete(0, 1, 1, 7));

    // The already-handed-out snapshot must be untouched...
    Assert.assertEquals(snapshotStat.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 5,
        "snapshot StatMap must not be mutated by a post-snapshot report");
    // ...and the late report must have been dropped entirely (session finalized), so a fresh snapshot also shows 5
    // and the responded counter did not advance.
    StreamingQuerySession.Coverage after = session.snapshotCoverage();
    StatMap<AggregateOperator.StatKey> afterStat =
        (StatMap<AggregateOperator.StatKey>) after.getStageAccumulator().get(0).getStatMap();
    Assert.assertEquals(afterStat.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 5,
        "late report after finalize must be ignored");
    Assert.assertEquals((int) after.getRespondedByStage().getOrDefault(0, 0), 1,
        "responded count must not advance after finalize");
  }

  /**
   * A failed opchain (success=false) records as merge-failed-or-responded depending on whether stats are present,
   * and triggers fan-out cancel exactly once across remaining streams.
   */
  @Test
  public void testFanOutCancelOnPeerError()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 3);
    AtomicInteger cancelCalls = new AtomicInteger();
    StreamingServerHandle a = req -> cancelCalls.incrementAndGet();
    StreamingServerHandle b = req -> cancelCalls.incrementAndGet();
    StreamingServerHandle c = req -> cancelCalls.incrementAndGet();
    session.registerStream(a);
    session.registerStream(b);
    session.registerStream(c);

    // Server B reports an error; should fan-out cancel to A and C (and B itself, since fan-out walks all open
    // streams — they're best-effort anyway).
    session.recordOpChainComplete(buildErrorOpChainComplete(0, 1, "boom"));
    Assert.assertEquals(cancelCalls.get(), 3);

    // A second error does not re-fire fan-out (idempotent).
    session.recordOpChainComplete(buildErrorOpChainComplete(0, 2, "boom2"));
    Assert.assertEquals(cancelCalls.get(), 3);
  }

  /**
   * Stream onError (transport failure) drains the latch and triggers fan-out cancel; the dispatcher's
   * remainingExpected variant lets it account for opchains that the dead server still owed.
   */
  @Test
  public void testStreamErrorDrainsLatchAndCancels()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 4);
    AtomicInteger cancelCalls = new AtomicInteger();
    StreamingServerHandle dead = req -> cancelCalls.incrementAndGet();
    StreamingServerHandle other = req -> cancelCalls.incrementAndGet();
    session.registerStream(dead);
    session.registerStream(other);

    // 'dead' owed 3 opchains.
    session.recordStreamError(dead, new RuntimeException("transport"), 3);
    // 'other' delivered its 1 opchain.
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 1));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS), "expected completion after dead drained");
    Assert.assertEquals(cancelCalls.get(), 1, "fan-out cancel should hit only 'other' (dead removed first)");
  }

  /**
   * Concurrent opchain reports across many threads: latch drains correctly with no lost updates.
   */
  @Test
  public void testConcurrentReports()
      throws Exception {
    int n = 50;
    StreamingQuerySession session = new StreamingQuerySession(1L, n);
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      int workerId = i;
      Thread t = new Thread(() -> {
        try {
          start.await();
        } catch (InterruptedException ignored) {
        }
        try {
          session.recordOpChainComplete(buildOpChainComplete(0, workerId, 1, 1));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      t.start();
      threads.add(t);
    }
    start.countDown();
    Assert.assertTrue(session.awaitCompletion(5, TimeUnit.SECONDS));
    for (Thread t : threads) {
      t.join();
    }
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().get(0), n);
    @SuppressWarnings("unchecked")
    StatMap<AggregateOperator.StatKey> merged =
        (StatMap<AggregateOperator.StatKey>) coverage.getStageAccumulator().get(0).getStatMap();
    Assert.assertEquals(merged.getLong(AggregateOperator.StatKey.EMITTED_ROWS), n);
  }

  /**
   * An opchain whose stats tree contains an operator type id absent from {@link
   * org.apache.pinot.query.runtime.operator.OperatorTypeRegistry} (simulating a newer server carrying a plugin
   * the broker has not installed) must not abort the query. The session marks the stage merge-failed and drains
   * the completion latch so the query result is returned normally.
   */
  @Test
  public void testDecodeFailedUnknownTypeIdDoesNotAbortQuery()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);

    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    // Type id 9999 is not in the registry — decode throws DecodeFailedException.
    session.recordOpChainComplete(buildOpChainCompleteWithTypeId(0, 1, 9999, ByteString.EMPTY));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "query should complete despite unknown operator type id");
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().getOrDefault(0, 0), 1,
        "only the successfully decoded opchain should be counted as responded");
    Assert.assertEquals((int) coverage.getMergeFailedByStage().getOrDefault(0, 0), 1,
        "the unknown-type opchain should be counted as merge-failed");
    Assert.assertNotNull(coverage.getStageAccumulator().get(0),
        "first worker's valid stats should remain in the accumulator");
  }

  /**
   * When two workers report the same stage with incompatible tree shapes (different operator types), the second
   * merge throws {@link StageStatsTreeNode.ShapeMismatchException}. Because {@code StageStatsTreeNode.merge}
   * mutates the existing node before recursing into children, the partially-merged node is discarded from the
   * accumulator (it may be corrupt), the stage is poisoned, and the query still completes.
   *
   * <p>A shape mismatch means the workers of the stage disagree on the tree shape (typically version skew), so no
   * merged result for the stage can be trusted: the first worker's report — whose data was dropped along with the
   * tree — is reclassified from {@code responded} to {@code mergeFailed}, and any later report for the stage also
   * counts as {@code mergeFailed} instead of re-seeding the accumulator. {@code responded + mergeFailed} still
   * reconciles with the expected opchain count, and {@code responded} never overstates what the response contains.
   */
  @Test
  public void testShapeMismatchDoesNotAbortQuery()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);

    // Worker 0 reports AGGREGATE at stage 0; populates the accumulator.
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));

    // Worker 1 reports HASH_JOIN at stage 0 with an empty-but-valid stat map.
    // The type id mismatch triggers ShapeMismatchException inside merge().
    session.recordOpChainComplete(
        buildOpChainCompleteWithTypeId(0, 1, MultiStageOperator.Type.HASH_JOIN.getId(), emptyStatBytes()));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "query should complete despite shape mismatch");
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    // The mismatch drops the stage tree, so worker 0's cleanly-merged data is gone too: both workers count as
    // merge-failed and none as responded. responded + mergeFailed = 2 = expected (reconciles).
    Assert.assertEquals((int) coverage.getRespondedByStage().getOrDefault(0, 0), 0,
        "no worker counts as responded once the stage tree is dropped");
    Assert.assertEquals((int) coverage.getMergeFailedByStage().getOrDefault(0, 0), 2,
        "both the pre-mismatch and the mismatching worker should be counted as merge-failed");
    Assert.assertNull(coverage.getStageAccumulator().get(0),
        "the partially-merged stage entry should be discarded from the accumulator after the failed merge");
  }

  /**
   * After a shape mismatch poisons a stage, a later report that decodes cleanly must NOT re-seed the accumulator:
   * its data alone would misrepresent the stage (the pre-mismatch merges were already dropped), so it is counted as
   * merge-failed and the stage stays absent from the accumulator.
   */
  @Test
  public void testPoisonedStageIgnoresLaterReports()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 3);

    // Worker 0 merges cleanly; worker 1 mismatches (HASH_JOIN vs AGGREGATE) and poisons the stage.
    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    session.recordOpChainComplete(
        buildOpChainCompleteWithTypeId(0, 1, MultiStageOperator.Type.HASH_JOIN.getId(), emptyStatBytes()));
    // Worker 2 decodes cleanly, but the stage is poisoned.
    session.recordOpChainComplete(buildOpChainComplete(0, 2, 1, 15));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS));
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().getOrDefault(0, 0), 0,
        "no worker counts as responded for a poisoned stage");
    Assert.assertEquals((int) coverage.getMergeFailedByStage().getOrDefault(0, 0), 3,
        "all reports for a poisoned stage count as merge-failed");
    Assert.assertNull(coverage.getStageAccumulator().get(0),
        "a poisoned stage must not be re-seeded by a later clean report");
  }

  /**
   * When a stat-map payload is unreadable (truncated bytes — not enough data for even the count header), the
   * decoder wraps the resulting {@link java.io.EOFException} as a
   * {@link org.apache.pinot.query.runtime.plan.MultiStageStatsTreeDecoder.DecodeFailedException}. The session
   * absorbs the failure, marks the stage merge-failed, and drains the latch. The query result is unaffected.
   */
  @Test
  public void testCorruptedStatBytesDoesNotAbortQuery()
      throws Exception {
    StreamingQuerySession session = new StreamingQuerySession(1L, 2);

    session.recordOpChainComplete(buildOpChainComplete(0, 0, 1, 5));
    // ByteString.EMPTY has 0 bytes; StatMap.deserialize() tries to readByte() for the count and throws
    // EOFException, which the decoder re-throws as DecodeFailedException.
    session.recordOpChainComplete(
        buildOpChainCompleteWithTypeId(0, 1, MultiStageOperator.Type.AGGREGATE.getId(), ByteString.EMPTY));

    Assert.assertTrue(session.awaitCompletion(1, TimeUnit.SECONDS),
        "query should complete despite corrupted stat bytes");
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Assert.assertEquals((int) coverage.getRespondedByStage().getOrDefault(0, 0), 1,
        "only the successfully decoded opchain should be counted as responded");
    Assert.assertEquals((int) coverage.getMergeFailedByStage().getOrDefault(0, 0), 1,
        "the opchain with corrupted stat bytes should be counted as merge-failed");
    Assert.assertNotNull(coverage.getStageAccumulator().get(0),
        "first worker's valid stats should remain in the accumulator");
  }

  // ---- helpers ----

  private static Worker.OpChainComplete buildOpChainComplete(int stageId, int workerId, int planNodeId, long emitted)
      throws IOException {
    StatMap<AggregateOperator.StatKey> stat = new StatMap<>(AggregateOperator.StatKey.class)
        .merge(AggregateOperator.StatKey.EMITTED_ROWS, emitted);
    Worker.StageStatsNode rootNode = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(MultiStageOperator.Type.AGGREGATE.getId())
        .addPlanNodeIds(planNodeId)
        .setStatMap(serialize(stat))
        .build();
    return Worker.OpChainComplete.newBuilder()
        .setStageId(stageId)
        .setWorkerId(workerId)
        .setSuccess(true)
        .setStats(Worker.MultiStageStatsTree.newBuilder()
            .setCurrentStageId(stageId)
            .setCurrentStage(rootNode)
            .build())
        .build();
  }

  private static Worker.OpChainComplete buildErrorOpChainComplete(int stageId, int workerId, String errorMsg) {
    return Worker.OpChainComplete.newBuilder()
        .setStageId(stageId)
        .setWorkerId(workerId)
        .setSuccess(false)
        .setErrorMsg(errorMsg)
        .build();
  }

  private static Worker.OpChainComplete buildOpChainCompleteWithTypeId(
      int stageId, int workerId, int typeId, ByteString statBytes) {
    Worker.StageStatsNode rootNode = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(typeId)
        .setStatMap(statBytes)
        .build();
    return Worker.OpChainComplete.newBuilder()
        .setStageId(stageId)
        .setWorkerId(workerId)
        .setSuccess(true)
        .setStats(Worker.MultiStageStatsTree.newBuilder()
            .setCurrentStageId(stageId)
            .setCurrentStage(rootNode)
            .build())
        .build();
  }

  /** Serialised empty {@link StatMap} — one zero byte representing zero entries. */
  private static ByteString emptyStatBytes()
      throws IOException {
    return serialize(new StatMap<>(AggregateOperator.StatKey.class));
  }

  private static ByteString serialize(StatMap<?> statMap)
      throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos)) {
      statMap.serialize(output);
      output.flush();
      return ByteString.copyFrom(baos.toByteArray());
    }
  }
}
