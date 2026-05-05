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
