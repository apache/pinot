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
package org.apache.pinot.core.query.scheduler;

import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pinot.core.query.scheduler.TestHelper.createQueryRequest;
import static org.testng.Assert.*;


public class SecondaryWorkloadQueueTest {
  private static final ServerMetrics METRICS = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private static final String TABLE = "secondaryTable";
  private static final byte[] RESPONSE = new byte[]{1, 2, 3};

  private static final long AWAIT_MS = 10_000L;
  private static final long NEVER_AWAIT_MS = 500L;
  private static final long TIMEOUT_MS = 60_000L;

  /// Expires queries after 1s, so a query with an older arrival time is dropped by the TTL sweep.
  private static final Map<String, Object> SHORT_DEADLINE =
      Map.of(SecondaryWorkloadQueue.SECONDARY_QUEUE_QUERY_TIMEOUT, 1);
  /// A wakeup interval far larger than AWAIT_MS, so only an explicit signal can wake a blocked reader in time.
  private static final Map<String, Object> NO_POLLING = Map.of(SecondaryWorkloadQueue.QUEUE_WAKEUP_MS, 60_000);

  private final List<ResourceManager> _resourceManagers = new ArrayList<>();
  private ExecutorService _executor;

  @BeforeMethod
  public void beforeMethod() {
    _executor = Executors.newCachedThreadPool();
  }

  @AfterMethod
  public void afterMethod() {
    _executor.shutdownNow();
    _resourceManagers.forEach(ResourceManager::stop);
    _resourceManagers.clear();
  }

  // verify that queries are returned in the order they were added,
  // and that the expiry handler is not called for live queries.
  @Test
  public void testTakeReturnsQueriesInArrivalOrder()
      throws Exception {
    List<SchedulerQueryContext> expiredQueries = new CopyOnWriteArrayList<>();
    SecondaryWorkloadQueue queue = createQueue(Map.of(), expiredQueries::add);

    SchedulerQueryContext first = liveQuery();
    SchedulerQueryContext second = liveQuery();
    queue.put(first);
    queue.put(second);

    assertSame(queue.take(), first);
    assertSame(queue.take(), second);
    assertTrue(expiredQueries.isEmpty());
  }

  // verify that drain() returns all pending queries in arrival order,
  // and that the queue is empty afterwards.
  @Test
  public void testDrainRemovesAllPendingQueries()
      throws Exception {
    SecondaryWorkloadQueue queue = createQueue(Map.of(), ignored -> { });

    SchedulerQueryContext first = liveQuery();
    SchedulerQueryContext second = liveQuery();
    queue.put(first);
    queue.put(second);

    assertEquals(queue.drain(), List.of(first, second));
    assertTrue(queue.drain().isEmpty());
  }

  /// A query dropped for exceeding the queue deadline must be handed to the expiry handler and have its future
  /// completed, otherwise the request never gets a response. Expiring the head must also not stall a live query
  /// queued behind it.
  @Test
  public void testExpiredQueryIsCompletedAndDoesNotBlockLiveQuery()
      throws Exception {
    List<SchedulerQueryContext> expiredQueries = new CopyOnWriteArrayList<>();
    SecondaryWorkloadQueue queue = createQueue(SHORT_DEADLINE, queryContext -> {
      expiredQueries.add(queryContext);
      queryContext.setResultFuture(Futures.immediateFuture(RESPONSE));
    });

    SchedulerQueryContext expired = expiredQuery();
    SchedulerQueryContext live = liveQuery();
    queue.put(expired);
    queue.put(live);

    assertSame(queue.take(), live);
    assertEquals(expiredQueries, List.of(expired));
    assertSame(expired.getResultFuture().get(AWAIT_MS, MILLISECONDS), RESPONSE);
  }

  /// Expiry is not a dispatch event, so a reader with nothing but expired queries keeps waiting. This is the only
  /// case that exercises the sweep running with no query to hand back.
  @Test(timeOut = TIMEOUT_MS)
  public void testExpiredQueryDoesNotWakeReader()
      throws Exception {
    CountDownLatch handled = new CountDownLatch(1);
    SecondaryWorkloadQueue queue = createQueue(SHORT_DEADLINE, queryContext -> handled.countDown());

    queue.put(expiredQuery());
    Future<SchedulerQueryContext> reader = _executor.submit(queue::take);

    assertTrue(handled.await(AWAIT_MS, MILLISECONDS), "Expired query was never handed to the handler");
    assertThrows(TimeoutException.class, () -> reader.get(NEVER_AWAIT_MS, MILLISECONDS));

    // The reader is still usable once a live query arrives.
    SchedulerQueryContext live = liveQuery();
    queue.put(live);
    assertSame(reader.get(AWAIT_MS, MILLISECONDS), live);
  }

  /// A failing expiry handler must not break take(): the live query is still returned and the expired query is
  /// still completed so its request gets a response.
  @Test
  public void testExpiryHandlerFailureStillCompletesExpiredQuery()
      throws Exception {
    SecondaryWorkloadQueue queue = createQueue(SHORT_DEADLINE, queryContext -> {
      throw new IllegalStateException("error");
    });

    SchedulerQueryContext expired = expiredQuery();
    SchedulerQueryContext live = liveQuery();
    queue.put(expired);
    queue.put(live);

    assertSame(queue.take(), live);
    ExecutionException e =
        expectThrows(ExecutionException.class, () -> expired.getResultFuture().get(AWAIT_MS, MILLISECONDS));
    assertTrue(e.getCause() instanceof IllegalStateException);
  }

  /// The handler writes to the request channel, so it must not run while the queue lock is held, otherwise Netty
  /// threads calling put() would block behind it.
  @Test(timeOut = TIMEOUT_MS)
  public void testExpiryHandlerRunsWithoutHoldingQueueLock()
      throws Exception {
    CountDownLatch handlerEntered = new CountDownLatch(1);
    CountDownLatch releaseHandler = new CountDownLatch(1);
    SecondaryWorkloadQueue queue = createQueue(SHORT_DEADLINE, queryContext -> {
      handlerEntered.countDown();
      await(releaseHandler);
    });

    queue.put(expiredQuery());
    Future<SchedulerQueryContext> reader = _executor.submit(queue::take);
    assertTrue(handlerEntered.await(AWAIT_MS, MILLISECONDS));

    // The handler is still running. This blocks forever if take() holds the lock across the callback.
    SchedulerQueryContext live = liveQuery();
    queue.put(live);

    releaseHandler.countDown();
    assertSame(reader.get(AWAIT_MS, MILLISECONDS), live);
  }

  /// With an empty queue the reader waits untimed, so put() is the only thing that can wake it.
  @Test(timeOut = TIMEOUT_MS)
  public void testTakeBlocksUntilPutSignals()
      throws Exception {
    SecondaryWorkloadQueue queue = createQueue(NO_POLLING, ignored -> { });

    Future<SchedulerQueryContext> reader = _executor.submit(queue::take);
    assertThrows(TimeoutException.class, () -> reader.get(NEVER_AWAIT_MS, MILLISECONDS));

    SchedulerQueryContext live = liveQuery();
    queue.put(live);
    assertSame(reader.get(AWAIT_MS, MILLISECONDS), live, "put() did not wake the reader");
  }

  /// A query that cannot be scheduled because the group is at its thread limit must be dispatched as soon as threads
  /// are released, without waiting for the TTL sweep.
  @Test(timeOut = TIMEOUT_MS)
  public void testTakeWakesOnSignalWorkersReleased()
      throws Exception {
    PinotConfiguration config = new PinotConfiguration(NO_POLLING);
    TestResourceManager resourceManager = newResourceManager(config);
    resourceManager._canSchedule = false;
    SecondaryWorkloadQueue queue = new SecondaryWorkloadQueue(config, resourceManager, ignored -> { });

    SchedulerQueryContext live = liveQuery();
    queue.put(live);
    Future<SchedulerQueryContext> reader = _executor.submit(queue::take);
    assertThrows(TimeoutException.class, () -> reader.get(NEVER_AWAIT_MS, MILLISECONDS));

    resourceManager._canSchedule = true;
    queue.signalWorkersReleased();
    assertSame(reader.get(AWAIT_MS, MILLISECONDS), live, "signalWorkersReleased() did not wake the reader");
  }

  /// Admission control ANDs the two conditions, so the pending limit alone never rejects a query.
  @Test
  public void testOutOfCapacityRequiresPendingAndThreadLimit()
      throws Exception {
    PinotConfiguration config = new PinotConfiguration(Map.of(SecondaryWorkloadQueue.MAX_PENDING_SECONDARY_QUERIES, 2));

    TestResourceManager belowThreadLimit = newResourceManager(config);
    belowThreadLimit._tableThreadsHardLimit = Integer.MAX_VALUE;
    SecondaryWorkloadQueue lenientQueue = new SecondaryWorkloadQueue(config, belowThreadLimit, ignored -> { });
    for (int i = 0; i < 5; i++) {
      lenientQueue.put(liveQuery());
    }

    TestResourceManager atThreadLimit = newResourceManager(config);
    atThreadLimit._tableThreadsHardLimit = 0;
    SecondaryWorkloadQueue strictQueue = new SecondaryWorkloadQueue(config, atThreadLimit, ignored -> { });
    strictQueue.put(liveQuery());
    strictQueue.put(liveQuery());
    assertThrows(OutOfCapacityException.class, () -> strictQueue.put(liveQuery()));
  }

  private static SchedulerQueryContext liveQuery() {
    return createQueryRequest(TABLE, METRICS);
  }

  /// Older than the SHORT_DEADLINE queue deadline, so the TTL sweep drops it.
  private static SchedulerQueryContext expiredQuery() {
    return createQueryRequest(TABLE, METRICS, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10));
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private SecondaryWorkloadQueue createQueue(Map<String, Object> properties,
      Consumer<SchedulerQueryContext> expiredQueryHandler) {
    PinotConfiguration config = new PinotConfiguration(properties);
    return new SecondaryWorkloadQueue(config, newResourceManager(config), expiredQueryHandler);
  }

  private TestResourceManager newResourceManager(PinotConfiguration config) {
    TestResourceManager resourceManager = new TestResourceManager(config);
    _resourceManagers.add(resourceManager);
    return resourceManager;
  }

  private static class TestResourceManager extends UnboundedResourceManager {
    volatile boolean _canSchedule = true;
    volatile int _tableThreadsHardLimit = Integer.MAX_VALUE;

    TestResourceManager(PinotConfiguration config) {
      super(config);
    }

    @Override
    public boolean canSchedule(SchedulerGroupAccountant accountant) {
      return _canSchedule;
    }

    @Override
    public int getTableThreadsHardLimit() {
      return _tableThreadsHardLimit;
    }
  }
}
