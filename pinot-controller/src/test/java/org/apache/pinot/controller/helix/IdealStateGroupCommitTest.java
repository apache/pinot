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
package org.apache.pinot.controller.helix;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.IdealStateGroupCommit;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class IdealStateGroupCommitTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommitTest.class);
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME_PREFIX = "potato_";

  private static final int SYSTEM_MULTIPLIER = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  private static final int NUM_PROCESSORS = 5 * SYSTEM_MULTIPLIER;
  private static final int NUM_UPDATES = 100 * SYSTEM_MULTIPLIER;
  private static final int NUM_TABLES = 20;

  private ExecutorService _executorService;

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.info("Starting IdealStateGroupCommitTest with SYSTEM_MULTIPLIER: {}", SYSTEM_MULTIPLIER);
    TEST_INSTANCE.setupSharedStateAndValidate();
    _executorService = Executors.newFixedThreadPool(4);
  }

  @BeforeMethod
  public void beforeTest() {
    for (int i = 0; i < NUM_UPDATES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      IdealState idealState = new IdealState(tableName);
      idealState.setStateModelDefRef("OnlineOffline");
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      idealState.setReplicas("1");
      idealState.setNumPartitions(0);
      TEST_INSTANCE.getHelixAdmin().addResource(TEST_INSTANCE.getHelixClusterName(), tableName, idealState);
      ControllerMetrics.get().removeTableMeter(tableName, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS);
    }
  }

  @AfterMethod
  public void afterTest() {
    for (int i = 0; i < NUM_UPDATES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdown();
    TEST_INSTANCE.cleanup();
  }

  /**
   * Regression test for the per-entry isolation fix in {@link IdealStateGroupCommit#commit}.
   *
   * Setup mirrors the pre-fix race condition that produced "in IdealState, no ZK metadata"
   * orphans in production:
   *
   *   1. The queue already contains an entry whose updater will throw PermanentUpdaterException
   *      (in production this is a pauseless segment whose 300s max-completion-time deadline has
   *      passed while waiting behind a bulk IdealState update such as a retention deletion).
   *   2. A fresh thread B calls commit() with a healthy updater. B's entry is enqueued AFTER the
   *      stuck entry (FIFO).
   *   3. B becomes the leader and iterates the queue in FIFO order.
   *
   * Pre-fix behavior (the bug): the stuck entry's updater threw, iteration stopped, B's commit()
   * exited with that exception, and B's still-queued entry was applied by a subsequent leader.
   * B's caller observed "my update failed" and ran cleanup (removeSegmentZKMetadataBestEffort)
   * on its new segment metadata even though B's IdealState update later succeeded -- producing
   * an "in IdealState, no ZK metadata" orphan.
   *
   * Post-fix behavior (asserted here): the stuck entry's updater throws and is isolated to that
   * entry's owner only. The leader continues iterating, applies B's updater, and CAS-writes the
   * result. B's commit() returns successfully with the updated IdealState. Co-batched entries
   * are no longer collateral damage.
   */
  @Test
  public void testFreshUpdaterAppliedAfterCallerThrows()
      throws Exception {
    String tableName = TABLE_NAME_PREFIX + "race_OFFLINE";
    IdealState initialState = new IdealState(tableName);
    initialState.setStateModelDefRef("OnlineOffline");
    initialState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    initialState.setReplicas("1");
    initialState.setNumPartitions(0);
    TEST_INSTANCE.getHelixAdmin().addResource(TEST_INSTANCE.getHelixClusterName(), tableName, initialState);

    try {
      IdealStateGroupCommit commit = new IdealStateGroupCommit();

      // Step 1: Pre-populate the queue with a stuck entry whose updater throws
      // PermanentUpdaterException. Simulates an in-flight queued entry for a pauseless segment
      // whose 300s deadline has expired.
      injectStuckEntry(commit, tableName);

      // Step 2: Fresh thread submits a healthy updater that adds a partition.
      Function<IdealState, IdealState> freshUpdater = is -> {
        is.setPartitionState("freshPartition", "instance1", "ONLINE");
        return is;
      };

      Throwable freshException = null;
      try {
        commit.commit(TEST_INSTANCE.getHelixManager(), tableName, freshUpdater,
            RetryPolicies.noDelayRetryPolicy(1), false);
      } catch (Throwable e) {
        freshException = e;
      }

      LOGGER.info("Fresh thread's commit() returned with exception: {}",
          freshException == null ? "(none -- fix is in place)"
              : freshException.getClass().getSimpleName() + ": " + freshException.getMessage());

      // Claim #1 (post-fix): the fresh thread's commit() does NOT throw. The stuck entry's
      // PermanentUpdaterException is isolated to that entry only.
      Assert.assertNull(freshException,
          "Fresh thread's commit() should not throw -- the co-batched stuck entry's exception "
              + "must be isolated to its own entry. If this assertion fails, the per-entry "
              + "isolation fix in IdealStateGroupCommit.commit() has regressed and the orphan "
              + "race observed in the 2026-05-20 15:21 UTC incident can reoccur.");

      // Step 3: Read the IdealState back from Helix and verify the fresh updater landed in a
      // single CAS (no drainer needed -- the stuck entry was consumed inside the fresh thread's
      // own batch).
      IdealState finalState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      Map<String, String> freshMap = finalState.getInstanceStateMap("freshPartition");

      LOGGER.info("Final IdealState partitions: {}", finalState.getPartitionSet());
      LOGGER.info("freshPartition state map:    {}", freshMap);

      // Claim #2: freshUpdater's change is in IdealState (the surviving updater was applied
      // and CAS-written in the same batch as the failed stuck entry).
      Assert.assertNotNull(freshMap,
          "Fresh updater's change should be present in IdealState after the per-entry isolation "
              + "fix applies the surviving entries in the same batched CAS.");
      Assert.assertEquals(freshMap.get("instance1"), "ONLINE");

      LOGGER.info("=== FIX VERIFIED ===");
      LOGGER.info("Fresh thread's commit() returned normally; freshPartition -> {}", freshMap);
    } finally {
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  /**
   * Multi-thread isolation test: N stuck owners + M fresh owners on the same resource. Each
   * stuck owner's commit() throws (their own PermanentUpdaterException); every fresh owner's
   * commit() returns successfully; the final IdealState contains every fresh owner's partition.
   * No reflection -- every stuck entry has a real owner thread observing the result.
   */
  @Test
  public void testMultipleStuckAndFreshIsolation()
      throws Exception {
    String tableName = TABLE_NAME_PREFIX + "multi_OFFLINE";
    IdealState initialState = new IdealState(tableName);
    initialState.setStateModelDefRef("OnlineOffline");
    initialState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    initialState.setReplicas("1");
    initialState.setNumPartitions(0);
    TEST_INSTANCE.getHelixAdmin().addResource(TEST_INSTANCE.getHelixClusterName(), tableName, initialState);

    int stuckCount = 3;
    int freshCount = 5;
    IdealStateGroupCommit commit = new IdealStateGroupCommit();
    ExecutorService pool = Executors.newFixedThreadPool(stuckCount + freshCount);

    try {
      CountDownLatch allReady = new CountDownLatch(stuckCount + freshCount);
      CountDownLatch goSignal = new CountDownLatch(1);

      List<AtomicReference<Throwable>> stuckResults = new ArrayList<>();
      for (int i = 0; i < stuckCount; i++) {
        AtomicReference<Throwable> result = new AtomicReference<>();
        stuckResults.add(result);
        final int idx = i;
        pool.submit(() -> {
          allReady.countDown();
          try {
            goSignal.await();
          } catch (InterruptedException ignored) {
          }
          Function<IdealState, IdealState> stuckUpdater = is -> {
            throw new HelixHelper.PermanentUpdaterException("stuck-" + idx);
          };
          try {
            commit.commit(TEST_INSTANCE.getHelixManager(), tableName, stuckUpdater,
                RetryPolicies.noDelayRetryPolicy(1), false);
          } catch (Throwable t) {
            result.set(t);
          }
        });
      }

      List<AtomicReference<Throwable>> freshResults = new ArrayList<>();
      for (int i = 0; i < freshCount; i++) {
        AtomicReference<Throwable> result = new AtomicReference<>();
        freshResults.add(result);
        final int idx = i;
        pool.submit(() -> {
          allReady.countDown();
          try {
            goSignal.await();
          } catch (InterruptedException ignored) {
          }
          Function<IdealState, IdealState> freshUpdater = is -> {
            is.setPartitionState("freshPart-" + idx, "instance1", "ONLINE");
            return is;
          };
          try {
            commit.commit(TEST_INSTANCE.getHelixManager(), tableName, freshUpdater,
                RetryPolicies.noDelayRetryPolicy(1), false);
          } catch (Throwable t) {
            result.set(t);
          }
        });
      }

      Assert.assertTrue(allReady.await(10, TimeUnit.SECONDS), "Worker threads failed to start");
      goSignal.countDown();
      pool.shutdown();
      Assert.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "Workers did not finish");

      // Every stuck owner observes its OWN PermanentUpdaterException (wrapped in RuntimeException
      // by IdealStateGroupCommit.commit at lines 184-187).
      for (int i = 0; i < stuckCount; i++) {
        Throwable t = stuckResults.get(i).get();
        Assert.assertNotNull(t, "Stuck owner " + i + " should have thrown");
        Throwable cause = t.getCause() != null ? t.getCause() : t;
        Assert.assertTrue(cause instanceof HelixHelper.PermanentUpdaterException,
            "Stuck owner " + i + " expected PermanentUpdaterException, got " + cause);
        Assert.assertEquals(cause.getMessage(), "stuck-" + i,
            "Stuck owner " + i + " should see ITS OWN exception, not a sibling's. "
                + "Per-entry isolation failed: got [" + cause.getMessage() + "].");
      }

      // Every fresh owner returns normally.
      for (int i = 0; i < freshCount; i++) {
        Throwable t = freshResults.get(i).get();
        Assert.assertNull(t,
            "Fresh owner " + i + " should not throw -- co-batched stuck entries must not leak. "
                + "Got: " + (t == null ? "null" : t.toString()));
      }

      // IdealState reflects all M fresh partitions.
      IdealState finalState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      for (int i = 0; i < freshCount; i++) {
        Map<String, String> map = finalState.getInstanceStateMap("freshPart-" + i);
        Assert.assertNotNull(map, "freshPart-" + i + " missing from IdealState");
        Assert.assertEquals(map.get("instance1"), "ONLINE",
            "freshPart-" + i + " should be ONLINE in IdealState");
      }

      LOGGER.info("=== Isolation verified: {} stuck owners threw, {} fresh owners succeeded ===",
          stuckCount, freshCount);
    } finally {
      if (!pool.isShutdown()) {
        pool.shutdownNow();
      }
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  /**
   * Uses reflection to push a stuck Entry (whose updater throws PermanentUpdaterException) into
   * IdealStateGroupCommit's internal per-resource queue, without going through commit(). This is
   * how we make the FIFO race deterministic in a unit test.
   */
  @SuppressWarnings("unchecked")
  private static void injectStuckEntry(IdealStateGroupCommit commit, String resourceName)
      throws Exception {
    Class<?> queueClass =
        Class.forName("org.apache.pinot.common.utils.helix.IdealStateGroupCommit$Queue");
    Class<?> entryClass =
        Class.forName("org.apache.pinot.common.utils.helix.IdealStateGroupCommit$Entry");

    Field queuesField = IdealStateGroupCommit.class.getDeclaredField("_queues");
    queuesField.setAccessible(true);
    Object[] queues = (Object[]) queuesField.get(commit);

    int bucket = (resourceName.hashCode() & Integer.MAX_VALUE) % queues.length;
    Object queue = queues[bucket];

    Field pendingField = queueClass.getDeclaredField("_pending");
    pendingField.setAccessible(true);
    ConcurrentLinkedQueue<Object> pending = (ConcurrentLinkedQueue<Object>) pendingField.get(queue);

    Function<IdealState, IdealState> stuckUpdater = is -> {
      throw new HelixHelper.PermanentUpdaterException(
          "simulated exceeded max segment completion time");
    };

    Constructor<?> entryCtor = entryClass.getDeclaredConstructor(String.class, Function.class);
    entryCtor.setAccessible(true);
    Object stuckEntry = entryCtor.newInstance(resourceName, stuckUpdater);

    pending.add(stuckEntry);
    LOGGER.info("Injected stuck (always-throwing) entry into queue bucket {} for resource {}",
        bucket, resourceName);
  }

  @Test(invocationCount = 5)
  public void testGroupCommit()
      throws InterruptedException {
    List<IdealStateGroupCommit> groupCommitList = new ArrayList<>();
    for (int i = 0; i < NUM_PROCESSORS; i++) {
      groupCommitList.add(new IdealStateGroupCommit());
    }
    for (int i = 0; i < NUM_UPDATES; i++) {
      for (int j = 0; j < NUM_TABLES; j++) {
        String tableName = TABLE_NAME_PREFIX + j + "_OFFLINE";
        IdealStateGroupCommit commit = groupCommitList.get(new Random().nextInt(NUM_PROCESSORS));
        Runnable runnable = new IdealStateUpdater(TEST_INSTANCE.getHelixManager(), commit, tableName, i);
        _executorService.submit(runnable);
      }
    }
    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      IdealState idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      while (idealState.getNumPartitions() < NUM_UPDATES) {
        Thread.sleep(500);
        idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      }
      Assert.assertEquals(idealState.getNumPartitions(), NUM_UPDATES);
      ControllerMetrics controllerMetrics = ControllerMetrics.get();
      long idealStateUpdateSuccessCount =
          controllerMetrics.getMeteredTableValue(tableName, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS).count();
      Assert.assertTrue(idealStateUpdateSuccessCount <= NUM_UPDATES);
      LOGGER.info("{} IdealState update are successfully committed with {} times zk updates.", NUM_UPDATES,
          idealStateUpdateSuccessCount);
    }
  }
}

class IdealStateUpdater implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommitTest.class);

  private final HelixManager _helixManager;
  private final IdealStateGroupCommit _commit;
  private final String _tableName;
  private final int _i;

  public IdealStateUpdater(HelixManager helixManager, IdealStateGroupCommit commit, String tableName, int i) {
    _helixManager = helixManager;
    _commit = commit;
    _tableName = tableName;
    _i = i;
  }

  @Override
  public void run() {
    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        idealState.setPartitionState("test_id" + _i, "test_id" + _i, "ONLINE");
        return idealState;
      }
    };

    while (true) {
      try {
        if (_commit.commit(_helixManager, _tableName, updater, RetryPolicies.noDelayRetryPolicy(1), false) != null) {
          break;
        }
      } catch (Throwable e) {
        LOGGER.warn("IdealState updater {} failed to commit.", _i, e);
      }
    }
  }
}
