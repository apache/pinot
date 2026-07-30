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
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.IdealStateGroupCommit;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
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
   * Regression test for the leader-entry cancellation fix in {@link IdealStateGroupCommit#commit}.
   *
   * Setup mirrors the pre-fix race condition that produced "in IdealState, no ZK metadata"
   * orphans in the pauseless segment commit path:
   *
   *   1. The queue already contains an entry whose updater will throw PermanentUpdaterException
   *      (a pauseless segment whose 300s max-completion-time deadline has passed while waiting
   *      behind a long IdealState operation such as a bulk retention deletion).
   *   2. A fresh thread calls commit() with a healthy updater. Its entry is enqueued AFTER the
   *      stuck entry in FIFO order.
   *   3. The fresh thread becomes leader and iterates the queue in FIFO order.
   *
   * Pre-fix behavior (the bug): the stuck entry's updater threw, iteration stopped, the fresh
   * thread's commit() exited with that exception, but its OWN queued entry was never iterated and
   * stayed in _pending. A subsequent leader applied it. The fresh thread's caller observed "my
   * update failed" and ran cleanup (removing the newly-created segment's ZK metadata), yet the
   * subsequent leader's CAS put the segment in IdealState -- producing the orphan.
   *
   * Post-fix behavior (asserted here): the catch in commit() sets _cancelled on the fresh
   * thread's own entry and adds it to `processed`. The fresh thread's commit() still throws
   * (pre-fix all-or-nothing batch semantics are preserved), but a subsequent leader's iteration
   * sees the cancelled entry, skips it, and removes it. No orphan can form because the fresh
   * updater is never applied to IdealState by any future batch.
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
      // PermanentUpdaterException. Simulates a queued entry for a pauseless segment whose 300s
      // deadline has expired.
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
          freshException == null ? "(none)"
              : freshException.getClass().getSimpleName() + ": " + freshException.getMessage());

      // The fresh thread's commit() throws (the all-or-nothing batch semantics are preserved).
      Assert.assertNotNull(freshException,
          "Fresh thread's commit() should throw because the batched IdealState commit aborts when "
              + "the co-batched stuck entry's lambda throws PermanentUpdaterException.");

      // Step 3: A drainer commit runs. If the fresh thread's entry was still in _pending and
      // NOT cancelled (the pre-fix bug), the drainer would iterate it and write freshPartition
      // into IdealState -- creating the orphan when paired with a caller that has already
      // cleaned up new segment metadata. With the cancellation fix, the entry is skipped.
      Function<IdealState, IdealState> drainerUpdater = is -> {
        is.setPartitionState("drainerPartition", "instance1", "ONLINE");
        return is;
      };
      commit.commit(TEST_INSTANCE.getHelixManager(), tableName, drainerUpdater,
          RetryPolicies.noDelayRetryPolicy(1), false);

      IdealState finalState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      Map<String, String> freshMap = finalState.getInstanceStateMap("freshPartition");
      Map<String, String> drainerMap = finalState.getInstanceStateMap("drainerPartition");

      LOGGER.info("Final IdealState partitions: {}", finalState.getPartitionSet());
      LOGGER.info("freshPartition state map:    {}", freshMap);
      LOGGER.info("drainerPartition state map:  {}", drainerMap);

      // Sanity: the drainer's own change must be present (proves the drainer commit ran).
      Assert.assertNotNull(drainerMap, "Drainer commit should have written drainerPartition.");
      Assert.assertEquals(drainerMap.get("instance1"), "ONLINE");

      // The key assertion: the fresh updater's change must NOT be in IdealState. The cancellation
      // flag must have caused the drainer's iteration to skip and remove the fresh entry. If this
      // assertion fails, the cancellation fix has regressed and the orphan-creation race can
      // reoccur (caller threw, ran cleanup, but subsequent leader applied the update anyway).
      Assert.assertNull(freshMap,
          "Fresh updater's change must NOT be in IdealState. The fresh thread's commit() threw, "
              + "indicating to its caller that the update did not happen; a subsequent leader "
              + "must not have applied it. Found: " + freshMap);

      LOGGER.info("=== FIX VERIFIED: caller threw, no future leader applied the cancelled entry ===");
    } finally {
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  /**
   * Multi-thread consistency test: N stuck owners + M fresh owners race to commit on the same
   * resource. The post-fix contract is per-owner consistency, not per-owner success: every owner
   * either observes a successful commit AND its update is present in IdealState, OR observes an
   * exception AND its update is NOT present in IdealState. The cancellation flag must ensure no
   * orphan can be left behind by any failed leader.
   */
  @Test
  public void testMultipleStuckAndFreshConsistency()
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

      // After the race, a drainer commit ensures any successful-but-not-yet-applied entries are
      // CAS-written, and any cancelled entries are skipped+removed. This makes the final
      // IdealState the ground truth for which fresh updaters actually took effect.
      commit.commit(TEST_INSTANCE.getHelixManager(), tableName, is -> {
        is.setPartitionState("drainer", "instance1", "ONLINE");
        return is;
      }, RetryPolicies.noDelayRetryPolicy(1), false);

      IdealState finalState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);

      // Every stuck owner must observe a throw. The stuck lambda always throws
      // PermanentUpdaterException, so the batch they participated in always aborts.
      for (int i = 0; i < stuckCount; i++) {
        Throwable t = stuckResults.get(i).get();
        Assert.assertNotNull(t, "Stuck owner " + i + " should have thrown");
      }

      // Consistency check for every fresh owner: observed outcome must match IdealState.
      int freshSucceeded = 0;
      int freshThrew = 0;
      for (int i = 0; i < freshCount; i++) {
        Throwable t = freshResults.get(i).get();
        Map<String, String> inIs = finalState.getInstanceStateMap("freshPart-" + i);
        if (t == null) {
          freshSucceeded++;
          Assert.assertNotNull(inIs,
              "Fresh owner " + i + " observed success but its partition is NOT in IdealState. "
                  + "Owner outcome and IdealState are inconsistent.");
          Assert.assertEquals(inIs.get("instance1"), "ONLINE");
        } else {
          freshThrew++;
          Assert.assertNull(inIs,
              "Fresh owner " + i + " observed exception [" + t.getMessage()
                  + "] but its partition IS in IdealState. This is the orphan-creating race: "
                  + "the cancellation flag must skip a failed leader's entry so no subsequent "
                  + "batch applies it.");
        }
      }

      LOGGER.info("=== Consistency verified: {} stuck threw, {} fresh succeeded, {} fresh threw ===",
          stuckCount, freshSucceeded, freshThrew);
    } finally {
      if (!pool.isShutdown()) {
        pool.shutdownNow();
      }
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  /**
   * Integration-style test that walks the same chain the orphan-creating production bug walked:
   *
   *   - Step 2 of commitSegmentMetadataInternal: write the new consuming segment's ZK metadata
   *     to the property store with status IN_PROGRESS.
   *   - Step 3: update IdealState to mark the old segment ONLINE and add the new segment as
   *     CONSUMING -- via IdealStateGroupCommit.commit().
   *   - If commit() throws, simulate the catch block in commitSegmentMetadataInternal: call
   *     removeSegmentZKMetadataBestEffort on the new segment.
   *   - Then let a drainer commit run (mimics any subsequent batch picking up the queue).
   *
   * Orphan condition: the new segment ends up in IdealState but its ZK metadata is gone.
   * Pre-fix: a co-batched stuck pauseless entry threw, causing this caller's commit() to throw
   * (so cleanup ran) while the caller's own updater stayed in _pending and was applied by the
   * next leader -- producing the orphan.
   *
   * The test asserts that no orphan is produced. With the cancellation fix it passes -- the
   * caller still throws (preserving pre-fix all-or-nothing batch semantics), but the entry is
   * cancelled so the drainer never applies it.
   */
  @Test
  public void testNoOrphanWhenCoBatchedEntryThrowsDuringStepThree()
      throws Exception {
    String tableName = TABLE_NAME_PREFIX + "orphan_REALTIME";
    String oldSegment = "orphanTest__0__0__T0000Z";
    String newSegment = "orphanTest__0__1__T0010Z";
    String instance = "instance1";

    IdealState initialState = new IdealState(tableName);
    initialState.setStateModelDefRef("OnlineOffline");
    initialState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    initialState.setReplicas("1");
    initialState.setNumPartitions(0);
    initialState.setPartitionState(oldSegment, instance, "CONSUMING");
    TEST_INSTANCE.getHelixAdmin().addResource(TEST_INSTANCE.getHelixClusterName(), tableName, initialState);

    ZkHelixPropertyStore<ZNRecord> propertyStore = TEST_INSTANCE.getPropertyStore();

    try {
      IdealStateGroupCommit commit = new IdealStateGroupCommit();

      // Step 2 (mimicked): persist new segment's ZK metadata as IN_PROGRESS.
      SegmentZKMetadata newSegmentMetadata = new SegmentZKMetadata(newSegment);
      newSegmentMetadata.setStatus(Status.IN_PROGRESS);
      boolean wrote = ZKMetadataProvider.setSegmentZKMetadata(propertyStore, tableName, newSegmentMetadata, -1);
      Assert.assertTrue(wrote, "Pre-condition: writing new segment ZK metadata should succeed");

      // Co-batched in-flight throwing entry: mimics a pauseless segment that has timed out
      // and whose queued updater will throw PermanentUpdaterException when iterated.
      injectStuckEntry(commit, tableName);

      // Step 3 updater: flip old segment to ONLINE, add new segment as CONSUMING.
      Function<IdealState, IdealState> stepThreeUpdater = is -> {
        is.setPartitionState(oldSegment, instance, "ONLINE");
        is.setPartitionState(newSegment, instance, "CONSUMING");
        return is;
      };

      Throwable callerThrew = null;
      try {
        commit.commit(TEST_INSTANCE.getHelixManager(), tableName, stepThreeUpdater,
            RetryPolicies.noDelayRetryPolicy(1), false);
      } catch (Throwable t) {
        callerThrew = t;
        // Mimic commitSegmentMetadataInternal's catch: delete the new segment's ZK metadata
        // best-effort because Step 3 appeared to fail.
        ZKMetadataProvider.removeSegmentZKMetadata(propertyStore, tableName, newSegment);
        LOGGER.info("Caller observed exception, ran removeSegmentZKMetadataBestEffort(newSegment): {}",
            t.getMessage());
      }

      // Drainer commit: mimics any subsequent IdealStateGroupCommit batch that processes
      // whatever is left in the queue. If our caller's updater were still queued (the pre-fix
      // bug), the drainer would apply it here -- writing newSegment into IdealState even
      // though we already deleted its ZK metadata. With the cancellation fix, the drainer's
      // iteration skips the cancelled entry.
      Function<IdealState, IdealState> drainer = is -> {
        is.setPartitionState("drainerPartition", instance, "ONLINE");
        return is;
      };
      commit.commit(TEST_INSTANCE.getHelixManager(), tableName, drainer,
          RetryPolicies.noDelayRetryPolicy(1), false);

      // Inspect ground truth.
      IdealState finalState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      Map<String, String> newSegInIs = finalState.getInstanceStateMap(newSegment);
      SegmentZKMetadata newSegMetadataAfter = ZKMetadataProvider.getSegmentZKMetadata(
          propertyStore, tableName, newSegment);

      boolean inIdealState = newSegInIs != null;
      boolean hasZkMetadata = newSegMetadataAfter != null;

      LOGGER.info("Caller threw: {}", callerThrew != null);
      LOGGER.info("newSegment in IdealState: {}", inIdealState);
      LOGGER.info("newSegment has ZK metadata: {}", hasZkMetadata);

      // The orphan condition is precisely "in IdealState && no ZK metadata".
      // Pre-fix: this assertion FAILS -- caller threw, ran cleanup, but the queued updater was
      // applied by the drainer.
      // Post-fix (cancellation): the entry is cancelled in commit()'s catch; the drainer skips
      // it. The newSegment is neither in IdealState nor has ZK metadata -- a clean failure.
      Assert.assertFalse(inIdealState && !hasZkMetadata,
          "ORPHAN DETECTED: newSegment is in IdealState but has no ZK metadata. "
              + "callerThrew=" + (callerThrew != null ? callerThrew.getMessage() : "null")
              + ". The cancellation fix in IdealStateGroupCommit must skip the caller's "
              + "still-queued entry so no subsequent batch applies it.");

      // Post-fix expected state: caller threw AND newSegment is neither in IdealState nor has
      // ZK metadata. The cancellation prevented the queued entry from being applied.
      Assert.assertNotNull(callerThrew,
          "Post-fix expectation: caller's commit() throws (all-or-nothing batch semantics).");
      Assert.assertFalse(inIdealState,
          "newSegment must NOT be in IdealState -- the caller's cancelled entry must be skipped "
              + "by the drainer's iteration.");
      Assert.assertFalse(hasZkMetadata,
          "newSegment's ZK metadata must have been cleaned up by the caller's catch.");
    } finally {
      // Best-effort cleanup of any stragglers. ControllerTest.cleanup() asserts /SEGMENTS has
      // zero child table directories, so we must remove the table-level node too.
      try {
        ZKMetadataProvider.removeSegmentZKMetadata(propertyStore, tableName, newSegment);
      } catch (Throwable ignored) {
      }
      try {
        propertyStore.remove("/SEGMENTS/" + tableName, org.apache.helix.AccessOption.PERSISTENT);
      } catch (Throwable ignored) {
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
