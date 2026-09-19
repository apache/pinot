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
package org.apache.pinot.server.starter.helix;

import com.google.common.cache.CacheBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises table lifecycle ordering with controlled shutdown and segment callbacks, without starting a server.
public class HelixInstanceDataManagerLifecycleTest {
  private static final long TIMEOUT_SECONDS = 10;

  @DataProvider
  public Object[][] segmentTypes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "segmentTypes")
  public void testRecreationWaitsForOldLeaseShutdown(boolean consuming)
      throws Exception {
    String table = "lifecycle_" + consuming + "_REALTIME";
    TableDataManager oldManager = mock(TableDataManager.class);
    TableDataManager replacement = mock(TableDataManager.class);
    CountDownLatch shutdownEntered = new CountDownLatch(1);
    CountDownLatch finishShutdown = new CountDownLatch(1);
    CountDownLatch shutdownFinished = new CountDownLatch(1);
    AtomicInteger creations = new AtomicInteger();
    AtomicReference<SegmentBuildTimeLeaseExtender> replacementLease = new AtomicReference<>();
    SegmentBuildTimeLeaseExtender oldLease = SegmentBuildTimeLeaseExtender.getOrCreate("server", null, table);
    TestInstanceDataManager instance = new TestInstanceDataManager(name -> {
      if (creations.getAndIncrement() == 0) {
        return oldManager;
      }
      assertTrue(shutdownFinished.getCount() == 0, "Replacement initialized before old lease shutdown completed");
      replacementLease.set(SegmentBuildTimeLeaseExtender.getOrCreate("server", null, name));
      return replacement;
    });
    doAnswer(invocation -> {
      shutdownEntered.countDown();
      await(finishShutdown);
      oldLease.shutDown();
      shutdownFinished.countDown();
      return null;
    }).when(oldManager).shutDown();
    addSegment(instance, table, "old", consuming);
    FutureTask<Void> deletion = new FutureTask<>(() -> {
      instance.deleteTable(table, 1L);
      return null;
    });
    Thread deletionThread = new Thread(deletion, "delete-table");
    FutureTask<Void> recreation = new FutureTask<>(() -> {
      addSegment(instance, table, "new", consuming);
      return null;
    });
    Thread recreationThread = new Thread(recreation, "recreate-table");
    try {
      deletionThread.start();
      await(shutdownEntered);
      recreationThread.start();
      // Wait for the competing operation to reach the lock, or expose an incorrectly completed creation.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
      while (recreationThread.getState() != Thread.State.WAITING && !recreation.isDone()
          && System.nanoTime() < deadline) {
        Thread.sleep(10L);
      }
      assertFalse(recreation.isDone(), "Recreation must wait for the old table's shutdown");
      assertSame(recreationThread.getState(), Thread.State.WAITING, "Recreation did not reach the lifecycle lock");
      finishShutdown.countDown();
      deletion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      recreation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNotSame(replacementLease.get(), oldLease);
      assertSame(SegmentBuildTimeLeaseExtender.getLeaseExtender(table), replacementLease.get());
      assertSame(instance.getTableDataManager(table), replacement);
    } finally {
      finishShutdown.countDown();
      join(deletionThread);
      join(recreationThread);
      SegmentBuildTimeLeaseExtender lease = SegmentBuildTimeLeaseExtender.getLeaseExtender(table);
      if (lease != null) {
        lease.shutDown();
      }
    }
  }

  @Test
  public void testOtherTableCanInitializeDuringShutdown()
      throws Exception {
    TableDataManager oldManager = mock(TableDataManager.class);
    TableDataManager otherManager = mock(TableDataManager.class);
    CountDownLatch shutdownEntered = new CountDownLatch(1);
    CountDownLatch finishShutdown = new CountDownLatch(1);
    TestInstanceDataManager instance = new TestInstanceDataManager(name -> name.equals("old_REALTIME")
        ? oldManager : otherManager);
    doAnswer(invocation -> {
      shutdownEntered.countDown();
      await(finishShutdown);
      return null;
    }).when(oldManager).shutDown();
    instance.addConsumingSegment("old_REALTIME", "old");
    FutureTask<Void> deletion = new FutureTask<>(() -> {
      instance.deleteTable("old_REALTIME", 1L);
      return null;
    });
    Thread deletionThread = new Thread(deletion, "delete-other-table");
    FutureTask<Void> creation = new FutureTask<>(() -> {
      instance.addConsumingSegment("other_REALTIME", "other");
      return null;
    });
    Thread creationThread = new Thread(creation, "create-other-table");
    try {
      deletionThread.start();
      await(shutdownEntered);
      creationThread.start();
      creation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      verify(otherManager).addConsumingSegment("other");
    } finally {
      finishShutdown.countDown();
      join(deletionThread);
      join(creationThread);
    }
    deletion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test(dataProvider = "segmentTypes")
  public void testSegmentCallbackDoesNotHoldLifecycleLock(boolean consuming)
      throws Exception {
    TableDataManager manager = mock(TableDataManager.class);
    CountDownLatch addEntered = new CountDownLatch(1);
    CountDownLatch finishAdd = new CountDownLatch(1);
    if (consuming) {
      doAnswer(invocation -> {
        addEntered.countDown();
        await(finishAdd);
        return null;
      }).when(manager).addConsumingSegment(anyString());
    } else {
      doAnswer(invocation -> {
        addEntered.countDown();
        await(finishAdd);
        return null;
      }).when(manager).addOnlineSegment(anyString());
    }
    TestInstanceDataManager instance = new TestInstanceDataManager(name -> manager);
    FutureTask<Void> add = new FutureTask<>(() -> {
      addSegment(instance, "callback_REALTIME", "segment", consuming);
      return null;
    });
    Thread addThread = new Thread(add, "add-segment");
    FutureTask<Void> deletion = new FutureTask<>(() -> {
      instance.deleteTable("callback_REALTIME", 1L);
      return null;
    });
    Thread deletionThread = new Thread(deletion, "delete-during-add");
    try {
      addThread.start();
      await(addEntered);
      deletionThread.start();
      deletion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      verify(manager).shutDown();
    } finally {
      finishAdd.countDown();
      join(addThread);
      join(deletionThread);
    }
    add.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void testShutdownFailureReleasesLifecycleLock()
      throws Exception {
    TableDataManager oldManager = mock(TableDataManager.class);
    TableDataManager replacement = mock(TableDataManager.class);
    RuntimeException failure = new IllegalStateException("shutdown failure");
    doThrow(failure).when(oldManager).shutDown();
    AtomicInteger creations = new AtomicInteger();
    TestInstanceDataManager instance = new TestInstanceDataManager(name ->
        creations.getAndIncrement() == 0 ? oldManager : replacement);
    instance.addConsumingSegment("failure_REALTIME", "old");
    assertThrows(IllegalStateException.class, () -> instance.deleteTable("failure_REALTIME", 1L));
    assertNull(instance.getTableDataManager("failure_REALTIME"));
    FutureTask<Void> creation = new FutureTask<>(() -> {
      instance.addConsumingSegment("failure_REALTIME", "new");
      return null;
    });
    Thread creationThread = new Thread(creation, "create-after-shutdown-failure");
    try {
      creationThread.start();
      creation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      verify(replacement).addConsumingSegment("new");
    } finally {
      join(creationThread);
    }
  }

  @Test
  public void testCreationFailureReleasesLifecycleLock()
      throws Exception {
    TableDataManager manager = mock(TableDataManager.class);
    AtomicInteger creations = new AtomicInteger();
    TestInstanceDataManager instance = new TestInstanceDataManager(name -> {
      if (creations.getAndIncrement() == 0) {
        throw new IllegalStateException("creation failure");
      }
      return manager;
    });
    assertThrows(IllegalStateException.class, () -> instance.addOnlineSegment("failure_REALTIME", "first"));
    FutureTask<Void> creation = new FutureTask<>(() -> {
      instance.addOnlineSegment("failure_REALTIME", "second");
      return null;
    });
    Thread creationThread = new Thread(creation, "retry-creation");
    try {
      creationThread.start();
      creation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      verify(manager).addOnlineSegment("second");
    } finally {
      join(creationThread);
    }
  }

  private static void addSegment(HelixInstanceDataManager instance, String table, String segment, boolean consuming)
      throws Exception {
    if (consuming) {
      instance.addConsumingSegment(table, segment);
    } else {
      instance.addOnlineSegment(table, segment);
    }
  }

  private static void await(CountDownLatch latch)
      throws InterruptedException {
    assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timed out waiting for controlled lifecycle operation");
  }

  private static void join(Thread thread)
      throws InterruptedException {
    thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(thread.isAlive(), "Lifecycle worker did not terminate");
  }

  /// Supplies controlled table instances while exercising the real instance manager's map and lifecycle operations.
  private static class TestInstanceDataManager extends HelixInstanceDataManager {
    private final Function<String, TableDataManager> _factory;

    TestInstanceDataManager(Function<String, TableDataManager> factory) {
      _factory = factory;
      _recentlyDeletedTables = CacheBuilder.newBuilder().build();
    }

    @Override
    TableDataManager createTableDataManager(String tableNameWithType) {
      return _factory.apply(tableNameWithType);
    }
  }
}
