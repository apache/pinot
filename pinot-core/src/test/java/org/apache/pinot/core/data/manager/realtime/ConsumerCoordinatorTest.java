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
/// **
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsumerCoordinatorTest {

  private static class FakeRealtimeTableDataManager extends RealtimeTableDataManager {

    private ConsumerCoordinator _consumerCoordinator;

    public FakeRealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
      super(segmentBuildSemaphore);
      super._recentlyDeletedSegments = CacheBuilder.newBuilder().build();
    }

    @Override
    ConsumerCoordinator getSemaphoreAccessCoordinator(int partitionId) {
      return _consumerCoordinator;
    }

    public void setConsumerCoordinator(ConsumerCoordinator consumerCoordinator) {
      _consumerCoordinator = consumerCoordinator;
    }
  }

  private static class FakeConsumerCoordinator extends ConsumerCoordinator {
    public FakeConsumerCoordinator(boolean enforceConsumptionInOrder,
        RealtimeTableDataManager realtimeTableDataManager) {
      super(enforceConsumptionInOrder, realtimeTableDataManager);
    }

    @Override
    public Map<String, Map<String, String>> getSegmentAssignment() {
      Map<String, String> serverSegmentStatusMap = new HashMap<>() {{
        put("server_1", "ONLINE");
        put("server_3", "ONLINE");
      }};
      return new HashMap<>() {{
        put("tableTest_REALTIME__1__101__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__1__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__14__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__111__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__131__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__91__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__90__20250304T0035Z", serverSegmentStatusMap);
      }};
    }
  }

  @Test
  public void testSequentialOrderRelyingOnIdealState()
      throws InterruptedException {
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);

    String segmentName = "tableTest_REALTIME__1__101__20250304T0035Z";
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    Assert.assertNotNull(llcSegmentName);

    // 1. test that acquire blocks when prev segment is not loaded.
    Thread thread = new Thread(() -> {
      try {
        consumerCoordinator.acquire(llcSegmentName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    thread.start();

    ReentrantLock lock = (ReentrantLock) consumerCoordinator.getLock();

    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = Mockito.mock(RealtimeSegmentDataManager.class);
    Mockito.when(mockedRealtimeSegmentDataManager.increaseReferenceCount()).thenReturn(true);
    Assert.assertNotNull(realtimeTableDataManager);

    // prev segment has seq 91, so registering seq 90 won't do anything.
    realtimeTableDataManager.registerSegment("tableTest_REALTIME__1__90__20250304T0035Z",
        mockedRealtimeSegmentDataManager);

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);

    // 2. test that registering prev segment will unblock thread.
    realtimeTableDataManager.registerSegment("tableTest_REALTIME__1__91__20250304T0035Z",
        mockedRealtimeSegmentDataManager);

    TestUtils.waitForCondition(aVoid -> (consumerCoordinator.getSemaphore().availablePermits() == 0), 5000,
        "Semaphore must be acquired after registering previous segment");
    Assert.assertFalse(lock.isLocked());
    Assert.assertFalse(lock.hasQueuedThreads());
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());

    consumerCoordinator.release();
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());

    Thread thread1 = new Thread(() -> {
      try {
        consumerCoordinator.acquire(llcSegmentName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    thread1.start();
    TestUtils.waitForCondition(aVoid -> (consumerCoordinator.getSemaphore().availablePermits() == 0), 5000,
        "Semaphore must be acquired after registering previous segment");

    Thread thread2 = new Thread(() -> {
      try {
        consumerCoordinator.acquire(llcSegmentName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    thread2.start();
    TestUtils.waitForCondition(aVoid -> (consumerCoordinator.getSemaphore().availablePermits() == 0) && (
            consumerCoordinator.getSemaphore().getQueueLength() == 1), 50000,
        "Semaphore must be already acquired by another thread.");
  }

  @Test
  public void testRandomOrder()
      throws InterruptedException {
    RealtimeTableDataManager realtimeTableDataManager = Mockito.mock(RealtimeTableDataManager.class);
    Mockito.when(realtimeTableDataManager.getTableName()).thenReturn("tableTest_REALTIME");

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(false, realtimeTableDataManager);

    String segmentName = "tableTest_REALTIME__1__101__20250304T0035Z";
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    Assert.assertNotNull(llcSegmentName);
    consumerCoordinator.acquire(llcSegmentName);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());

    consumerCoordinator.release();
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());
  }

  @Test
  public void testPreviousSegment() {
    RealtimeTableDataManager realtimeTableDataManager = Mockito.mock(RealtimeTableDataManager.class);
    Mockito.when(realtimeTableDataManager.getTableName()).thenReturn("tableTest_REALTIME");

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);

    String segmentName = "tableTest_REALTIME__1__101__20250304T0035Z";
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    Assert.assertNotNull(llcSegmentName);
    String previousSegment = consumerCoordinator.getPreviousSegment(llcSegmentName);
    Assert.assertEquals(previousSegment, "tableTest_REALTIME__1__91__20250304T0035Z");
  }
}
