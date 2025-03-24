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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsumerCoordinatorTest {

  private static class FakeRealtimeTableDataManager extends RealtimeTableDataManager {

    private ConsumerCoordinator _consumerCoordinator;
    private StreamIngestionConfig _streamIngestionConfig = null;

    public FakeRealtimeTableDataManager(Semaphore segmentBuildSemaphore, boolean trackSegmentSeq) {
      super(segmentBuildSemaphore);
      super._recentlyDeletedSegments = CacheBuilder.newBuilder().build();
      if (trackSegmentSeq) {
        StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(List.of(new HashMap<>()));
        streamIngestionConfig.setEnforceConsumptionInOrder(true);
        streamIngestionConfig.setTrackSegmentSeqNumber(true);
        _streamIngestionConfig = streamIngestionConfig;
      }
    }

    @Override
    ConsumerCoordinator getSemaphoreAccessCoordinator(int partitionId) {
      return _consumerCoordinator;
    }

    public void setConsumerCoordinator(ConsumerCoordinator consumerCoordinator) {
      _consumerCoordinator = consumerCoordinator;
    }

    @Override
    public StreamIngestionConfig getStreamIngestionConfig() {
      return _streamIngestionConfig;
    }
  }

  private static class FakeConsumerCoordinator extends ConsumerCoordinator {
    private final Map<String, Map<String, String>> _segmentAssignmentMap;

    public FakeConsumerCoordinator(boolean enforceConsumptionInOrder,
        RealtimeTableDataManager realtimeTableDataManager) {
      super(enforceConsumptionInOrder, realtimeTableDataManager);
      Map<String, String> serverSegmentStatusMap = new HashMap<>() {{
        put("server_1", "ONLINE");
        put("server_3", "ONLINE");
      }};
      _segmentAssignmentMap = new HashMap<>() {{
        put("tableTest_REALTIME__1__101__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__1__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__14__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__91__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__1__90__20250304T0035Z", serverSegmentStatusMap);
      }};
    }

    @Override
    public Map<String, Map<String, String>> getSegmentAssignment() {
      return _segmentAssignmentMap;
    }
  }

  @Test
  public void testFirstConsumer() {
    // 1. Enable tracking segment seq num.
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, true);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);
    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);
  }

  @Test
  public void testSequentialOrderNotRelyingOnIdealState()
      throws InterruptedException {
    // 1. Enable tracking segment seq num.
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, true);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);

    // 2. check first transition blocked on ideal state
    Thread thread1 = getNewThread(consumerCoordinator, getLLCSegment(101));
    thread1.start();

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), -1);
    Assert.assertFalse(consumerCoordinator.getIsFirstTransitionProcessed().get());

    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = getMockedRealtimeSegmentDataManager();

    // 3. register older segment and check seq num watermark and semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(90), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 90);
    Assert.assertFalse(consumerCoordinator.getIsFirstTransitionProcessed().get());

    // 4. register prev segment and check watermark and if thread was unblocked
    realtimeTableDataManager.registerSegment(getSegmentName(91), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());

    // 5. check that all the following transitions rely on seq num watermark and gets blocked.
    Thread thread2 = getNewThread(consumerCoordinator, getLLCSegment(102));
    Thread thread3 = getNewThread(consumerCoordinator, getLLCSegment(103));
    Thread thread4 = getNewThread(consumerCoordinator, getLLCSegment(104));
    thread3.start();
    thread2.start();
    thread4.start();

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());

    // 6. check that all above threads are still blocked even if semaphore is released.
    consumerCoordinator.getSemaphore().release();

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());

    // 6. mark 101 seg as complete. Check 102 acquired the semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(101), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 101);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 0);

    // 7. register 102 seg, check if seg 103 is waiting on semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(102), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);

    // 8. release the semaphore and check if semaphore is acquired by seg 103.
    consumerCoordinator.getSemaphore().release();
    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 0);

    // 8. register 103 seg and check if seg 104 is now queued on semaphore
    realtimeTableDataManager.registerSegment(getSegmentName(103), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 103);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);
  }

  @Test
  public void testSequentialOrderRelyingOnIdealState()
      throws InterruptedException {
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, false);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);

    // 1. test that acquire blocks when prev segment is not loaded.
    Thread thread = getNewThread(consumerCoordinator, getLLCSegment(101));
    thread.start();

    ReentrantLock lock = (ReentrantLock) consumerCoordinator.getLock();

    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = Mockito.mock(RealtimeSegmentDataManager.class);
    Mockito.when(mockedRealtimeSegmentDataManager.increaseReferenceCount()).thenReturn(true);
    Assert.assertNotNull(realtimeTableDataManager);

    // prev segment has seq 91, so registering seq 90 won't do anything.
    realtimeTableDataManager.registerSegment(getSegmentName(90), mockedRealtimeSegmentDataManager);

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);

    // 2. test that registering prev segment will unblock thread.
    realtimeTableDataManager.registerSegment(getSegmentName(91), mockedRealtimeSegmentDataManager);

    TestUtils.waitForCondition(aVoid -> (consumerCoordinator.getSemaphore().availablePermits() == 0), 5000,
        "Semaphore must be acquired after registering previous segment");
    Assert.assertFalse(lock.isLocked());
    Assert.assertFalse(lock.hasQueuedThreads());
    Assert.assertTrue(consumerCoordinator.getIsFirstTransitionProcessed().get());

    consumerCoordinator.release();
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), -1);
    realtimeTableDataManager.registerSegment(getSegmentName(101), mockedRealtimeSegmentDataManager);

    // 3. test that segment 103 will be blocked.
    Map<String, String> serverSegmentStatusMap = new HashMap<>() {{
      put("server_1", "ONLINE");
      put("server_3", "ONLINE");
    }};
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(102), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(103), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(104), serverSegmentStatusMap);

    Thread thread1 = getNewThread(consumerCoordinator, getLLCSegment(103));
    thread1.start();

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);

    // 3. test that segment 102 will acquire semaphore.
    Thread thread2 = getNewThread(consumerCoordinator, getLLCSegment(102));
    thread2.start();

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());

    // 4. registering seg 102 should unblock seg 103
    realtimeTableDataManager.registerSegment(getSegmentName(102), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);

    // 5. releasing semaphore should let seg 103 acquire it
    consumerCoordinator.getSemaphore().release();

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 0);
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), -1);
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

  private Thread getNewThread(FakeConsumerCoordinator consumerCoordinator, LLCSegmentName llcSegmentName) {
    return new Thread(() -> {
      try {
        consumerCoordinator.acquire(llcSegmentName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private RealtimeSegmentDataManager getMockedRealtimeSegmentDataManager() {
    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = Mockito.mock(RealtimeSegmentDataManager.class);
    Mockito.when(mockedRealtimeSegmentDataManager.increaseReferenceCount()).thenReturn(true);
    Assert.assertNotNull(mockedRealtimeSegmentDataManager);
    return mockedRealtimeSegmentDataManager;
  }

  private LLCSegmentName getLLCSegment(int seqNum) {
    String segmentName = getSegmentName(seqNum);
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    Assert.assertNotNull(llcSegmentName);
    return llcSegmentName;
  }

  private String getSegmentName(int seqNum) {
    return "tableTest_REALTIME__1__" + seqNum + "__20250304T0035Z";
  }
}
