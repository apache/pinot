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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsumerCoordinatorTest {

  private static class FakeRealtimeTableDataManager extends RealtimeTableDataManager {
    private final StreamIngestionConfig _streamIngestionConfig;
    private ConsumerCoordinator _consumerCoordinator;

    public FakeRealtimeTableDataManager(Semaphore segmentBuildSemaphore,
        boolean useIdealStateToCalculatePreviousSegment) {
      super(segmentBuildSemaphore);
      super._recentlyDeletedSegments = CacheBuilder.newBuilder().build();
      StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(List.of(new HashMap<>()));
      streamIngestionConfig.setEnforceConsumptionInOrder(true);
      if (useIdealStateToCalculatePreviousSegment) {
        streamIngestionConfig.setUseIdealStateToCalculatePreviousSegment(true);
      }
      _streamIngestionConfig = streamIngestionConfig;
    }

    @Override
    ConsumerCoordinator getConsumerCoordinator(int partitionId) {
      return _consumerCoordinator;
    }

    public void setConsumerCoordinator(ConsumerCoordinator consumerCoordinator) {
      _consumerCoordinator = consumerCoordinator;
    }

    @Override
    public StreamIngestionConfig getStreamIngestionConfig() {
      return _streamIngestionConfig;
    }

    @Override
    public String getServerInstance() {
      return "server_1";
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
        put("tableTest_REALTIME__2__101__20250304T0035Z", serverSegmentStatusMap);
        put("tableTest_REALTIME__2__100__20250304T0035Z", serverSegmentStatusMap);
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

    @Override
    public boolean isSegmentAlreadyConsumed(String currSegmentName) {
      return false;
    }
  }

  @Test
  public void testAwaitForPreviousSegmentSequenceNumber()
      throws InterruptedException {
    // 1. enable tracking segment seq num.
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, false);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);
    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);

    // 2. check if thread waits on prev segment seq
    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    Thread thread1 = new Thread(() -> {
      LLCSegmentName llcSegmentName = getLLCSegment(101);
      try {
        boolean b = consumerCoordinator.awaitForPreviousSegmentSequenceNumber(llcSegmentName, 5000);
        atomicBoolean.set(b);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    thread1.start();

    // 3. add prev segment and check if thread is unblocked.
    consumerCoordinator.trackSegment(getLLCSegment(100));

    TestUtils.waitForCondition(aVoid -> atomicBoolean.get(), 4000,
        "Thread waiting on previous segment should have been unblocked.");
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 100);

    // 4. check if second thread waits on prev segment seq until timeout and returns false
    AtomicBoolean atomicBoolean2 = new AtomicBoolean(false);
    Thread thread2 = new Thread(() -> {
      LLCSegmentName llcSegmentName = getLLCSegment(102);
      try {
        boolean b = consumerCoordinator.awaitForPreviousSegmentSequenceNumber(llcSegmentName, 500);
        atomicBoolean2.set(b);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    thread2.start();

    Thread.sleep(1500);

    Assert.assertFalse(atomicBoolean2.get());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 100);
  }

  @Test
  public void testFirstConsumer()
      throws InterruptedException {
    // 1. Enable tracking segment seq num.
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, false);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);
    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);
    ReentrantLock lock = (ReentrantLock) consumerCoordinator.getLock();
    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = getMockedRealtimeSegmentDataManager();
    Map<String, String> serverSegmentStatusMap = new HashMap<>() {{
      put("server_1", "ONLINE");
      put("server_3", "ONLINE");
    }};
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(100), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(102), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(104), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(106), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(107), serverSegmentStatusMap);
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(109), serverSegmentStatusMap);

    // 2. create multiple helix transitions in this order: 106, 109, 104, 107
    Thread thread1 = getNewThread(consumerCoordinator, getLLCSegment(106));
    Thread thread2 = getNewThread(consumerCoordinator, getLLCSegment(109));
    Thread thread3 = getNewThread(consumerCoordinator, getLLCSegment(104));
    Thread thread4 = getNewThread(consumerCoordinator, getLLCSegment(107));

    thread1.start();

    Thread.sleep(1000);

    // 3. load segment 100, 101, 102
    realtimeTableDataManager.registerSegment(getSegmentName(100), mockedRealtimeSegmentDataManager);
    realtimeTableDataManager.registerSegment(getSegmentName(101), mockedRealtimeSegmentDataManager);
    realtimeTableDataManager.registerSegment(getSegmentName(102), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);

    // 4. check all of the above threads wait
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertFalse(consumerCoordinator.getFirstTransitionProcessed().get());

    thread2.start();
    thread3.start();
    thread4.start();

    Thread.sleep(1000);

    // 5. check that first thread acquiring semaphore is of segment 104
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());

    realtimeTableDataManager.registerSegment(getSegmentName(104), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 104);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);

    // 6. check the next threads acquiring semaphore is 106
    consumerCoordinator.getSemaphore().release();
    realtimeTableDataManager.registerSegment(getSegmentName(106), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 106);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);
  }

  @Test
  public void testSequentialOrderNotRelyingOnIdealState()
      throws InterruptedException {
    // 1. Enable tracking segment seq num.
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, false);
    realtimeTableDataManager.setEnforceConsumptionInOrder(true);

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);
    realtimeTableDataManager.setConsumerCoordinator(consumerCoordinator);
    ReentrantLock lock = (ReentrantLock) consumerCoordinator.getLock();

    // 2. check first transition blocked on ideal state
    Thread thread1 = getNewThread(consumerCoordinator, getLLCSegment(101));
    thread1.start();

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), -1);
    Assert.assertFalse(consumerCoordinator.getFirstTransitionProcessed().get());

    RealtimeSegmentDataManager mockedRealtimeSegmentDataManager = getMockedRealtimeSegmentDataManager();

    // 3. register older segment and check seq num watermark and semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(90), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 90);
    Assert.assertFalse(consumerCoordinator.getFirstTransitionProcessed().get());

    // 4. register prev segment and check watermark and if thread was unblocked
    realtimeTableDataManager.registerSegment(getSegmentName(91), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());

    // 5. check that all the following transitions rely on seq num watermark and gets blocked.
    Thread thread2 = getNewThread(consumerCoordinator, getLLCSegment(102));
    Thread thread3 = getNewThread(consumerCoordinator, getLLCSegment(103));
    Thread thread4 = getNewThread(consumerCoordinator, getLLCSegment(104));
    thread3.start();
    thread2.start();
    thread4.start();

    Thread.sleep(2000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());

    // 6. check that all above threads are still blocked even if semaphore is released.
    consumerCoordinator.getSemaphore().release();

    Thread.sleep(1000);

    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 91);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());

    // 6. mark 101 seg as complete. Check 102 acquired the semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(101), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 101);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 0);

    // 7. register 102 seg, check if seg 103 is waiting on semaphore.
    realtimeTableDataManager.registerSegment(getSegmentName(102), mockedRealtimeSegmentDataManager);

    Thread.sleep(1000);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);

    // 8. release the semaphore and check if semaphore is acquired by seg 103.
    consumerCoordinator.getSemaphore().release();
    Thread.sleep(1000);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 102);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 0);

    // 8. register 103 seg and check if seg 104 is now queued on semaphore
    realtimeTableDataManager.registerSegment(getSegmentName(103), mockedRealtimeSegmentDataManager);
    Thread.sleep(1000);
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertEquals(consumerCoordinator.getMaxSegmentSeqNumLoaded(), 103);
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 0);
    Assert.assertEquals(consumerCoordinator.getSemaphore().getQueueLength(), 1);
  }

  @Test
  public void testSequentialOrderRelyingOnIdealState()
      throws InterruptedException {
    FakeRealtimeTableDataManager realtimeTableDataManager = new FakeRealtimeTableDataManager(null, true);
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
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
    Assert.assertTrue(consumerCoordinator.getFirstTransitionProcessed().get());

    consumerCoordinator.release();
    Assert.assertEquals(consumerCoordinator.getSemaphore().availablePermits(), 1);
    Assert.assertFalse(consumerCoordinator.getSemaphore().hasQueuedThreads());
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
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
    Assert.assertFalse(lock.hasQueuedThreads() && lock.isLocked());
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
    Mockito.when(realtimeTableDataManager.getServerInstance()).thenReturn("server_1");

    FakeConsumerCoordinator consumerCoordinator = new FakeConsumerCoordinator(true, realtimeTableDataManager);

    String segmentName = "tableTest_REALTIME__1__101__20250304T0035Z";
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    Assert.assertNotNull(llcSegmentName);
    String previousSegment = consumerCoordinator.getPreviousSegmentFromIdealState(llcSegmentName);
    Assert.assertEquals(previousSegment, "tableTest_REALTIME__1__91__20250304T0035Z");

    consumerCoordinator.getSegmentAssignment().clear();
    Map<String, String> serverSegmentStatusMap = new HashMap<>() {{
      put("server_3", "ONLINE");
    }};
    consumerCoordinator.getSegmentAssignment().put(getSegmentName(100), serverSegmentStatusMap);
    previousSegment = consumerCoordinator.getPreviousSegmentFromIdealState(llcSegmentName);
    Assert.assertNull(previousSegment);
  }

  @Test
  public void testIfSegmentIsConsumed() {
    RealtimeTableDataManager realtimeTableDataManager = Mockito.mock(RealtimeTableDataManager.class);
    Mockito.when(realtimeTableDataManager.fetchZKMetadata(getSegmentName(101))).thenReturn(null);

    ConsumerCoordinator consumerCoordinator = new ConsumerCoordinator(true, realtimeTableDataManager);
    Assert.assertTrue(consumerCoordinator.isSegmentAlreadyConsumed(getSegmentName(101)));

    SegmentZKMetadata mockSegmentZKMetadata = Mockito.mock(SegmentZKMetadata.class);

    Mockito.when(mockSegmentZKMetadata.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    Mockito.when(realtimeTableDataManager.fetchZKMetadata(getSegmentName(101))).thenReturn(mockSegmentZKMetadata);
    Assert.assertFalse(consumerCoordinator.isSegmentAlreadyConsumed(getSegmentName(101)));

    Mockito.when(mockSegmentZKMetadata.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.COMMITTING);
    Mockito.when(realtimeTableDataManager.fetchZKMetadata(getSegmentName(101))).thenReturn(mockSegmentZKMetadata);
    Assert.assertFalse(consumerCoordinator.isSegmentAlreadyConsumed(getSegmentName(101)));

    Mockito.when(mockSegmentZKMetadata.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.DONE);
    Mockito.when(realtimeTableDataManager.fetchZKMetadata(getSegmentName(101))).thenReturn(mockSegmentZKMetadata);
    Assert.assertTrue(consumerCoordinator.isSegmentAlreadyConsumed(getSegmentName(101)));

    Mockito.when(mockSegmentZKMetadata.getStatus()).thenReturn(CommonConstants.Segment.Realtime.Status.UPLOADED);
    Mockito.when(realtimeTableDataManager.fetchZKMetadata(getSegmentName(101))).thenReturn(mockSegmentZKMetadata);
    Assert.assertTrue(consumerCoordinator.isSegmentAlreadyConsumed(getSegmentName(101)));
  }

  private Thread getNewThread(FakeConsumerCoordinator consumerCoordinator, LLCSegmentName llcSegmentName) {
    return new Thread(() -> {
      try {
        consumerCoordinator.acquire(llcSegmentName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }, String.valueOf(llcSegmentName.getSequenceNumber()));
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
