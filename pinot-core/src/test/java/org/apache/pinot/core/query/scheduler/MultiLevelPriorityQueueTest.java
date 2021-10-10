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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.scheduler.resources.PolicyBasedResourceManager;
import org.apache.pinot.core.query.scheduler.resources.ResourceLimitPolicy;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.core.query.scheduler.TestHelper.createQueryRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MultiLevelPriorityQueueTest {
  private static final ServerMetrics METRICS = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  private static final SchedulerGroupMapper GROUP_MAPPER = new TableBasedGroupMapper();
  private static final TestSchedulerGroupFactory GROUP_FACTORY = new TestSchedulerGroupFactory();
  private static final String GROUP_ONE = "1";
  private static final String GROUP_TWO = "2";

  @BeforeMethod
  public void beforeMethod() {
    GROUP_FACTORY.reset();
  }

  @Test
  public void testSimplePutTake()
      throws OutOfCapacityException {
    MultiLevelPriorityQueue queue = createQueue();
    // NOTE: Timing matters here...running through debugger for
    // over 30 seconds can cause the query to expire
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    queue.put(createQueryRequest(GROUP_ONE, METRICS));

    assertEquals(GROUP_FACTORY._numCalls.get(), 2);
    SchedulerQueryContext r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), GROUP_ONE);
    r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), GROUP_ONE);
    r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), GROUP_TWO);
  }

  @Test
  public void testPutOutOfCapacity()
      throws OutOfCapacityException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 2);

    PinotConfiguration configuration = new PinotConfiguration(properties);

    ResourceManager rm = new UnboundedResourceManager(configuration);
    MultiLevelPriorityQueue queue = createQueue(configuration, rm);
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    GROUP_FACTORY._groupMap.get(GROUP_ONE).addReservedThreads(rm.getTableThreadsHardLimit());
    // we should still be able to add one more waiting query
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    // this assert is to test that above call to put() is not the one
    // throwing exception
    assertTrue(true);
    // it should throw now
    try {
      queue.put(createQueryRequest(GROUP_ONE, METRICS));
    } catch (OutOfCapacityException e) {
      assertTrue(true);
      return;
    }
    assertTrue(false);
  }

  @Test
  public void testPutForBlockedReader()
      throws Exception {
    // test adding a query immediately makes blocked take() to return
    final MultiLevelPriorityQueue queue = createQueue();
    QueueReader reader = new QueueReader(queue);
    // we know thread has started. Sleep for the wakeup duration and check again
    reader.startAndWaitForQueueWakeup();
    assertTrue(reader._reader.isAlive());
    assertEquals(reader._readQueries.size(), 0);
    sleepForQueueWakeup(queue);
    // add a request. We should get it back in atleast wakupTimeDuration (possibly sooner)
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    sleepForQueueWakeup(queue);
    assertEquals(reader._readQueries.size(), 1);
  }

  @Test
  public void testTakeWithLimits()
      throws OutOfCapacityException, BrokenBarrierException, InterruptedException {
    // Test that take() will not return query if that group is already using hardLimit resources
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, 40);
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, 10);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 20);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 80);

    PinotConfiguration configuration = new PinotConfiguration(properties);

    PolicyBasedResourceManager rm = new PolicyBasedResourceManager(configuration);
    MultiLevelPriorityQueue queue = createQueue(configuration, rm);

    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    // group one has higher priority but it's above soft thread limit
    TestSchedulerGroup testGroupOne = GROUP_FACTORY._groupMap.get(GROUP_ONE);
    TestSchedulerGroup testGroupTwo = GROUP_FACTORY._groupMap.get(GROUP_TWO);

    testGroupOne.addReservedThreads(rm.getTableThreadsSoftLimit() + 1);
    QueueReader reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader._readQueries.size(), 1);
    assertEquals(reader._readQueries.poll().getSchedulerGroup().name(), GROUP_TWO);

    // add one more group two
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader._readQueries.size(), 1);
    assertEquals(reader._readQueries.poll().getSchedulerGroup().name(), GROUP_TWO);

    // add one more groupTwo and set groupTwo threads to higher than groupOne
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    testGroupTwo.addReservedThreads(testGroupOne.totalReservedThreads() + 1);
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader._readQueries.size(), 1);
    assertEquals(reader._readQueries.poll().getSchedulerGroup().name(), GROUP_ONE);

    // set groupOne above hard limit
    testGroupOne.addReservedThreads(rm.getTableThreadsHardLimit());
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader._readQueries.size(), 1);
    assertEquals(reader._readQueries.poll().getSchedulerGroup().name(), GROUP_TWO);

    // all groups above hard limit
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    queue.put(createQueryRequest(GROUP_TWO, METRICS));
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    testGroupTwo.addReservedThreads(rm.getTableThreadsHardLimit());
    reader = new QueueReader(queue);
    reader.startAndWaitForQueueWakeup();
    assertEquals(reader._readQueries.size(), 0);
    // try again
    sleepForQueueWakeup(queue);
    assertEquals(reader._readQueries.size(), 0);

    // now set thread limit lower for a group (aka. query finished)
    testGroupTwo.releasedReservedThreads(testGroupTwo.totalReservedThreads());
    sleepForQueueWakeup(queue);
    assertEquals(reader._readQueries.size(), 1);
  }

  private void sleepForQueueWakeup(MultiLevelPriorityQueue queue)
      throws InterruptedException {
    // sleep is okay since we sleep for short time
    // add 10 millis to avoid any race condition around time boundary
    Thread.sleep(queue.getWakeupTimeMicros() / 1000 + 10);
  }

  @Test
  public void testNoPendingAfterTrim()
      throws OutOfCapacityException, BrokenBarrierException, InterruptedException {
    MultiLevelPriorityQueue queue = createQueue();
    // Pick a query arrival time older than the query deadline of 30s
    long queryArrivalTimeMs = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(100);
    queue.put(createQueryRequest(GROUP_ONE, METRICS, queryArrivalTimeMs));
    queue.put(createQueryRequest(GROUP_TWO, METRICS, queryArrivalTimeMs));
    // group one has higher priority but it's above soft thread limit
    TestSchedulerGroup testGroupOne = GROUP_FACTORY._groupMap.get(GROUP_ONE);
    TestSchedulerGroup testGroupTwo = GROUP_FACTORY._groupMap.get(GROUP_TWO);
    QueueReader reader = new QueueReader(queue);
    reader.startAndWaitForQueueWakeup();
    assertTrue(reader._readQueries.isEmpty());
    assertTrue(testGroupOne.isEmpty());
    assertTrue(testGroupTwo.isEmpty());
    queue.put(createQueryRequest(GROUP_ONE, METRICS));
    sleepForQueueWakeup(queue);
  }

  private MultiLevelPriorityQueue createQueue() {
    PinotConfiguration conf = new PinotConfiguration();
    return createQueue(conf, new UnboundedResourceManager(conf));
  }

  private MultiLevelPriorityQueue createQueue(PinotConfiguration config, ResourceManager rm) {
    return new MultiLevelPriorityQueue(config, rm, GROUP_FACTORY, GROUP_MAPPER);
  }

  // caller needs to start the thread
  class QueueReader {
    private final MultiLevelPriorityQueue _queue;
    CyclicBarrier _startBarrier = new CyclicBarrier(2);
    CountDownLatch _readDoneSignal = new CountDownLatch(1);
    ConcurrentLinkedQueue<SchedulerQueryContext> _readQueries = new ConcurrentLinkedQueue<>();
    Thread _reader;

    QueueReader(final MultiLevelPriorityQueue queue) {
      Preconditions.checkNotNull(queue);
      _queue = queue;
      _reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            _startBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          _readQueries.add(queue.take());
          try {
            _readDoneSignal.countDown();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    // this is for main thread that creates reader. Pattern is odd
    // it keeps calling code concise
    // Use this when test expects to read something from queue. This blocks
    // till an entry is read from the queue
    void startAndWaitForRead()
        throws BrokenBarrierException, InterruptedException {
      _reader.start();
      _startBarrier.await();
      _readDoneSignal.await();
    }

    // Use this if the reader is not expected to complete read after queue wakeup duration
    void startAndWaitForQueueWakeup()
        throws InterruptedException, BrokenBarrierException {
      _reader.start();
      _startBarrier.await();
      _readDoneSignal.await(_queue.getWakeupTimeMicros() + TimeUnit.MICROSECONDS.convert(10, TimeUnit.MILLISECONDS),
          TimeUnit.MICROSECONDS);
    }
  }
}
