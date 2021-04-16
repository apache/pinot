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
  SchedulerGroup group;
  final ServerMetrics metrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  final SchedulerGroupMapper groupMapper = new TableBasedGroupMapper();
  final TestSchedulerGroupFactory groupFactory = new TestSchedulerGroupFactory();
  final String groupOne = "1";
  final String groupTwo = "2";

  @BeforeMethod
  public void beforeMethod() {
    groupFactory.reset();
  }

  @Test
  public void testSimplePutTake()
      throws OutOfCapacityException {
    MultiLevelPriorityQueue queue = createQueue();
    // NOTE: Timing matters here...running through debugger for
    // over 30 seconds can cause the query to expire
    queue.put(createQueryRequest(groupOne, metrics));
    queue.put(createQueryRequest(groupTwo, metrics));
    queue.put(createQueryRequest(groupOne, metrics));

    assertEquals(groupFactory.numCalls.get(), 2);
    SchedulerQueryContext r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), groupOne);
    r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), groupOne);
    r = queue.take();
    assertEquals(r.getSchedulerGroup().name(), groupTwo);
  }

  @Test
  public void testPutOutOfCapacity()
      throws OutOfCapacityException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 2);

    PinotConfiguration configuration =new PinotConfiguration(properties);

    ResourceManager rm = new UnboundedResourceManager(configuration);
    MultiLevelPriorityQueue queue = createQueue(configuration, rm);
    queue.put(createQueryRequest(groupOne, metrics));
    groupFactory.groupMap.get(groupOne).addReservedThreads(rm.getTableThreadsHardLimit());
    // we should still be able to add one more waiting query
    queue.put(createQueryRequest(groupOne, metrics));
    // this assert is to test that above call to put() is not the one
    // throwing exception
    assertTrue(true);
    // it should throw now
    try {
      queue.put(createQueryRequest(groupOne, metrics));
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
    assertTrue(reader.reader.isAlive());
    assertEquals(reader.readQueries.size(), 0);
    sleepForQueueWakeup(queue);
    // add a request. We should get it back in atleast wakupTimeDuration (possibly sooner)
    queue.put(createQueryRequest(groupOne, metrics));
    sleepForQueueWakeup(queue);
    assertEquals(reader.readQueries.size(), 1);
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

    queue.put(createQueryRequest(groupOne, metrics));
    queue.put(createQueryRequest(groupOne, metrics));
    queue.put(createQueryRequest(groupTwo, metrics));
    // group one has higher priority but it's above soft thread limit
    TestSchedulerGroup testGroupOne = groupFactory.groupMap.get(groupOne);
    TestSchedulerGroup testGroupTwo = groupFactory.groupMap.get(groupTwo);

    testGroupOne.addReservedThreads(rm.getTableThreadsSoftLimit() + 1);
    QueueReader reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader.readQueries.size(), 1);
    assertEquals(reader.readQueries.poll().getSchedulerGroup().name(), groupTwo);

    // add one more group two
    queue.put(createQueryRequest(groupTwo, metrics));
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader.readQueries.size(), 1);
    assertEquals(reader.readQueries.poll().getSchedulerGroup().name(), groupTwo);

    // add one more groupTwo and set groupTwo threads to higher than groupOne
    queue.put(createQueryRequest(groupTwo, metrics));
    testGroupTwo.addReservedThreads(testGroupOne.totalReservedThreads() + 1);
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader.readQueries.size(), 1);
    assertEquals(reader.readQueries.poll().getSchedulerGroup().name(), groupOne);

    // set groupOne above hard limit
    testGroupOne.addReservedThreads(rm.getTableThreadsHardLimit());
    reader = new QueueReader(queue);
    reader.startAndWaitForRead();
    assertEquals(reader.readQueries.size(), 1);
    assertEquals(reader.readQueries.poll().getSchedulerGroup().name(), groupTwo);

    // all groups above hard limit
    queue.put(createQueryRequest(groupTwo, metrics));
    queue.put(createQueryRequest(groupTwo, metrics));
    queue.put(createQueryRequest(groupOne, metrics));
    testGroupTwo.addReservedThreads(rm.getTableThreadsHardLimit());
    reader = new QueueReader(queue);
    reader.startAndWaitForQueueWakeup();
    assertEquals(reader.readQueries.size(), 0);
    // try again
    sleepForQueueWakeup(queue);
    assertEquals(reader.readQueries.size(), 0);

    // now set thread limit lower for a group (aka. query finished)
    testGroupTwo.releasedReservedThreads(testGroupTwo.totalReservedThreads());
    sleepForQueueWakeup(queue);
    assertEquals(reader.readQueries.size(), 1);
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
    queue.put(createQueryRequest(groupOne, metrics, queryArrivalTimeMs));
    queue.put(createQueryRequest(groupTwo, metrics, queryArrivalTimeMs));
    // group one has higher priority but it's above soft thread limit
    TestSchedulerGroup testGroupOne = groupFactory.groupMap.get(groupOne);
    TestSchedulerGroup testGroupTwo = groupFactory.groupMap.get(groupTwo);
    QueueReader reader = new QueueReader(queue);
    reader.startAndWaitForQueueWakeup();
    assertTrue(reader.readQueries.isEmpty());
    assertTrue(testGroupOne.isEmpty());
    assertTrue(testGroupTwo.isEmpty());
    queue.put(createQueryRequest(groupOne, metrics));
    sleepForQueueWakeup(queue);
  }

  private MultiLevelPriorityQueue createQueue() {
    PinotConfiguration conf = new PinotConfiguration();
    return createQueue(conf, new UnboundedResourceManager(conf));
  }

  private MultiLevelPriorityQueue createQueue(PinotConfiguration config, ResourceManager rm) {
    return new MultiLevelPriorityQueue(config, rm, groupFactory, groupMapper);
  }

  // caller needs to start the thread
  class QueueReader {
    private final MultiLevelPriorityQueue queue;
    CyclicBarrier startBarrier = new CyclicBarrier(2);
    CountDownLatch readDoneSignal = new CountDownLatch(1);
    ConcurrentLinkedQueue<SchedulerQueryContext> readQueries = new ConcurrentLinkedQueue<>();
    Thread reader;

    QueueReader(final MultiLevelPriorityQueue queue) {
      Preconditions.checkNotNull(queue);
      this.queue = queue;
      reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            startBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          readQueries.add(queue.take());
          try {
            readDoneSignal.countDown();
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
      reader.start();
      startBarrier.await();
      readDoneSignal.await();
    }

    // Use this if the reader is not expected to complete read after queue wakeup duration
    void startAndWaitForQueueWakeup()
        throws InterruptedException, BrokenBarrierException {
      reader.start();
      startBarrier.await();
      readDoneSignal.await(queue.getWakeupTimeMicros() + TimeUnit.MICROSECONDS.convert(10, TimeUnit.MILLISECONDS),
          TimeUnit.MICROSECONDS);
    }
  }
}
