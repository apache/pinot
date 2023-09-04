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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nullable;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.PolicyBasedResourceManager;
import org.apache.pinot.core.query.scheduler.resources.ResourceLimitPolicy;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.core.query.scheduler.TestHelper.createQueryRequest;
import static org.apache.pinot.core.query.scheduler.TestHelper.createServerQueryRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PrioritySchedulerTest {
  private static final ServerMetrics METRICS = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  private static boolean _useBarrier = false;
  private static CyclicBarrier _startupBarrier;
  private static CyclicBarrier _validationBarrier;
  private static CountDownLatch _numQueries = new CountDownLatch(1);

  @AfterMethod
  public void afterMethod() {
    _useBarrier = false;
    _startupBarrier = null;
    _validationBarrier = null;
    _numQueries = new CountDownLatch(1);
  }

  // Tests that there is no "hang" on stop
  @Test
  public void testStartStop()
      throws InterruptedException {
    TestPriorityScheduler scheduler = TestPriorityScheduler.create();
    scheduler.start();
    // 100 is arbitrary.. we need to wait for scheduler thread to have completely started
    Thread.sleep(100);
    scheduler.stop();
    long queueWakeTimeMicros = ((MultiLevelPriorityQueue) scheduler.getQueue()).getWakeupTimeMicros();
    long sleepTimeMs = queueWakeTimeMicros >= 1000 ? queueWakeTimeMicros / 1000 + 10 : 10;
    Thread.sleep(sleepTimeMs);
    assertFalse(scheduler._scheduler.isAlive());
  }

  @Test
  public void testStartStopQueries()
      throws ExecutionException, InterruptedException, IOException {
    TestPriorityScheduler scheduler = TestPriorityScheduler.create();
    scheduler.start();

    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 5);
    conf.setProperty(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 5);
    List<ListenableFuture<byte[]>> results = new ArrayList<>();
    results.add(scheduler.submit(createServerQueryRequest("1", METRICS)));
    TestSchedulerGroup group = TestPriorityScheduler._groupFactory._groupMap.get("1");
    group.addReservedThreads(10);
    group.addLast(createQueryRequest("1", METRICS));
    results.add(scheduler.submit(createServerQueryRequest("1", METRICS)));

    scheduler.stop();
    long queueWakeTimeMicros = ((MultiLevelPriorityQueue) scheduler.getQueue()).getWakeupTimeMicros();
    long sleepTimeMs = queueWakeTimeMicros >= 1000 ? queueWakeTimeMicros / 1000 + 10 : 10;
    Thread.sleep(sleepTimeMs);
    int hasServerShuttingDownError = 0;
    for (ListenableFuture<byte[]> result : results) {
      DataTable table = DataTableFactory.getDataTable(result.get());
      hasServerShuttingDownError +=
          table.getExceptions().containsKey(QueryException.SERVER_SCHEDULER_DOWN_ERROR.getErrorCode()) ? 1 : 0;
    }
    assertTrue(hasServerShuttingDownError > 0);
  }

  @Test
  public void testOneQuery()
      throws InterruptedException, ExecutionException, IOException, BrokenBarrierException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceLimitPolicy.THREADS_PER_QUERY_PCT, 50);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 40);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 20);
    _useBarrier = true;
    _startupBarrier = new CyclicBarrier(2);
    _validationBarrier = new CyclicBarrier(2);

    TestPriorityScheduler scheduler = TestPriorityScheduler.create(new PinotConfiguration(properties));
    int totalPermits = scheduler.getRunningQueriesSemaphore().availablePermits();
    scheduler.start();
    ListenableFuture<byte[]> result = scheduler.submit(createServerQueryRequest("1", METRICS));
    _startupBarrier.await();
    TestSchedulerGroup group = TestPriorityScheduler._groupFactory._groupMap.get("1");
    assertEquals(group.numRunning(), 1);
    assertEquals(group.getThreadsInUse(), 1);
    // The total number of threads allocated for query execution will be dependent on the underlying
    // platform (number of cores). Scheduler will assign total threads up to but not exceeding total
    // number of segments. On servers with less cores, this can assign only 1 thread (less than total segments)
    assertTrue(group.totalReservedThreads() <= 2 /* 2: numSegments in request*/);
    _validationBarrier.await();
    byte[] resultData = result.get();
    DataTable table = DataTableFactory.getDataTable(resultData);
    assertEquals(table.getMetadata().get(MetadataKey.TABLE.getName()), "1");
    // verify that accounting is handled right
    assertEquals(group.numPending(), 0);
    assertEquals(group.getThreadsInUse(), 0);
    assertEquals(group.totalReservedThreads(), 0);
    // -1 because we expect that 1 permit is blocked by the scheduler main thread
    assertEquals(scheduler.getRunningQueriesSemaphore().availablePermits(), totalPermits - 1);
    assertTrue(scheduler.getLatestQueryTime() > 0 && scheduler.getLatestQueryTime() <= System.currentTimeMillis());
    scheduler.stop();
  }

  @Test
  public void testMultiThreaded()
      throws InterruptedException {
    // add queries from multiple threads and verify that all those are executed
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, 60);
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, 20);
    properties.put(ResourceLimitPolicy.THREADS_PER_QUERY_PCT, 50);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 60);
    properties.put(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 40);
    properties.put(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 10);

    final TestPriorityScheduler scheduler = TestPriorityScheduler.create(new PinotConfiguration(properties));
    scheduler.start();
    final Random random = new Random();
    final ConcurrentLinkedQueue<ListenableFuture<byte[]>> results = new ConcurrentLinkedQueue<>();
    final int numThreads = 3;
    final int queriesPerThread = 10;
    _numQueries = new CountDownLatch(numThreads * queriesPerThread);

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      new Thread(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < queriesPerThread; j++) {
            results.add(scheduler.submit(createServerQueryRequest(Integer.toString(index), METRICS)));
            Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
          }
        }
      }).start();
    }
    _numQueries.await();
    scheduler.stop();
  }

  /*
   * Disabled because of race condition
   */
  @Test(enabled = false)
  public void testOutOfCapacityResponse()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 5);
    properties.put(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 1);
    TestPriorityScheduler scheduler = TestPriorityScheduler.create(new PinotConfiguration(properties));
    scheduler.start();
    List<ListenableFuture<byte[]>> results = new ArrayList<>();
    results.add(scheduler.submit(createServerQueryRequest("1", METRICS)));
    TestSchedulerGroup group = TestPriorityScheduler._groupFactory._groupMap.get("1");
    group.addReservedThreads(10);
    group.addLast(createQueryRequest("1", METRICS));
    results.add(scheduler.submit(createServerQueryRequest("1", METRICS)));
    DataTable dataTable = DataTableFactory.getDataTable(results.get(1).get());
    assertTrue(dataTable.getMetadata()
        .containsKey(DataTable.EXCEPTION_METADATA_KEY + QueryException.SERVER_OUT_OF_CAPACITY_ERROR.getErrorCode()));
    scheduler.stop();
  }

  @Test
  public void testSubmitBeforeRunning()
      throws ExecutionException, InterruptedException, IOException {
    TestPriorityScheduler scheduler = TestPriorityScheduler.create();
    ListenableFuture<byte[]> result = scheduler.submit(createServerQueryRequest("1", METRICS));
    // start is not called
    DataTable response = DataTableFactory.getDataTable(result.get());
    assertTrue(response.getExceptions().containsKey(QueryException.SERVER_SCHEDULER_DOWN_ERROR.getErrorCode()));
    assertFalse(response.getMetadata().containsKey(MetadataKey.TABLE.getName()));
    scheduler.stop();
  }

  static class TestPriorityScheduler extends PriorityScheduler {
    static TestSchedulerGroupFactory _groupFactory;
    static LongAccumulator _latestQueryTime;

    // store locally for easy access
    public TestPriorityScheduler(PinotConfiguration config, ResourceManager resourceManager,
        QueryExecutor queryExecutor, SchedulerPriorityQueue queue, ServerMetrics metrics,
        LongAccumulator latestQueryTime) {
      super(config, resourceManager, queryExecutor, queue, metrics, latestQueryTime);
    }

    public static TestPriorityScheduler create(PinotConfiguration config) {
      ResourceManager rm = new PolicyBasedResourceManager(config);
      QueryExecutor qe = new TestQueryExecutor();
      _groupFactory = new TestSchedulerGroupFactory();
      MultiLevelPriorityQueue queue =
          new MultiLevelPriorityQueue(config, rm, _groupFactory, new TableBasedGroupMapper());
      _latestQueryTime = new LongAccumulator(Long::max, 0);
      return new TestPriorityScheduler(config, rm, qe, queue, METRICS, _latestQueryTime);
    }

    public static TestPriorityScheduler create() {
      return create(new PinotConfiguration());
    }

    ResourceManager getResourceManager() {
      return _resourceManager;
    }

    @Override
    public String name() {
      return "TestScheduler";
    }

    public Semaphore getRunningQueriesSemaphore() {
      return _runningQueriesSemaphore;
    }

    Thread getSchedulerThread() {
      return _scheduler;
    }

    SchedulerPriorityQueue getQueue() {
      return _queryQueue;
    }

    public long getLatestQueryTime() {
      return _latestQueryTime.get();
    }
  }

  static class TestQueryExecutor implements QueryExecutor {

    @Override
    public void init(PinotConfiguration config, InstanceDataManager instanceDataManager, ServerMetrics serverMetrics) {
    }

    @Override
    public void start() {
    }

    @Override
    public void shutDown() {
    }

    @Override
    public InstanceResponseBlock execute(ServerQueryRequest queryRequest, ExecutorService executorService,
        @Nullable ResultsBlockStreamer streamer) {
      if (_useBarrier) {
        try {
          _startupBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      InstanceResponseBlock instanceResponse = new InstanceResponseBlock();
      instanceResponse.addMetadata(MetadataKey.TABLE.getName(), queryRequest.getTableNameWithType());
      if (_useBarrier) {
        try {
          _validationBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      _numQueries.countDown();
      return instanceResponse;
    }
  }
}
