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
package org.apache.pinot.broker.broker;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.ServiceUnavailableException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class BrokerManagedAsyncExecutorProviderTest {

  public static BrokerMetrics _brokerMetrics;

  @BeforeClass
  public void setUp() {
    _brokerMetrics = new BrokerMetrics(CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX,
        PinotMetricUtils.getPinotMetricsRegistry(new PinotConfiguration()),
        CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS, Collections.emptyList());
  }

  @Test
  public void testExecutorService()
      throws InterruptedException, ExecutionException {
    // create a new instance of the executor provider
    BrokerManagedAsyncExecutorProvider provider = new BrokerManagedAsyncExecutorProvider(2, 2, 2, _brokerMetrics);

    // get the executor service
    ThreadPoolExecutor executor = (ThreadPoolExecutor) provider.getExecutorService();

    // submit a task to the executor service and wait for it to complete
    Future<Integer> futureResult = executor.submit(() -> 1 + 1);
    Integer result = futureResult.get();

    // verify that the task was executed and returned the expected result
    assertNotNull(result);
    assertEquals((int) result, 2);

    // wait for the executor service to shutdown
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testGet()
      throws InterruptedException {
    BrokerManagedAsyncExecutorProvider provider = new BrokerManagedAsyncExecutorProvider(1, 1, 1, _brokerMetrics);
    ExecutorService executorService = provider.getExecutorService();

    // verify that the executor has the expected properties
    assertNotNull(executorService);
    assertTrue(executorService instanceof ThreadPoolExecutor);

    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;

    assertEquals(1, threadPoolExecutor.getCorePoolSize());
    assertEquals(1, threadPoolExecutor.getMaximumPoolSize());

    BlockingQueue<Runnable> blockingQueue = threadPoolExecutor.getQueue();
    assertNotNull(blockingQueue);
    assertTrue(blockingQueue instanceof ArrayBlockingQueue);
    assertEquals(0, blockingQueue.size());
    assertEquals(1, blockingQueue.remainingCapacity());

    RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
    assertNotNull(rejectedExecutionHandler);
    assertTrue(
        rejectedExecutionHandler instanceof BrokerManagedAsyncExecutorProvider.BrokerThreadPoolRejectExecutionHandler);

    // test that the executor actually executes tasks
    AtomicInteger counter = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < 1; i++) {
      threadPoolExecutor.execute(() -> {
        counter.incrementAndGet();
        latch.countDown();
      });
    }
    latch.await();
    assertEquals(counter.get(), 1);
  }

  @Test(expectedExceptions = ServiceUnavailableException.class)
  public void testRejectHandler()
      throws InterruptedException {
    BrokerManagedAsyncExecutorProvider provider = new BrokerManagedAsyncExecutorProvider(1, 1, 1, _brokerMetrics);
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) provider.getExecutorService();

    // test the rejection policy
    AtomicInteger counter = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(10);
    for (int i = 0; i < 10; i++) {
      threadPoolExecutor.execute(() -> {
        counter.incrementAndGet();
        latch.countDown();
      });
    }
    latch.await();
  }
}
