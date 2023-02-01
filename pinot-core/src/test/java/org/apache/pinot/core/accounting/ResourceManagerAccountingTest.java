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
package org.apache.pinot.core.accounting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ResourceManagerAccountingTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerAccountingTest.class);

  /**
   * Test thread cpu usage tracking in multithread environment, add @Test to run.
   * Default to unused as this is a proof of concept and will take a long time to run.
   * The last occurrence of `Finished task mem: {q%d=...}` (%d in 0, 1, ..., 29) in log should
   * have the value of around 150000000 ~ 210000000
   */
  @SuppressWarnings("unused")
  public void testCPUtimeProvider()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs);
    Future[] futures = new Future[2000];
    AtomicInteger atomicInteger = new AtomicInteger();

    for (int k = 0; k < 30; k++) {
      int finalK = k;
      rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          rm.getQueryWorkers().submit(() -> {
            ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadResourceUsageProvider,
                threadExecutionContext);
            for (int i = 0; i < (finalJ + 1) * 10; i++) {
              Tracing.ThreadAccountantOps.sample();
              for (int m = 0; m < 1000; m++) {
                atomicInteger.getAndAccumulate(m % 178123, Integer::sum);
              }
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
              }
            }
            Tracing.ThreadAccountantOps.clear();
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
          Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }

  /**
   * Test thread memory usage tracking in multithread environment, add @Test to run.
   * Default to unused as this is a proof of concept and will take a long time to run.
   * The last occurrence of `Finished task mem: {q%d=...}` (%d in 0, 1, ..., 29) in log should
   * have the value of around 4416400 (550 * 1000 * 8 + some overhead).
   */
  @SuppressWarnings("unused")
  public void testThreadMemoryAccounting()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs);

    for (int k = 0; k < 30; k++) {
      int finalK = k;
      rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          rm.getQueryWorkers().submit(() -> {
            ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadResourceUsageProvider,
                threadExecutionContext);
            long[][] a = new long[1000][];
            for (int i = 0; i < (finalJ + 1) * 10; i++) {
              Tracing.ThreadAccountantOps.sample();
              a[i] = new long[1000];
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
              }
            }
            Tracing.ThreadAccountantOps.clear();
            System.out.println(a[0][0]);
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
          Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }

  /**
   * Test the mechanism of worker thread checking for runnerThread's interruption flag
   */
  @Test
  public void testWorkerThreadInterruption()
      throws Exception {
    ResourceManager rm = getResourceManager(2, 5, 1, 3, Collections.emptyMap());
    AtomicReference<Future>[] futures = new AtomicReference[5];
    for (int i = 0; i < 5; i++) {
      futures[i] = new AtomicReference<>();
    }
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    AtomicReference<Thread> runnerThread = new AtomicReference<>();
    rm.getQueryRunners().submit(() -> {
      Thread thread = Thread.currentThread();
      runnerThread.set(thread);
      for (int j = 0; j < 5; j++) {
        futures[j].set(rm.getQueryWorkers().submit(() -> {
          for (int i = 0; i < 1000000; i++) {
            try {
              Thread.sleep(5);
            } catch (InterruptedException ignored) {
            }
            if (thread.isInterrupted()) {
              throw new EarlyTerminationException();
            }
          }
        }));
      }
      while (true) {
      }
    });
    Thread.sleep(50);
    runnerThread.get().interrupt();

    for (int i = 0; i < 5; i++) {
      try {
        futures[i].get().get();
      } catch (ExecutionException e) {
        Assert.assertFalse(futures[i].get().isCancelled());
        Assert.assertTrue(futures[i].get().isDone());
        Assert.assertEquals(e.getMessage(), "org.apache.pinot.spi.exception.EarlyTerminationException");
        return;
      }
    }
    Assert.fail("Expected EarlyTerminationException to be thrown");
  }

  /**
   * Test thread memory usage tracking and query killing in multi-thread environment， add @Test to run.
   */
  @SuppressWarnings("unused")
  public void testThreadMemory()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.9f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs);
    Future[] futures = new Future[30];

    for (int k = 0; k < 4; k++) {
      int finalK = k;
      futures[finalK] = rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        Future[] futuresThread = new Future[10];
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          futuresThread[j] = rm.getQueryWorkers().submit(() -> {
            ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadResourceUsageProvider,
                threadExecutionContext);
            long[][] a = new long[1000][];
            for (int i = 0; i < (finalK + 1) * 80; i++) {
              Tracing.ThreadAccountantOps.sample();
              if (Thread.interrupted() || thread.isInterrupted()) {
                Tracing.ThreadAccountantOps.clear();
                LOGGER.error("KilledWorker " + queryId + " " + finalJ);
                return;
              }
              a[i] = new long[200000];
              for (int m = 0; m < 10000; m++) {
                a[i][m] = m % 178123;
              }
            }
            Tracing.ThreadAccountantOps.clear();
            System.out.println(a[0][0]);
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
        } catch (InterruptedException e) {
          for (int i = 0; i < 10; i++) {
            futuresThread[i].cancel(true);
          }
          LOGGER.error("Killed " + queryId);
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }

  private ResourceManager getResourceManager(int runners, int workers, final int softLimit, final int hardLimit,
      Map<String, Object> map) {

    return new ResourceManager(getConfig(runners, workers, map)) {

      @Override
      public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
        return new QueryExecutorService() {
          @Override
          public void execute(Runnable command) {
            getQueryWorkers().execute(command);
          }
        };
      }

      @Override
      public int getTableThreadsHardLimit() {
        return hardLimit;
      }

      @Override
      public int getTableThreadsSoftLimit() {
        return softLimit;
      }
    };
  }

  private PinotConfiguration getConfig(int runners, int workers, Map<String, Object> map) {
    Map<String, Object> properties = new HashMap<>(map);
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);
    return new PinotConfiguration(properties);
  }
}
