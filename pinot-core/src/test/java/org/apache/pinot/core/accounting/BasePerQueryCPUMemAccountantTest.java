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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;


abstract class BasePerQueryCPUMemAccountantTest {
  @AfterMethod
  void resetAccountant() {
    Tracing.unregisterThreadAccountant();
  }

  protected void startQueryThreads(String queryId, CountDownLatch sampleLatch, AtomicInteger terminationCount,
      List<Integer> threadMemoryAllocationSamples) {
    Thread anchorThread = new Thread(() -> {
      try {
        Tracing.ThreadAccountantOps.setupRunner(queryId, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        ThreadExecutionContext taskExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        int taskId = 0;
        for (Integer memoryAllocationSample : threadMemoryAllocationSamples) {
          createTaskThread(taskExecutionContext, terminationCount, sampleLatch, taskId++, memoryAllocationSample);
        }

        sampleLatch.await();
        Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (EarlyTerminationException e) {
        terminationCount.incrementAndGet();
      }
    });
    anchorThread.start();
  }

  private static void createTaskThread(ThreadExecutionContext parentContext, AtomicInteger terminationCount,
      CountDownLatch sampleLatch, int taskId, long memoryAllocationSampleBytes) {
    Thread workerThread = new Thread(() -> {
      try {
        Tracing.ThreadAccountantOps.setupWorker(taskId, parentContext);
        CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry =
            ((TestResourceAccountant) Tracing.getThreadAccountant()).getThreadEntry();
        threadEntry._currentThreadMemoryAllocationSampleBytes = memoryAllocationSampleBytes;

        sampleLatch.await();
        Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (EarlyTerminationException e) {
        terminationCount.incrementAndGet();
      }
    });
    workerThread.start();
  }

  protected void waitForQueryResourceTracker(TestResourceAccountant accountant, String queryId, long expectedBytes) {
    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> map = accountant.getQueryResources();
      return map.containsKey(queryId) && map.get(queryId).getAllocatedBytes() == expectedBytes;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");
  }
}
