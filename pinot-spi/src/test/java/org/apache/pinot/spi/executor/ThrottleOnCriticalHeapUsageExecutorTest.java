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
package org.apache.pinot.spi.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class ThrottleOnCriticalHeapUsageExecutorTest {
  @Test
  void testThrottle()
      throws Exception {
    ThreadResourceUsageAccountant accountant = new ThreadResourceUsageAccountant() {
      final AtomicLong _numCalls = new AtomicLong(0);

      @Override
      public void clear() {
      }

      @Override
      public boolean isAnchorThreadInterrupted() {
        return false;
      }

      @Override
      public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
          String workloadName) {
      }

      @Override
      public void setupWorker(int taskId, ThreadExecutionContext.TaskType taskType,
          @Nullable ThreadExecutionContext parentContext) {
      }

      @Nullable
      @Override
      public ThreadExecutionContext getThreadExecutionContext() {
        return null;
      }

      @Override
      public void sampleUsage() {
      }

      @Override
      public boolean throttleQuerySubmission() {
        return _numCalls.getAndIncrement() > 1;
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes,
          TrackingScope trackingScope) {
      }

      @Override
      public void startWatcherTask() {
      }

      @Override
      public Exception getErrorStatus() {
        return null;
      }

      @Override
      public Collection<? extends ThreadResourceTracker> getThreadResources() {
        return List.of();
      }

      @Override
      public Map<String, ? extends QueryResourceTracker> getQueryResources() {
        return Map.of();
      }
    };

    ThrottleOnCriticalHeapUsageExecutor executor =
        new ThrottleOnCriticalHeapUsageExecutor(Executors.newCachedThreadPool(), accountant);

    CyclicBarrier barrier = new CyclicBarrier(2);

    try {
      executor.execute(() -> {
        try {
          barrier.await();
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException | BrokenBarrierException e) {
          // do nothing
        }
      });

      barrier.await();
      try {
        executor.execute(() -> {
          // do nothing
        });
        fail("Should not allow more than 1 task");
      } catch (QueryException e) {
        // as expected
        assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
        assertEquals(e.getMessage(), "Tasks throttled due to high heap usage.");
      }
    } catch (BrokenBarrierException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
    }
  }
}
