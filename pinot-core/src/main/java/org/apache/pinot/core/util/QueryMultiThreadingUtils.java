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
package org.apache.pinot.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * The term `task` and `thread` are used interchangeably in the logic to parallelize CombinePlanNode and
 * BaseCombineOperator. This class provides common methods used to set up the parallel processing.
 */
public class QueryMultiThreadingUtils {
  private QueryMultiThreadingUtils() {
  }

  /**
   * Use at most 10 or half of the processors threads for each query. If there are less than 2 processors, use 1
   * thread.
   * <p>NOTE: Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g.
   * Kubernetes.
   */
  public static final int MAX_NUM_THREADS_PER_QUERY =
      Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));

  /**
   * Returns the number of tasks for the query execution. The tasks can be assigned to multiple execution threads so
   * that they can run in parallel. The parallelism is bounded by the task count.
   */
  public static int getNumTasksForQuery(int numOperators, int maxExecutionThreads) {
    return getNumTasks(numOperators, 1, maxExecutionThreads);
  }

  public static int getNumTasks(int numWorkUnits, int minUnitsPerThread, int maxExecutionThreads) {
    if (numWorkUnits <= minUnitsPerThread) {
      return 1;
    }
    if (maxExecutionThreads <= 0) {
      maxExecutionThreads = MAX_NUM_THREADS_PER_QUERY;
    }
    return Math.min((numWorkUnits + minUnitsPerThread - 1) / minUnitsPerThread, maxExecutionThreads);
  }

  /**
   * Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
   * returns. We need to ensure no execution left before the main thread returning because the main thread holds the
   * reference to the segments, and if the segments are deleted/refreshed, the segments can be released after the main
   * thread returns, which would lead to undefined behavior (even JVM crash) when executing queries against them.
   */
  public static <T> void runTasksWithDeadline(int numTasks, Function<Integer, T> taskFunc, Consumer<T> resCollector,
      Consumer<Exception> errHandler, ExecutorService executorService, long deadlineInMs) {
    Phaser phaser = new Phaser(1);
    List<Future<T>> futures = new ArrayList<>(numTasks);
    for (int i = 0; i < numTasks; i++) {
      int index = i;
      futures.add(executorService.submit(new TraceCallable<T>() {
        @Override
        public T callJob() {
          try {
            // Register the thread to the phaser for main thread to wait for it to complete.
            if (phaser.register() < 0) {
              return null;
            }
            return taskFunc.apply(index);
          } finally {
            phaser.arriveAndDeregister();
          }
        }
      }));
    }
    try {
      // Check deadline while waiting for the task results.
      for (Future<T> future : futures) {
        T taskResult = future.get(deadlineInMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        resCollector.accept(taskResult);
      }
    } catch (Exception e) {
      errHandler.accept(e);
    } finally {
      // Cancel all ongoing jobs
      for (Future<T> future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      // Deregister the main thread and wait for all threads done
      phaser.awaitAdvance(phaser.arriveAndDeregister());
    }
  }
}
