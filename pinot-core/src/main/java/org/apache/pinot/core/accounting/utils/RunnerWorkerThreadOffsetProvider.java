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
package org.apache.pinot.core.accounting.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * map each thread to a unique id, starting from zero, used as its offset to access
 * cputime/memory/job status, etc
 */
public class RunnerWorkerThreadOffsetProvider {

  private final int _numQueryRunnerThreads;
  // Thread local variable containing each thread's ID
  private final AtomicInteger _atomicInteger = new AtomicInteger(0);
  private final ThreadLocal<Integer> _threadId;
  public RunnerWorkerThreadOffsetProvider(int numQueryRunnerThreads) {
    _numQueryRunnerThreads = numQueryRunnerThreads;
    _threadId = ThreadLocal.withInitial(() -> {
          String threadName = Thread.currentThread().getName();
          if (threadName.startsWith(CommonConstants.ExecutorService.PINOT_QUERY_RUNNER_NAME_PREFIX)) {
            return Integer.parseInt(
                threadName.substring(CommonConstants.ExecutorService.PINOT_QUERY_RUNNER_NAME_PREFIX.length()));
          } else if (threadName.startsWith(CommonConstants.ExecutorService.PINOT_QUERY_WORKER_NAME_PREFIX)) {
            return Integer.parseInt(
                threadName.substring(CommonConstants.ExecutorService.PINOT_QUERY_WORKER_NAME_PREFIX.length()))
                + _numQueryRunnerThreads;
          } else {
            return _atomicInteger.getAndIncrement();
          }
        }
    );
  }

  @VisibleForTesting
  public void reset() {
    _atomicInteger.set(0);
  }

  // Returns the current thread's unique ID, assigning it if necessary
  public int get() {
    return _threadId.get();
  }
}
