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


/**
 * map each thread to a unique id, starting from zero, used as its offset to access
 * cputime/memory/job status, etc
 */
public class RunnerWorkerThreadOffsetProvider {

  // Thread local variable containing each thread's ID
  private final AtomicInteger _atomicInteger = new AtomicInteger(0);
  private final ThreadLocal<Integer> _threadId = ThreadLocal.withInitial(_atomicInteger::getAndIncrement);

  public RunnerWorkerThreadOffsetProvider() {
  }

  @VisibleForTesting
  public void reset() {
    _atomicInteger.set(0);
  }

  // TODO: make this not dependent on numRunnerThreads
  /**
   * Returns the current thread's unique ID, assigning it if necessary
   */
  public int get() {
    return _threadId.get();
  }
}
