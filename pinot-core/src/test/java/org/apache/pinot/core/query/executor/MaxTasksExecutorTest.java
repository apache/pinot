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
package org.apache.pinot.core.query.executor;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class MaxTasksExecutorTest {

  private static final int MAX_TASKS = 5;

  @Test
  public void testExecutor() {
    MaxTasksExecutor ex = new MaxTasksExecutor(MAX_TASKS, Executors.newCachedThreadPool());

    final Semaphore sem1 = new Semaphore(0);
    final Semaphore sem2 = new Semaphore(0);
    final Semaphore sem3 = new Semaphore(0);

    for (int i = 1; i <= MAX_TASKS; i++) {
      ex.execute(() -> {
        sem2.release();
        sem1.acquireUninterruptibly();
        sem3.release();
      });
      sem2.acquire();
    }

    try {
      ex.execute(() -> {
      });
      fail("Should not allow more than " + MAX_TASKS + " threads");
    } catch (Exception e) {
      // as expected
    }

    for (int i = MAX_TASKS; i > 0; i--) {
      sem1.release();
    }

    try {
      sem3.tryAcquire(MAX_TASKS, 10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted waiting for tasks to complete");
    }

    try {
      ex.execute(() -> {
      });
    } catch (Exception e) {
      fail("Exception submitting task after release: " + e);
    }
  }
}
