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
package org.apache.pinot.core.query.scheduler.resources;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;


public class BoundedAccountingExecutorTest {
  private AtomicInteger running = new AtomicInteger(0);

  private class Syncer {
    CyclicBarrier validationBarrier;
    CyclicBarrier startupBarrier;
  }

  // Created bounded executor with limit 3 and add 5 jobs. Only 3 can be running at a time
  @Test
  public void testBoundsWithinThreadCount()
      throws BrokenBarrierException, InterruptedException {
    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    // Test below relies on jobs > limit
    final int limit = 3;
    final int jobs = 5;
    // we want total threads > limit
    Executor es = Executors.newFixedThreadPool(2 * limit);
    final BoundedAccountingExecutor bes = new BoundedAccountingExecutor(es, limit, accountant);
    final Syncer syncer = new Syncer();
    // barrier parties: all the executables plus 1 for main testing thread
    // startup barrier pauses main thread till all the threads have started
    // validation barrier allows for validation to complete before proceeding further
    syncer.startupBarrier = new CyclicBarrier(limit + 1);
    syncer.validationBarrier = new CyclicBarrier(limit + 1);

    // start adding jobs in new thread. We need to add jobs in new thread
    // because the thread adding jobs is expected to block at limit
    new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < jobs; i++) {
          bes.execute(new Runnable() {
            @Override
            public void run() {
              try {
                running.incrementAndGet();
                syncer.startupBarrier.await();
                syncer.validationBarrier.await();
                running.decrementAndGet();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          });
        }
      }
    }).start();

    syncer.startupBarrier.await();
    // At this point, 'limit' jobs should have executed the startupBarrier.await() call.
    // The other jobs should be waiting on a semaphore for permits inside the BoundedAccountingExecutor.execute()
    // so they have not executed running.incrementAndGet() yet.
    assertEquals(running.get(), limit);
    verify(accountant, times(limit)).incrementThreads();
    // reset will clear the counts on incrementThreads
    reset(accountant);
    final int pendingJobs = jobs - limit;
    // Before the pendingJobs get to startupBarrier, reset it to a different value
    // since we cannot change the limit of the CyclicBarrier once created.
    // The new limit will be pending jobs plus the await we will call in this thread.
    syncer.startupBarrier = new CyclicBarrier(pendingJobs + 1);

    // Now let the running threads complete and call running.decrementAndGet. As
    // they exit, the pending jobs will acquire permits and start to increment
    // the running counter and wait on startupBarrier.await().
    syncer.validationBarrier.await();
    // verify additional jobs are run as soon as current job finishes
    syncer.validationBarrier = new CyclicBarrier(pendingJobs + 1);
    // When we run the test in a small number of cores, it is possible that the running jobs
    // have not yet gotten to execute running.decrementAndGet(), but the pending jobs have already
    // done the increment. So, we need to wait until we check the running counter to equal the
    // pending jobs.
    TestUtils.waitForCondition(aVoid -> running.get() == pendingJobs, 10_000,
        "Invalid number of running jobs" + running.get());

    // Now that there are no jobs running, we can let the new ones in.
    // All the pending jobs will wait on the validationBarrier after we let them pass
    // the startupbarrier below.
    syncer.startupBarrier.await();
    verify(accountant, times(pendingJobs)).incrementThreads();
    syncer.validationBarrier.await();
  }
}

