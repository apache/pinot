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

import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Bounded accounting executor service that puts an upper bound on the
 * number of concurrent jobs that can be executed using this service. This service
 * allows us to divide the resources (threads) of an existing executor service between
 * different clients and limit the concurrent executions each client can run.
 *
 * This class also supports a resource accounting interface to accurately track resources
 * utilization based on submission time and end time of a task. This does not require
 * any changes to client code which continue to use ExecutorService interface.
 */
public class BoundedAccountingExecutor extends QueryExecutorService {
  private static Logger LOGGER = LoggerFactory.getLogger(BoundedAccountingExecutor.class);

  private final Executor delegateExecutor;
  private final int bounds;
  private Semaphore semaphore;
  private final SchedulerGroupAccountant accountant;

  public BoundedAccountingExecutor(@Nonnull Executor s, int bounds, @Nonnull SchedulerGroupAccountant accountant) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(accountant);
    Preconditions.checkArgument(bounds > 0);
    this.delegateExecutor = s;
    this.bounds = bounds;
    this.semaphore = new Semaphore(bounds);
    this.accountant = accountant;
  }

  @Override
  public void execute(Runnable command) {
    delegateExecutor.execute(toAccountingRunnable(command));
  }

  @Override
  public void releaseWorkers() {
    accountant.releasedReservedThreads(bounds);
  }

  private QueryAccountingRunnable toAccountingRunnable(Runnable runnable) {
    acquirePermits(1);
    return new QueryAccountingRunnable(runnable, semaphore, accountant);
  }

  private void acquirePermits(int permits) {
    try {
      semaphore.acquire(permits);
    } catch (InterruptedException e) {
      LOGGER.error("Thread interrupted while waiting for semaphore", e);
      throw new RuntimeException(e);
    }
  }

  private class QueryAccountingRunnable implements Runnable {
    private final Runnable runnable;
    private final Semaphore semaphore;
    private final SchedulerGroupAccountant accountant;

    QueryAccountingRunnable(Runnable r, Semaphore semaphore, SchedulerGroupAccountant accountant) {
      this.runnable = r;
      this.semaphore = semaphore;
      this.accountant = accountant;
    }

    @Override
    public void run() {
      try {
        if (accountant != null) {
          accountant.incrementThreads();
        }
        runnable.run();
      } finally {
        if (accountant != null) {
          accountant.decrementThreads();
        }
        semaphore.release();
      }
    }
  }
}
