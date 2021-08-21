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
  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedAccountingExecutor.class);

  private final Executor _delegateExecutor;
  private final int _bounds;
  private Semaphore _semaphore;
  private final SchedulerGroupAccountant _accountant;

  public BoundedAccountingExecutor(@Nonnull Executor s, int bounds, @Nonnull SchedulerGroupAccountant accountant) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(accountant);
    Preconditions.checkArgument(bounds > 0);
    _delegateExecutor = s;
    _bounds = bounds;
    _semaphore = new Semaphore(bounds);
    _accountant = accountant;
  }

  @Override
  public void execute(Runnable command) {
    _delegateExecutor.execute(toAccountingRunnable(command));
  }

  @Override
  public void releaseWorkers() {
    _accountant.releasedReservedThreads(_bounds);
  }

  private QueryAccountingRunnable toAccountingRunnable(Runnable runnable) {
    acquirePermits(1);
    return new QueryAccountingRunnable(runnable, _semaphore, _accountant);
  }

  private void acquirePermits(int permits) {
    try {
      _semaphore.acquire(permits);
    } catch (InterruptedException e) {
      LOGGER.error("Thread interrupted while waiting for semaphore", e);
      throw new RuntimeException(e);
    }
  }

  private class QueryAccountingRunnable implements Runnable {
    private final Runnable _runnable;
    private final Semaphore _semaphore;
    private final SchedulerGroupAccountant _accountant;

    QueryAccountingRunnable(Runnable r, Semaphore semaphore, SchedulerGroupAccountant accountant) {
      _runnable = r;
      _semaphore = semaphore;
      _accountant = accountant;
    }

    @Override
    public void run() {
      try {
        if (_accountant != null) {
          _accountant.incrementThreads();
        }
        _runnable.run();
      } finally {
        if (_accountant != null) {
          _accountant.decrementThreads();
        }
        _semaphore.release();
      }
    }
  }
}
