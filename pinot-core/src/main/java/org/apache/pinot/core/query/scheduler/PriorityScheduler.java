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
package org.apache.pinot.core.query.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Schedules queries from a {@link SchedulerGroup} with highest number of tokens on priority
 */
public abstract class PriorityScheduler extends QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PriorityScheduler.class);

  protected final SchedulerPriorityQueue _queryQueue;

  @VisibleForTesting
  protected final Semaphore _runningQueriesSemaphore;
  private final int _numRunners;
  @VisibleForTesting
  Thread _scheduler;

  public PriorityScheduler(PinotConfiguration config, ResourceManager resourceManager, QueryExecutor queryExecutor,
      SchedulerPriorityQueue queue, ServerMetrics metrics, LongAccumulator latestQueryTime) {
    super(config, queryExecutor, resourceManager, metrics, latestQueryTime);
    Preconditions.checkNotNull(queue);
    _queryQueue = queue;
    _numRunners = resourceManager.getNumQueryRunnerThreads();
    _runningQueriesSemaphore = new Semaphore(_numRunners);
  }

  @Override
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    if (!_isRunning) {
      return shuttingDown(queryRequest);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    final SchedulerQueryContext schedQueryContext = new SchedulerQueryContext(queryRequest);
    try {
      _queryQueue.put(schedQueryContext);
    } catch (OutOfCapacityException e) {
      LOGGER.error("Out of capacity for table {}, message: {}", queryRequest.getTableNameWithType(), e.getMessage());
      return outOfCapacity(queryRequest);
    }
    return schedQueryContext.getResultFuture();
  }

  @Override
  public void start() {
    super.start();
    _scheduler = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_isRunning) {
          try {
            _runningQueriesSemaphore.acquire();
          } catch (InterruptedException e) {
            if (!_isRunning) {
              LOGGER.info("Shutting down scheduler");
            } else {
              LOGGER.error("Interrupt while acquiring semaphore. Exiting.", e);
            }
            break;
          }
          try {
            final SchedulerQueryContext request = _queryQueue.take();
            if (request == null) {
              continue;
            }
            ServerQueryRequest queryRequest = request.getQueryRequest();
            final QueryExecutorService executor =
                _resourceManager.getExecutorService(queryRequest, request.getSchedulerGroup());
            ExecutorService innerExecutor = QueryThreadContext.contextAwareExecutorService(executor);
            final ListenableFutureTask<byte[]> queryFutureTask = createQueryFutureTask(queryRequest, innerExecutor);
            queryFutureTask.addListener(new Runnable() {
              @Override
              public void run() {
                executor.releaseWorkers();
                request.getSchedulerGroup().endQuery();
                _runningQueriesSemaphore.release();
                checkStopResourceManager();
                if (!_isRunning && _runningQueriesSemaphore.availablePermits() == _numRunners) {
                  _resourceManager.stop();
                }
              }
            }, MoreExecutors.directExecutor());
            request.setResultFuture(queryFutureTask);
            request.getSchedulerGroup().startQuery();
            queryRequest.getTimerContext().getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT).stopAndRecord();
            _resourceManager.getQueryRunners().submit(queryFutureTask);
          } catch (Throwable t) {
            LOGGER.error(
                "Error in scheduler thread. This is indicative of a bug. Please report this. Server will continue "
                    + "with errors", t);
          }
        }
        if (_isRunning) {
          throw new RuntimeException("FATAL: Scheduler thread is quitting.....something went horribly wrong.....!!!");
        } else {
          failAllPendingQueries();
        }
      }
    });
    _scheduler.setName("scheduler");
    _scheduler.setPriority(Thread.MAX_PRIORITY);
    _scheduler.setDaemon(true);
    _scheduler.start();
  }

  @Override
  public void stop() {
    super.stop();
    // without this, scheduler will never stop if there are no pending queries
    if (_scheduler != null) {
      _scheduler.interrupt();
    }
  }

  synchronized private void failAllPendingQueries() {
    List<SchedulerQueryContext> pending = _queryQueue.drain();
    for (SchedulerQueryContext queryContext : pending) {
      queryContext.setResultFuture(shuttingDown(queryContext.getQueryRequest()));
    }
  }

  private void checkStopResourceManager() {
    if (!_isRunning && _runningQueriesSemaphore.availablePermits() == _numRunners) {
      _resourceManager.stop();
    }
  }
}
