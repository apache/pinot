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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.BinaryWorkloadResourceManager;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This scheduler is designed to deal with two types of workloads
 * 1. Primary Workloads -> regular queries from the application
 * 2. Secondary Workloads -> adhoc queries fired from tools, testing, etc
 *
 *
 * Primary Workload Queries
 * Primary workloads queries are executed with priority and submitted to the Runner threads as and when they arrive.
 * The resources used by a primary workload query is not capped.
 *
 * Secondary Workload Queries
 *   - Secondary workload queries are identified using a query option -> "SET isSecondaryWorkload=true"
 *   - Secondary workload queries are contained as follows:
 *       - Restrictions on number of runner threads available to process secondary queries
 *       - Restrictions on total number of worker threads available to process a single secondary query
 *       - Restrictions on total number of worker threads available to process all in-progress secondary queries
 */
public class BinaryWorkloadScheduler extends QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BinaryWorkloadScheduler.class);

  public static final String MAX_SECONDARY_QUERIES = "binarywlm.maxSecondaryRunnerThreads";
  public static final int DEFAULT_MAX_SECONDARY_QUERIES = 5;

  // Secondary Workload Runners.
  private final int _numSecondaryRunners;
  private final Semaphore _secondaryRunnerSemaphore;

  private final SecondaryWorkloadQueue _secondaryQueryQ;

  Thread _scheduler;

  public BinaryWorkloadScheduler(PinotConfiguration config, QueryExecutor queryExecutor, ServerMetrics metrics,
      LongAccumulator latestQueryTime) {
    super(config, queryExecutor, new BinaryWorkloadResourceManager(config), metrics, latestQueryTime);

    _secondaryQueryQ = new SecondaryWorkloadQueue(config, _resourceManager);
    _numSecondaryRunners = config.getProperty(MAX_SECONDARY_QUERIES, DEFAULT_MAX_SECONDARY_QUERIES);
    LOGGER.info("numSecondaryRunners={}", _numSecondaryRunners);
    _secondaryRunnerSemaphore = new Semaphore(_numSecondaryRunners);
  }

  @Override
  public String name() {
    return "BinaryWorkloadScheduler";
  }

  @Override
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    if (!_isRunning) {
      return shuttingDown(queryRequest);
    }

    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (!QueryOptionsUtils.isSecondaryWorkload(queryRequest.getQueryContext().getQueryOptions())) {
      QueryExecutorService queryExecutorService = _resourceManager.getExecutorService(queryRequest, null);
      ExecutorService innerExecutorService = QueryThreadContext.contextAwareExecutorService(queryExecutorService);
      ListenableFutureTask<byte[]> queryTask = createQueryFutureTask(queryRequest, innerExecutorService);
      _resourceManager.getQueryRunners().submit(queryTask);
      return queryTask;
    }

    final SchedulerQueryContext schedQueryContext = new SchedulerQueryContext(queryRequest);
    try {
      // Update metrics
      _serverMetrics.addMeteredTableValue(queryRequest.getTableNameWithType(), ServerMeter.NUM_SECONDARY_QUERIES, 1L);

      _secondaryQueryQ.put(schedQueryContext);
    } catch (OutOfCapacityException e) {
      LOGGER.error("Out of capacity for query {} table {}, message: {}", queryRequest.getRequestId(),
          queryRequest.getTableNameWithType(), e.getMessage());
      return outOfCapacity(queryRequest);
    } catch (Exception e) {
      // We should not throw any other exception other than OutOfCapacityException. Signal that there's an issue with
      // the scheduler if any other exception is thrown.
      LOGGER.error("Internal error for query {} table {}, message {}", queryRequest.getRequestId(),
          queryRequest.getTableNameWithType(), e.getMessage());
      return shuttingDown(queryRequest);
    }
    return schedQueryContext.getResultFuture();
  }

  @Override
  public void start() {
    super.start();
    _scheduler = getScheduler();
    _scheduler.setName("scheduler");
    // TODO: Considering setting a lower priority to avoid busy loop when all threads are busy processing queries.
    _scheduler.setPriority(Thread.MAX_PRIORITY);
    _scheduler.setDaemon(true);
    _scheduler.start();
  }

  private Thread getScheduler() {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        while (_isRunning) {
          try {
            _secondaryRunnerSemaphore.acquire();
          } catch (InterruptedException e) {
            if (!_isRunning) {
              LOGGER.info("Shutting down scheduler");
            } else {
              LOGGER.error("Interrupt while acquiring semaphore. Exiting.", e);
            }
            break;
          }
          try {
            final SchedulerQueryContext request = _secondaryQueryQ.take();
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
                _secondaryRunnerSemaphore.release();
                checkStopResourceManager();
              }
            }, MoreExecutors.directExecutor());

            // Update metrics
            updateSecondaryWorkloadMetrics(queryRequest);

            request.setResultFuture(queryFutureTask);
            request.getSchedulerGroup().startQuery();
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
  }

  private void updateSecondaryWorkloadMetrics(ServerQueryRequest queryRequest) {
    long timeInQMs = System.currentTimeMillis() - queryRequest.getTimerContext().getQueryArrivalTimeMs();
    _serverMetrics.addTimedTableValue(queryRequest.getTableNameWithType(), ServerTimer.SECONDARY_Q_WAIT_TIME_MS,
        timeInQMs, TimeUnit.MILLISECONDS);
    _serverMetrics.addMeteredTableValue(queryRequest.getTableNameWithType(),
        ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, 1L);
  }

  @Override
  public void stop() {
    super.stop();
    // without this, scheduler will never stop if there are no pending queries
    if (_scheduler != null) {
      _scheduler.interrupt();
    }
  }

  private void checkStopResourceManager() {
    if (!_isRunning && _secondaryRunnerSemaphore.availablePermits() == _numSecondaryRunners) {
      _resourceManager.stop();
    }
  }

  synchronized private void failAllPendingQueries() {
    List<SchedulerQueryContext> pending = _secondaryQueryQ.drain();
    for (SchedulerQueryContext queryContext : pending) {
      ListenableFuture<byte[]> serverShuttingDown = shuttingDown(queryContext.getQueryRequest());
      queryContext.setResultFuture(serverShuttingDown);
    }
  }
}
