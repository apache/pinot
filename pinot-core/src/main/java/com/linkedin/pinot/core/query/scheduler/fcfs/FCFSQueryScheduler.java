/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.scheduler.fcfs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.core.query.executor.QueryExecutor;
import com.linkedin.pinot.core.query.request.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.core.query.scheduler.resources.QueryExecutorService;
import com.linkedin.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;


/**
 * First Come First Served(FCFS) query scheduler. The FCFS policy applies across all tables.
 * This implementation does not throttle resource utilization. That makes it unsafe in
 * the multi-tenant clusters.
 */
public class FCFSQueryScheduler extends QueryScheduler {

  public FCFSQueryScheduler(@Nonnull Configuration config, @Nonnull QueryExecutor queryExecutor,
      @Nonnull ServerMetrics serverMetrics, @Nonnull LongAccumulator latestQueryTime) {
    super(queryExecutor, new UnboundedResourceManager(config), serverMetrics, latestQueryTime);
  }

  @Nonnull
  @Override
  public ListenableFuture<byte[]> submit(@Nonnull ServerQueryRequest queryRequest) {
    if (!isRunning) {
      return immediateErrorResponse(queryRequest, QueryException.SERVER_SCHEDULER_DOWN_ERROR);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    QueryExecutorService queryExecutorService = resourceManager.getExecutorService(queryRequest, null);
    ListenableFutureTask<byte[]> queryTask = createQueryFutureTask(queryRequest, queryExecutorService);
    resourceManager.getQueryRunners().submit(queryTask);
    return queryTask;
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public String name() {
    return "fcfs";
  }
}
