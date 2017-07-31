/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.core.query.executor.QueryExecutor;
import com.linkedin.pinot.core.query.request.ServerQueryRequest;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First Come First Served(FCFS) query scheduler. The FCFS policy applies across all tables.
 * This implementation does not throttle resource utilization. That makes it unsafe in
 * the multi-tenant clusters.
 *
 * This is similar to the existing query scheduling logic.
 */
public class FCFSQueryScheduler extends QueryScheduler {

  private static Logger LOGGER = LoggerFactory.getLogger(FCFSQueryScheduler.class);

  public FCFSQueryScheduler(@Nonnull Configuration schedulerConfig, @Nonnull QueryExecutor queryExecutor, @Nonnull
      ServerMetrics serverMetrics) {
    super(schedulerConfig, queryExecutor, serverMetrics);
  }

  @Override
  public ListenableFuture<byte[]> submit(final ServerQueryRequest queryRequest) {
    Preconditions.checkNotNull(queryRequest);
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    ListenableFutureTask<byte[]> queryTask = getQueryFutureTask(queryRequest);
    queryRunners.submit(queryTask);
    return queryTask;
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public String name() {
    return "fcfs";
  }
}
