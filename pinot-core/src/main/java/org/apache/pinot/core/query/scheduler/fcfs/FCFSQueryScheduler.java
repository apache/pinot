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
package org.apache.pinot.core.query.scheduler.fcfs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.ThrottlingRuntime;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * First Come First Served(FCFS) query scheduler. The FCFS policy applies across all tables.
 * This implementation does not throttle resource utilization. That makes it unsafe in
 * the multi-tenant clusters.
 */
public class FCFSQueryScheduler extends QueryScheduler {

  public FCFSQueryScheduler(PinotConfiguration config, String instanceId, QueryExecutor queryExecutor,
      ThreadAccountant threadAccountant, LongAccumulator latestQueryTime) {
    super(config, instanceId, queryExecutor, threadAccountant, latestQueryTime, new UnboundedResourceManager(config));
  }

  @Override
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    if (!_isRunning) {
      return shuttingDown(queryRequest);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    // Global runtime throttling gate
    ThrottlingRuntime.acquireSchedulerPermit();
    QueryExecutorService executorService = _resourceManager.getExecutorService(queryRequest, null);
    ListenableFutureTask<byte[]> queryTask = createQueryFutureTask(queryRequest, executorService);
    _resourceManager.getQueryRunners().submit(queryTask);
    queryTask.addListener(() -> {
      // nothing else to release here (no scheduler group in FCFS), but ensure we free the global permit
      ThrottlingRuntime.releaseSchedulerPermit();
    }, MoreExecutors.directExecutor());
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
    return "FCFS";
  }
}
