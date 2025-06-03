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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.accounting.WorkloadBudgetManager;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.WorkloadResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadScheduler extends QueryScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadScheduler.class);

  private final WorkloadBudgetManager _workloadBudgetManager;
  private String _secondaryWorkloadName;

  public WorkloadScheduler(PinotConfiguration config, QueryExecutor queryExecutor, ServerMetrics metrics,
                           LongAccumulator latestQueryTime) {
    super(config, queryExecutor, new WorkloadResourceManager(config), metrics, latestQueryTime);
    _workloadBudgetManager = WorkloadBudgetManager.getInstance();
    initSecondaryWorkload(config);
  }

  private void initSecondaryWorkload(PinotConfiguration config) {
    _secondaryWorkloadName = config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_SECONDARY_WORKLOAD_NAME,
        CommonConstants.Accounting.DEFAULT_SECONDARY_WORKLOAD_NAME);

    double secondaryCpuPercentage = config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_SECONDARY_WORKLOAD_CPU_PERCENTAGE,
        CommonConstants.Accounting.DEFAULT_SECONDARY_WORKLOAD_CPU_PERCENTAGE);

    // The Secondary workload budget is common for all secondary queries. Setting the CPU budget based on the
    // CPU percentage allocated for secondary workload and the enforcement window. The memory budget is set to
    // Long.MAX_VALUE for now, since we do not have a specific memory budget for secondary queries.
    long secondaryCpuBudget =
        (long) (secondaryCpuPercentage * _workloadBudgetManager.getEnforcementWindowMs() * 100_000L);

    // TODO: Add memory budget for secondary workload queries
    _workloadBudgetManager.addOrUpdateWorkload(_secondaryWorkloadName, secondaryCpuBudget, Long.MAX_VALUE);
  }

  @Override
  public String name() {
    return "WorkloadScheduler";
  }

  @Override
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    if (!_isRunning) {
      return shuttingDown(queryRequest);
    }

    boolean isSecondary = QueryOptionsUtils.isSecondaryWorkload(queryRequest.getQueryContext().getQueryOptions());
    String workloadName = isSecondary
        ? _secondaryWorkloadName
        : QueryOptionsUtils.getWorkloadName(queryRequest.getQueryContext().getQueryOptions());

    if (!_workloadBudgetManager.canAdmitQuery(workloadName)) {
      LOGGER.warn("Workload budget exceeded for workload: {} query: {} table: {}", workloadName,
          queryRequest.getRequestId(), queryRequest.getTableNameWithType());
      return outOfCapacity(queryRequest);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    QueryExecutorService queryExecutorService = _resourceManager.getExecutorService(queryRequest, null);
    ExecutorService innerExecutorService = QueryThreadContext.contextAwareExecutorService(queryExecutorService);
    ListenableFutureTask<byte[]> queryTask = createQueryFutureTask(queryRequest, innerExecutorService);
    _resourceManager.getQueryRunners().submit(queryTask);
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
}
