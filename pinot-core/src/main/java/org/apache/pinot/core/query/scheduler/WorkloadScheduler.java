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
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scheduler implementation that supports query admission control based on workload-specific budgets.
 *
 * <p>This class integrates with the {@link WorkloadBudgetManager} to apply CPU and memory budget enforcement
 * for different workloads, including primary and secondary workloads.</p>
 *
 * <p>Secondary workload configuration is used for queries tagged as "secondary". Queries that exceed their budget
 * will be rejected.</p>
 *
 */
public class WorkloadScheduler extends QueryScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadScheduler.class);

  private final WorkloadBudgetManager _workloadBudgetManager;
  private final ServerMetrics _serverMetrics;
  private String _secondaryWorkloadName;

  public WorkloadScheduler(PinotConfiguration config, QueryExecutor queryExecutor, ServerMetrics metrics,
                           LongAccumulator latestQueryTime, ThreadResourceUsageAccountant resourceUsageAccountant) {
    super(config, queryExecutor, new UnboundedResourceManager(config, resourceUsageAccountant), metrics,
        latestQueryTime);
    _serverMetrics = metrics;
    _workloadBudgetManager = Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
    _secondaryWorkloadName = config.getProperty(CommonConstants.Accounting.CONFIG_OF_SECONDARY_WORKLOAD_NAME,
        CommonConstants.Accounting.DEFAULT_SECONDARY_WORKLOAD_NAME);
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
    // This is for backward compatibility, where we want to honor the isSecondary query option to use the secondary
    // workload budget.
    String workloadName = isSecondary
        ? _secondaryWorkloadName
        : QueryOptionsUtils.getWorkloadName(queryRequest.getQueryContext().getQueryOptions());
    if (!_workloadBudgetManager.canAdmitQuery(workloadName)) {
      // TODO: Explore queuing the query instead of rejecting it.
      String tableName = TableNameBuilder.extractRawTableName(queryRequest.getTableNameWithType());
      LOGGER.warn("Workload budget exceeded for workload: {} query: {} table: {}", workloadName,
          queryRequest.getRequestId(), tableName);
      _serverMetrics.addMeteredValue(workloadName, ServerMeter.WORKLOAD_BUDGET_EXCEEDED, 1L);
      _serverMetrics.addMeteredTableValue(tableName, ServerMeter.WORKLOAD_BUDGET_EXCEEDED, 1L);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.WORKLOAD_BUDGET_EXCEEDED, 1L);
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
