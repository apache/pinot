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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceContext;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceProgressStats;
import org.apache.pinot.controller.validation.UtilizationChecker;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to check if tenant rebalance jobs are stuck and retry them. Controller crashes or restarts could
 * make a tenant rebalance job stuck.
 * The task checks for each tenant rebalance job's metadata in ZK, look at the TenantTableRebalanceJobContext in
 * ongoingJobsQueue, which is a list with table rebalance job ids that the controller is currently processing, to see
 * if any of these table rebalance jobs has not updated their progress stats longer than the configured heartbeat
 * timeout. If so, the tenant rebalance job is considered stuck, and the task will resume the tenant rebalance job by
 * aborting all the ongoing table rebalance jobs, move the ongoing TenantTableRebalanceJobContext back to the head of
 * the parallel queue, and then trigger the tenant rebalance job again with the updated context.
 * <p>
 * Notice that this is not a retry but a resume, since we will not re-do the table rebalance for those tables that have
 * already been processed.
 */
public class TenantRebalanceChecker extends BasePeriodicTask {
  private final static String TASK_NAME = TenantRebalanceChecker.class.getSimpleName();
  public final static Integer DEFAULT_FREQUENCY_IN_SECONDS = 300;
  public final static Integer DEFAULT_INITIAL_DELAY = 300;
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantRebalanceChecker.class);
  private static final double RETRY_DELAY_SCALE_FACTOR = 2.0;
  private final TenantRebalancer _tenantRebalancer;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantRebalanceChecker(ControllerConf config, PoolingHttpClientConnectionManager connectionManager,
      ControllerMetrics controllerMetrics, List<UtilizationChecker> utilizationCheckers, Executor executor,
      PinotHelixResourceManager pinotHelixResourceManager, TenantRebalancer tenantRebalancer) {
    super(TASK_NAME, DEFAULT_FREQUENCY_IN_SECONDS, DEFAULT_INITIAL_DELAY);
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tenantRebalancer = tenantRebalancer;
  }

  @Override
  protected void runTask(Properties periodicTaskProperties) {
    checkAndRetryTenantRebalance();
  }

  private void checkAndRetryTenantRebalance() {
    Map<String, Map<String, String>> allJobMetadataByJobId =
        _pinotHelixResourceManager.getAllJobs(Set.of(ControllerJobTypes.TENANT_REBALANCE), x -> true);
    for (Map.Entry<String, Map<String, String>> entry : allJobMetadataByJobId.entrySet()) {
      String jobId = entry.getKey();
      Map<String, String> jobZKMetadata = entry.getValue();

      try {
        // Check if the tenant rebalance job is stuck
        DefaultTenantRebalanceContext tenantRebalanceContext =
            JsonUtils.stringToObject(jobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT),
                DefaultTenantRebalanceContext.class);
        TenantRebalanceProgressStats progressStats =
            JsonUtils.stringToObject(jobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
                TenantRebalanceProgressStats.class);
        long statsUpdatedAt = Long.parseLong(jobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));

        if (isTenantRebalanceJobStuck(tenantRebalanceContext, statsUpdatedAt)) {
          for (TenantRebalancer.TenantTableRebalanceJobContext ctx;
              (ctx = tenantRebalanceContext.getOngoingJobsQueue().poll()) != null; ) {
            abortTableRebalanceJob(ctx.getTableName());
            tenantRebalanceContext.getParallelQueue().addFirst(ctx);
          }
          // If the job is stuck, we retry it
          resumeTenantRebalanceJob(tenantRebalanceContext, progressStats);
        } else {
          LOGGER.info("Tenant rebalance job: {} is not stuck", jobId);
        }
      } catch (JsonProcessingException e) {
        // If we cannot parse the job metadata, we skip this job
        LOGGER.warn("Failed to parse tenant rebalance context for job: {}, skipping", jobId);
      }
    }
  }

  private void resumeTenantRebalanceJob(DefaultTenantRebalanceContext tenantRebalanceContext,
      TenantRebalanceProgressStats progressStats) {
    DefaultTenantRebalanceContext tenantRebalanceContextForRetry =
        DefaultTenantRebalanceContext.forRetry(tenantRebalanceContext.getOriginalJobId(),
            tenantRebalanceContext.getConfig(), tenantRebalanceContext.getAttemptId() + 1,
            tenantRebalanceContext.getParallelQueue(),
            tenantRebalanceContext.getSequentialQueue(), tenantRebalanceContext.getOngoingJobsQueue());

    ZkBasedTenantRebalanceObserver observer =
        new ZkBasedTenantRebalanceObserver(tenantRebalanceContext.getJobId(),
            tenantRebalanceContext.getConfig().getTenantName(),
            progressStats, tenantRebalanceContextForRetry, _pinotHelixResourceManager);
    ((DefaultTenantRebalancer) _tenantRebalancer).rebalanceWithContext(tenantRebalanceContextForRetry, observer);
  }

  private boolean isTenantRebalanceJobStuck(
      DefaultTenantRebalanceContext tenantRebalanceContext, long statsUpdatedAt) {
    if (tenantRebalanceContext.getOngoingJobsQueue().isEmpty()) {
      if (tenantRebalanceContext.getParallelQueue().isEmpty() && tenantRebalanceContext.getSequentialQueue()
          .isEmpty()) {
        return false;
      }
      // If there are no ongoing jobs but in parallel or sequential queue, it could be because the tenant rebalancer
      // is in the interval of the previous done job and consumption of the next job. We need to check the heartbeat
      // timeout to be sure that it's actually stuck at this state for a long while.
      long heartbeatTimeoutMs = tenantRebalanceContext.getConfig().getHeartbeatTimeoutInMs();
      return (System.currentTimeMillis() - statsUpdatedAt >= heartbeatTimeoutMs);
    }

    // Check if there's any stuck ongoing table rebalance jobs
    for (TenantRebalancer.TenantTableRebalanceJobContext ctx : new ArrayList<>(
        tenantRebalanceContext.getOngoingJobsQueue())) {
      if (isTableRebalanceJobStuck(ctx.getJobId())) {
        // If any of the table rebalance jobs is stuck, we consider the tenant rebalance job as stuck.
        LOGGER.info("Found stuck table rebalance job: {} for tenant: {}", ctx.getJobId(),
            tenantRebalanceContext.getConfig().getTenantName());
        return true;
      }
    }
    return false;
  }

  private boolean isTableRebalanceJobStuck(String jobId) {
    Map<String, String> jobMetadata =
        _pinotHelixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TABLE_REBALANCE);
    if (jobMetadata == null) {
      return false;
    }
    long statsUpdatedAt = Long.parseLong(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));
    String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
    if (StringUtils.isEmpty(jobStatsInStr)) {
      // Skip rebalance job as it has no job progress stats
      return false;
    }
    String jobCtxInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT);
    if (StringUtils.isEmpty(jobCtxInStr)) {
      // Skip rebalance job: {} as it has no job context
      return false;
    }

    try {
      TableRebalanceProgressStats jobStats = JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
      TableRebalanceContext jobCtx = JsonUtils.stringToObject(jobCtxInStr, TableRebalanceContext.class);
      if (jobStats.getStatus() == RebalanceResult.Status.IN_PROGRESS) {
        long heartbeatTimeoutMs = jobCtx.getConfig().getHeartbeatTimeoutInMs();
        if (System.currentTimeMillis() - statsUpdatedAt >= heartbeatTimeoutMs) {
          LOGGER.info("Found stuck rebalance job: {} that has not updated its status in ZK within "
              + "heartbeat timeout: {}", jobId, heartbeatTimeoutMs);
          return true;
        }
      }
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to parse table rebalance context for job: " + jobId,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return false;
  }

  private void abortTableRebalanceJob(String tableNameWithType) {
    // TODO: This is a duplicate of a private method in RebalanceChecker, we should refactor it to a common place.
    boolean updated =
        _pinotHelixResourceManager.updateJobsForTable(tableNameWithType, ControllerJobTypes.TABLE_REBALANCE,
            jobMetadata -> {
              String jobId = jobMetadata.get(CommonConstants.ControllerJob.JOB_ID);
              try {
                String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
                TableRebalanceProgressStats jobStats =
                    JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
                if (jobStats.getStatus() != RebalanceResult.Status.IN_PROGRESS) {
                  return;
                }
                LOGGER.info("Abort rebalance job: {} for table: {}", jobId, tableNameWithType);
                jobStats.setStatus(RebalanceResult.Status.ABORTED);
                jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
                    JsonUtils.objectToString(jobStats));
              } catch (Exception e) {
                LOGGER.error("Failed to abort rebalance job: {} for table: {}", jobId, tableNameWithType, e);
              }
            });
    LOGGER.info("Tried to abort existing jobs at best effort and done: {}", updated);
  }
}
