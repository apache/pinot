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
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
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
 * the parallel queue, and then trigger the tenant rebalance job again with the updated context, with an attempt job ID
 * <p>
 * Notice that fundamentally this is not a retry but a resume, since we will not re-do the table rebalance for those
 * tables that have already been processed.
 */
public class TenantRebalanceChecker extends BasePeriodicTask {
  private final static String TASK_NAME = TenantRebalanceChecker.class.getSimpleName();
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantRebalanceChecker.class);
  private final TenantRebalancer _tenantRebalancer;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  // To avoid multiple retries of tenant rebalance job on one controller, we only allow one ongoing retry job at a time.
  private ZkBasedTenantRebalanceObserver _ongoingJobObserver = null;

  public TenantRebalanceChecker(ControllerConf config,
      PinotHelixResourceManager pinotHelixResourceManager, TenantRebalancer tenantRebalancer) {
    super(TASK_NAME, config.getTenantRebalanceCheckerFrequencyInSeconds(),
        config.getTenantRebalanceCheckerInitialDelayInSeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tenantRebalancer = tenantRebalancer;
  }

  @Override
  protected void runTask(Properties periodicTaskProperties) {
    if (_ongoingJobObserver == null || _ongoingJobObserver.isDone()) {
      checkAndRetryTenantRebalance();
    } else {
      LOGGER.info("Skip checking tenant rebalance jobs as there's an ongoing retry job: {} for tenant: {}",
          _ongoingJobObserver.getJobId(), _ongoingJobObserver.getTenantName());
    }
  }

  private void checkAndRetryTenantRebalance() {
    Map<String, Map<String, String>> allJobMetadataByJobId =
        _pinotHelixResourceManager.getAllJobs(Set.of(ControllerJobTypes.TENANT_REBALANCE), x -> true);
    for (Map.Entry<String, Map<String, String>> entry : allJobMetadataByJobId.entrySet()) {
      String jobId = entry.getKey();
      Map<String, String> jobZKMetadata = entry.getValue();

      try {
        // Check if the tenant rebalance job is stuck
        TenantRebalanceContext tenantRebalanceContext =
            TenantRebalanceContext.fromTenantRebalanceJobMetadata(jobZKMetadata);
        TenantRebalanceProgressStats progressStats =
            TenantRebalanceProgressStats.fromTenantRebalanceJobMetadata(jobZKMetadata);
        if (tenantRebalanceContext == null || progressStats == null) {
          // Skip rebalance job: {} as it has no job context or progress stats
          LOGGER.info("Skip checking tenant rebalance job: {} as it has no job context or progress stats", jobId);
          continue;
        }
        long statsUpdatedAt = Long.parseLong(jobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));

        TenantRebalanceContext retryTenantRebalanceContext =
            prepareRetryIfTenantRebalanceJobStuck(jobZKMetadata, tenantRebalanceContext, statsUpdatedAt);
        if (retryTenantRebalanceContext != null) {
          // abort the existing job, then retry with the new job context
          ZkBasedTenantRebalanceObserver observer =
              new ZkBasedTenantRebalanceObserver(jobId, jobZKMetadata.get(CommonConstants.ControllerJob.TENANT_NAME),
                  _pinotHelixResourceManager);
          Pair<List<String>, Boolean> result = observer.cancelJob(false);
          if (result.getRight()) {
            TenantRebalancer.TenantTableRebalanceJobContext ctx;
            while ((ctx = retryTenantRebalanceContext.getOngoingJobsQueue().poll()) != null) {
              TenantRebalancer.TenantTableRebalanceJobContext newCtx =
                  new TenantRebalancer.TenantTableRebalanceJobContext(
                      ctx.getTableName(), UUID.randomUUID().toString(), ctx.shouldRebalanceWithDowntime());
              retryTenantRebalanceContext.getParallelQueue().addFirst(newCtx);
            }
            retryTenantRebalanceJob(retryTenantRebalanceContext, progressStats);
          } else {
            LOGGER.warn("Failed to abort the stuck tenant rebalance job: {}, will not retry", jobId);
          }
          return;
        } else {
          LOGGER.info("Tenant rebalance job: {} is not stuck", jobId);
        }
      } catch (JsonProcessingException e) {
        // If we cannot parse the job metadata, we skip this job
        LOGGER.warn("Failed to parse tenant rebalance context for job: {}, skipping", jobId);
      }
    }
  }

  @VisibleForTesting
  void retryTenantRebalanceJob(TenantRebalanceContext tenantRebalanceContextForRetry,
      TenantRebalanceProgressStats progressStats) {
    ZkBasedTenantRebalanceObserver observer =
        new ZkBasedTenantRebalanceObserver(tenantRebalanceContextForRetry.getJobId(),
            tenantRebalanceContextForRetry.getConfig().getTenantName(),
            progressStats, tenantRebalanceContextForRetry, _pinotHelixResourceManager);
    _ongoingJobObserver = observer;
    _tenantRebalancer.rebalanceWithObserver(observer, tenantRebalanceContextForRetry.getConfig());
  }

  /**
   * Check if the tenant rebalance job is stuck, and prepare to retry it if necessary.
   * A tenant rebalance job is considered stuck if:
   * <ol>
   *   <li>There are no ongoing jobs, but there are jobs in the parallel or sequential queue, and the stats have not
   *   been updated for longer than the heartbeat timeout.</li>
   *   <li>There are ongoing table rebalance jobs, and at least one of them has not updated its status for longer
   *   than the heartbeat timeout.</li>
   * </ol>
   * We cannot simply check whether the tenant rebalance job's metadata was updated within the heartbeat timeout to
   * determine if this tenant rebalance job is stuck, because a tenant rebalance job can have no updates for a
   * longer period if the underlying table rebalance jobs take extra long time to finish, while they individually do
   * update regularly within heartbeat timeout.
   * The retry is prepared by creating a new tenant rebalance job with an incremented attempt ID, and persisting it to
   * ZK.
   *
   * @param jobZKMetadata The ZK metadata of the tenant rebalance job.
   * @param tenantRebalanceContext The context of the tenant rebalance job.
   * @param statsUpdatedAt The timestamp when the stats were last updated.
   * @return The TenantRebalanceContext for retry if the tenant rebalance job is stuck and should be retried, null if
   * the job is not stuck, or it's stuck but other controller has prepared the retry first.
   */
  private TenantRebalanceContext prepareRetryIfTenantRebalanceJobStuck(
      Map<String, String> jobZKMetadata, TenantRebalanceContext tenantRebalanceContext, long statsUpdatedAt) {
    boolean isStuck = false;
    String stuckTableRebalanceJobId = null;
    long heartbeatTimeoutMs = tenantRebalanceContext.getConfig().getHeartbeatTimeoutInMs();
    if (tenantRebalanceContext.getOngoingJobsQueue().isEmpty()) {
      if (!tenantRebalanceContext.getParallelQueue().isEmpty() || !tenantRebalanceContext.getSequentialQueue()
          .isEmpty()) {
        // If there are no ongoing jobs but in parallel or sequential queue, it could be because the tenant rebalancer
        // is in the interval of the previous done job and consumption of the next job. We need to check the heartbeat
        // timeout to be sure that it's actually stuck at this state for a long while.
        isStuck = (System.currentTimeMillis() - statsUpdatedAt >= heartbeatTimeoutMs);
        LOGGER.info(
            "No ongoing table rebalance jobs for tenant: {}, but there are jobs in parallel queue size: {}, "
                + "sequential queue size: {}, stats last updated at: {}, isStuck: {}",
            tenantRebalanceContext.getConfig().getTenantName(), tenantRebalanceContext.getParallelQueue().size(),
            tenantRebalanceContext.getSequentialQueue().size(), statsUpdatedAt, isStuck);
      }
    } else {
      // Check if there's any stuck ongoing table rebalance jobs
      for (TenantRebalancer.TenantTableRebalanceJobContext ctx : new ArrayList<>(
          tenantRebalanceContext.getOngoingJobsQueue())) {
        if (isOngoingTableRebalanceJobStuck(ctx.getJobId(), statsUpdatedAt, heartbeatTimeoutMs)) {
          isStuck = true;
          stuckTableRebalanceJobId = ctx.getJobId();
          break;
        }
      }
    }
    if (isStuck) {
      // If any of the table rebalance jobs is stuck, we consider the tenant rebalance job as stuck.
      // We need to make sure only one controller instance retries the tenant rebalance job.
      TenantRebalanceContext retryTenantRebalanceContext =
          TenantRebalanceContext.forRetry(tenantRebalanceContext.getOriginalJobId(),
              tenantRebalanceContext.getConfig(), tenantRebalanceContext.getAttemptId() + 1,
              tenantRebalanceContext.getParallelQueue(), tenantRebalanceContext.getSequentialQueue(),
              tenantRebalanceContext.getOngoingJobsQueue());
      try {
        jobZKMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT,
            JsonUtils.objectToString(retryTenantRebalanceContext));
        // this returns false if the retryJob already exists in ZK, which means another controller has already
        // prepared the retry, so we should not retry again.
        boolean shouldRetry =
            _pinotHelixResourceManager.addControllerJobToZK(retryTenantRebalanceContext.getJobId(), jobZKMetadata,
                ControllerJobTypes.TENANT_REBALANCE, Objects::isNull);
        if (!shouldRetry) {
          LOGGER.info("Another controller has already prepared the retry job: {} for tenant: {}. Do not retry.",
              stuckTableRebalanceJobId, tenantRebalanceContext.getConfig().getTenantName());
          return null;
        }
        LOGGER.info("Found and prepared to retry stuck table rebalance job: {} for tenant: {}.",
            stuckTableRebalanceJobId, tenantRebalanceContext.getConfig().getTenantName());
        return retryTenantRebalanceContext;
      } catch (JsonProcessingException e) {
        LOGGER.error(
            "Error serialising rebalance context to JSON for updating retryTenantRebalanceContext to ZK {}",
            tenantRebalanceContext.getJobId(), e);
      }
    }
    return null;
  }

  /**
   * Check if the table rebalance job in tenant job's ongoing queue is stuck.
   * A table rebalance job is considered stuck if:
   * <ol>
   * <li> The job metadata does not exist in ZK, and the tenant rebalance job stats have not been updated for longer
   * than the heartbeat timeout.</li>
   * <li> The job metadata exists, but it has not been updated for longer than the heartbeat timeout.</li>
   * </ol>
   * This is different to how we consider a table rebalance job is stuck in
   * {@link org.apache.pinot.controller.helix.core.rebalance.RebalanceChecker}, where we consider a table rebalance
   * job stuck even if it's DONE, ABORTED, FAILED, or CANCELLED for more than heartbeat timeout, because they should
   * have been removed from the ongoing queue once they are DONE or ABORTED etc., if the controller is working properly.
   *
   * @param jobId The ID of the table rebalance job.
   * @param tenantRebalanceJobStatsUpdatedAt The timestamp when the tenant rebalance job stats were last updated.
   * @param heartbeatTimeoutMs The heartbeat timeout in milliseconds.
   * @return True if the table rebalance job is stuck, false otherwise.
   */
  private boolean isOngoingTableRebalanceJobStuck(String jobId, long tenantRebalanceJobStatsUpdatedAt,
      long heartbeatTimeoutMs) {
    Map<String, String> jobMetadata =
        _pinotHelixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TABLE_REBALANCE);
    if (jobMetadata == null) {
      // if the table rebalance job metadata has not created for the table rebalance job id in ongoingJobsQueue
      // longer than heartbeat timeout, the controller may have crashed or restarted
      return System.currentTimeMillis() - tenantRebalanceJobStatsUpdatedAt >= heartbeatTimeoutMs;
    }
    long statsUpdatedAt = Long.parseLong(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));

    if (System.currentTimeMillis() - statsUpdatedAt >= heartbeatTimeoutMs) {
      LOGGER.info("Found stuck table rebalance job: {} that has not updated its status in ZK within "
          + "heartbeat timeout: {}", jobId, heartbeatTimeoutMs);
      return true;
    }
    return false;
  }
}
