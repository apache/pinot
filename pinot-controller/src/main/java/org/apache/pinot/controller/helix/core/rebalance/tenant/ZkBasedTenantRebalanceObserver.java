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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptFailureException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBasedTenantRebalanceObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTenantRebalanceObserver.class);
  private static final int MIN_ZK_UPDATE_RETRY_DELAY_MS = 100;
  private static final int MAX_ZK_UPDATE_RETRY_DELAY_MS = 200;
  public static final int DEFAULT_ZK_UPDATE_MAX_RETRIES = 5;

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final String _jobId;
  private final String _tenantName;
  // Keep track of number of updates. Useful during debugging.
  private final AtomicInteger _numUpdatesToZk;
  private final RetryPolicy _retryPolicy;
  private boolean _isDone;

  private ZkBasedTenantRebalanceObserver(String jobId, String tenantName,
      PinotHelixResourceManager pinotHelixResourceManager, int zkUpdateMaxRetries) {
    _isDone = false;
    _jobId = jobId;
    _tenantName = tenantName;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _numUpdatesToZk = new AtomicInteger(0);
    _retryPolicy = RetryPolicies.randomDelayRetryPolicy(zkUpdateMaxRetries, MIN_ZK_UPDATE_RETRY_DELAY_MS,
        MAX_ZK_UPDATE_RETRY_DELAY_MS);
  }

  private ZkBasedTenantRebalanceObserver(String jobId, String tenantName, TenantRebalanceProgressStats progressStats,
      TenantRebalanceContext tenantRebalanceContext, PinotHelixResourceManager pinotHelixResourceManager,
      int zkUpdateMaxRetries) {
    this(jobId, tenantName, pinotHelixResourceManager, zkUpdateMaxRetries);
    try {
      _retryPolicy.attempt(() -> _pinotHelixResourceManager.addControllerJobToZK(_jobId,
          makeJobMetadata(tenantRebalanceContext, progressStats),
          ControllerJobTypes.TENANT_REBALANCE, Objects::isNull)
      );
    } catch (AttemptFailureException e) {
      LOGGER.error("Error creating initial job metadata in ZK for jobId: {} for tenant rebalance", _jobId, e);
      throw new RuntimeException("Error creating initial job metadata in ZK for jobId: " + _jobId, e);
    }
    _numUpdatesToZk.incrementAndGet();
  }

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName, Set<String> tables,
      TenantRebalanceContext tenantRebalanceContext, PinotHelixResourceManager pinotHelixResourceManager,
      int zkUpdateMaxRetries) {
    this(jobId, tenantName, new TenantRebalanceProgressStats(tables), tenantRebalanceContext,
        pinotHelixResourceManager, zkUpdateMaxRetries);
  }

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName,
      PinotHelixResourceManager pinotHelixResourceManager) {
    this(jobId, tenantName, pinotHelixResourceManager, DEFAULT_ZK_UPDATE_MAX_RETRIES);
  }

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName, TenantRebalanceProgressStats progressStats,
      TenantRebalanceContext tenantRebalanceContext, PinotHelixResourceManager pinotHelixResourceManager) {
    this(jobId, tenantName, progressStats, tenantRebalanceContext, pinotHelixResourceManager,
        DEFAULT_ZK_UPDATE_MAX_RETRIES);
  }

  public void onStart() {
    try {
      updateTenantRebalanceJobMetadataInZk(
          (ctx, progressStats) -> progressStats.setStartTimeMs(System.currentTimeMillis()));
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} on starting tenant rebalance", _jobId, e);
      throw new RuntimeException("Error updating ZK for jobId: " + _jobId + " on starting tenant rebalance", e);
    }
  }

  public void onSuccess(String msg) {
    onFinish(msg);
  }

  public void onError(String errorMsg) {
    onFinish(errorMsg);
  }

  private void onFinish(String msg) {
    try {
      updateTenantRebalanceJobMetadataInZk((ctx, progressStats) -> {
        if (StringUtils.isEmpty(progressStats.getCompletionStatusMsg())) {
          progressStats.setCompletionStatusMsg(msg);
          progressStats.setTimeToFinishInSeconds((System.currentTimeMillis() - progressStats.getStartTimeMs()) / 1000);
        }
      });
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} on successful completion of tenant rebalance", _jobId, e);
      throw new RuntimeException(
          "Error updating ZK for jobId: " + _jobId + " on successful completion of tenant rebalance", e);
    }
    _isDone = true;
  }

  private Map<String, String> makeJobMetadata(TenantRebalanceContext tenantRebalanceContext,
      TenantRebalanceProgressStats progressStats) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.TENANT_NAME, _tenantName);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, _jobId);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TENANT_REBALANCE.name());
    try {
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(progressStats));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising rebalance stats to JSON for persisting to ZK {}", _jobId, e);
    }
    try {
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT,
          JsonUtils.objectToString(tenantRebalanceContext));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising rebalance context to JSON for persisting to ZK {}", _jobId, e);
    }
    return jobMetadata;
  }

  public TenantRebalancer.TenantTableRebalanceJobContext pollQueue(boolean isParallel) {
    AtomicReference<TenantRebalancer.TenantTableRebalanceJobContext> ret = new AtomicReference<>();
    try {
      updateTenantRebalanceJobMetadataInZk((ctx, progressStats) -> {
        TenantRebalancer.TenantTableRebalanceJobContext polled =
            isParallel ? ctx.getParallelQueue().poll() : ctx.getSequentialQueue().poll();
        if (polled != null) {
          ctx.getOngoingJobsQueue().add(polled);
          String tableName = polled.getTableName();
          String rebalanceJobId = polled.getJobId();
          progressStats.updateTableStatus(tableName, TenantRebalanceProgressStats.TableStatus.REBALANCING.name());
          progressStats.putTableRebalanceJobId(tableName, rebalanceJobId);
        }
        ret.set(polled);
      });
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} while polling from {} queue", _jobId,
          isParallel ? "parallel" : "sequential", e);
      throw new RuntimeException(
          "Error updating ZK for jobId: " + _jobId + " while polling from " + (isParallel ? "parallel" : "sequential")
              + " queue", e);
    }
    return ret.get();
  }

  public TenantRebalancer.TenantTableRebalanceJobContext pollParallel() {
    return pollQueue(true);
  }

  public TenantRebalancer.TenantTableRebalanceJobContext pollSequential() {
    return pollQueue(false);
  }

  public void onTableJobError(TenantRebalancer.TenantTableRebalanceJobContext jobContext, String errorMessage) {
    onTableJobComplete(jobContext, errorMessage);
  }

  public void onTableJobDone(TenantRebalancer.TenantTableRebalanceJobContext jobContext) {
    onTableJobComplete(jobContext, TenantRebalanceProgressStats.TableStatus.DONE.name());
  }

  private void onTableJobComplete(TenantRebalancer.TenantTableRebalanceJobContext jobContext, String message) {
    try {
      updateTenantRebalanceJobMetadataInZk((ctx, progressStats) -> {
        if (ctx.getOngoingJobsQueue().remove(jobContext)) {
          progressStats.updateTableStatus(jobContext.getTableName(), message);
          progressStats.setRemainingTables(progressStats.getRemainingTables() - 1);
        }
      });
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} on completion of table rebalance job: {}", _jobId, jobContext, e);
      throw new RuntimeException(
          "Error updating ZK for jobId: " + _jobId + " on completion of table rebalance job: " + jobContext, e);
    }
  }

  /**
   * Cancel the tenant rebalance job.
   * @param isCancelledByUser true if the cancellation is triggered by user, false if it is triggered by system
   *                           (e.g. tenant rebalance checker retrying a job)
   * @return a pair of "list of TABLE rebalance job IDs that are successfully cancelled" and "whether the TENANT
   * rebalance
   * job cancellation is successful"
   */
  public Pair<List<String>, Boolean> cancelJob(boolean isCancelledByUser) {
    List<String> cancelledJobs = new ArrayList<>();
    try {
      // Empty the queues first to prevent any new jobs from being picked up.
      updateTenantRebalanceJobMetadataInZk((tenantRebalanceContext, progressStats) -> {
        TenantRebalancer.TenantTableRebalanceJobContext ctx;
        while ((ctx = tenantRebalanceContext.getParallelQueue().poll()) != null) {
          progressStats.getTableStatusMap()
              .put(ctx.getTableName(), TenantRebalanceProgressStats.TableStatus.NOT_SCHEDULED.name());
        }
        while ((ctx = tenantRebalanceContext.getSequentialQueue().poll()) != null) {
          progressStats.getTableStatusMap()
              .put(ctx.getTableName(), TenantRebalanceProgressStats.TableStatus.NOT_SCHEDULED.name());
        }
      });
      // Try to cancel ongoing jobs with best efforts. There could be some ongoing jobs that are marked cancelled but
      // was completed if table rebalance completed right after TableRebalanceManager marked it.
      updateTenantRebalanceJobMetadataInZk((tenantRebalanceContext, progressStats) -> {
        TenantRebalancer.TenantTableRebalanceJobContext ctx;
        while ((ctx = tenantRebalanceContext.getOngoingJobsQueue().poll()) != null) {
          cancelledJobs.addAll(TableRebalanceManager.cancelRebalance(ctx.getTableName(), _pinotHelixResourceManager,
              isCancelledByUser ? RebalanceResult.Status.CANCELLED : RebalanceResult.Status.ABORTED));
          progressStats.getTableStatusMap()
              .put(ctx.getTableName(), isCancelledByUser ? TenantRebalanceProgressStats.TableStatus.CANCELLED.name()
                  : TenantRebalanceProgressStats.TableStatus.ABORTED.name());
        }
        progressStats.setRemainingTables(0);
        progressStats.setCompletionStatusMsg(
            "Tenant rebalance job has been " + (isCancelledByUser ? "cancelled." : "aborted."));
        progressStats.setTimeToFinishInSeconds((System.currentTimeMillis() - progressStats.getStartTimeMs()) / 1000);
      });
      return Pair.of(cancelledJobs, true);
    } catch (AttemptFailureException e) {
      return Pair.of(cancelledJobs, false);
    }
  }

  private void updateTenantRebalanceJobMetadataInZk(
      BiConsumer<TenantRebalanceContext, TenantRebalanceProgressStats> updater)
      throws AttemptFailureException {
    _retryPolicy.attempt(() -> {
      Map<String, String> jobMetadata =
          _pinotHelixResourceManager.getControllerJobZKMetadata(_jobId, ControllerJobTypes.TENANT_REBALANCE);
      if (jobMetadata == null) {
        LOGGER.warn("Skip updating ZK since job metadata is not present in ZK for jobId: {}", _jobId);
        return false;
      }
      TenantRebalanceContext originalContext = TenantRebalanceContext.fromTenantRebalanceJobMetadata(jobMetadata);
      TenantRebalanceProgressStats originalStats =
          TenantRebalanceProgressStats.fromTenantRebalanceJobMetadata(jobMetadata);
      if (originalContext == null || originalStats == null) {
        LOGGER.warn("Skip updating ZK since rebalance context or progress stats is not present in ZK for jobId: {}",
            _jobId);
        return false;
      }
      TenantRebalanceContext updatedContext = new TenantRebalanceContext(originalContext);
      TenantRebalanceProgressStats updatedStats = new TenantRebalanceProgressStats(originalStats);
      updater.accept(updatedContext, updatedStats);
      boolean updateSuccessful =
          _pinotHelixResourceManager.addControllerJobToZK(_jobId, makeJobMetadata(updatedContext, updatedStats),
              ControllerJobTypes.TENANT_REBALANCE, prevJobMetadata -> {
                try {
                  TenantRebalanceContext prevContext =
                      TenantRebalanceContext.fromTenantRebalanceJobMetadata(prevJobMetadata);
                  TenantRebalanceProgressStats prevStats =
                      TenantRebalanceProgressStats.fromTenantRebalanceJobMetadata(prevJobMetadata);
                  if (prevContext == null || prevStats == null) {
                    LOGGER.warn(
                        "Failed to update ZK since rebalance context or progress stats was removed in ZK for "
                            + "jobId: {}", _jobId);
                    return false;
                  }
                  return prevContext.equals(originalContext) && prevStats.equals(originalStats);
                } catch (JsonProcessingException e) {
                  LOGGER.error("Error deserializing rebalance context from ZK for jobId: {}", _jobId, e);
                  return false;
                }
              });
      if (updateSuccessful) {
        return true;
      } else {
        LOGGER.warn(
            "Tenant rebalance context or progress stats is out of sync with ZK while polling, fetching the latest "
                + "context and progress stats from ZK and retry. jobId: {}", _jobId);
        return false;
      }
    });
    LOGGER.debug("Number of updates to Zk: {} for rebalanceJob: {}  ", _numUpdatesToZk.incrementAndGet(), _jobId);
  }

  public boolean isDone() {
    return _isDone;
  }

  public String getJobId() {
    return _jobId;
  }

  public String getTenantName() {
    return _tenantName;
  }

  @VisibleForTesting
  void setDone(boolean isDone) {
    _isDone = isDone;
  }
}
