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
import io.netty.util.internal.StringUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptFailureException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBasedTenantRebalanceObserver implements TenantRebalanceObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTenantRebalanceObserver.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final String _jobId;
  private final String _tenantName;
  // Keep track of number of updates. Useful during debugging.
  private final AtomicInteger _numUpdatesToZk;
  private boolean _isDone;

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName, TenantRebalanceProgressStats progressStats,
      TenantRebalanceContext tenantRebalanceContext,
      PinotHelixResourceManager pinotHelixResourceManager) {
    _isDone = false;
    _jobId = jobId;
    _tenantName = tenantName;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixResourceManager.addControllerJobToZK(_jobId, makeJobMetadata(tenantRebalanceContext, progressStats),
        ControllerJobTypes.TENANT_REBALANCE);
    _numUpdatesToZk = new AtomicInteger(1);
  }

  public ZkBasedTenantRebalanceObserver(String jobId, String tenantName, Set<String> tables,
      TenantRebalanceContext tenantRebalanceContext,
      PinotHelixResourceManager pinotHelixResourceManager) {
    this(jobId, tenantName, new TenantRebalanceProgressStats(tables), tenantRebalanceContext,
        pinotHelixResourceManager);
  }

  @Override
  public void onTrigger(Trigger trigger, String tableName, String description) {
  }

  public void onStart() {
    try {
      updateTenantRebalanceContextInZk(
          (ctx, progressStats) -> progressStats.setStartTimeMs(System.currentTimeMillis()));
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} on starting tenant rebalance", _jobId, e);
      throw new RuntimeException("Error updating ZK for jobId: " + _jobId + " on starting tenant rebalance", e);
    }
  }

  @Override
  public void onSuccess(String msg) {
    onFinish(msg);
  }

  @Override
  public void onError(String errorMsg) {
    onFinish(errorMsg);
  }

  private void onFinish(String msg) {
    try {
      updateTenantRebalanceContextInZk((ctx, progressStats) -> {
        if (StringUtil.isNullOrEmpty(progressStats.getCompletionStatusMsg())) {
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
    final TenantRebalancer.TenantTableRebalanceJobContext[] ret =
        new TenantRebalancer.TenantTableRebalanceJobContext[1];
    try {
      updateTenantRebalanceContextInZk((ctx, progressStats) -> {
        TenantRebalancer.TenantTableRebalanceJobContext polled =
            isParallel ? ctx.getParallelQueue().poll() : ctx.getSequentialQueue().poll();
        if (polled != null) {
          ctx.getOngoingJobsQueue().add(polled);
          String tableName = polled.getTableName();
          String rebalanceJobId = polled.getJobId();
          progressStats.updateTableStatus(tableName, TenantRebalanceProgressStats.TableStatus.REBALANCING.name());
          progressStats.putTableRebalanceJobId(tableName, rebalanceJobId);
        }
        ret[0] = polled;
      });
    } catch (AttemptFailureException e) {
      LOGGER.error("Error updating ZK for jobId: {} while polling from {} queue", _jobId,
          isParallel ? "parallel" : "sequential", e);
      throw new RuntimeException(
          "Error updating ZK for jobId: " + _jobId + " while polling from " + (isParallel ? "parallel" : "sequential")
              + " queue", e);
    }
    return ret[0];
  }

  public TenantRebalancer.TenantTableRebalanceJobContext pollParallel() {
    return pollQueue(true);
  }

  public TenantRebalancer.TenantTableRebalanceJobContext pollSequential() {
    return pollQueue(false);
  }

  public void onJobError(TenantRebalancer.TenantTableRebalanceJobContext jobContext, String errorMessage) {
    onJobComplete(jobContext, errorMessage);
  }

  public void onJobDone(TenantRebalancer.TenantTableRebalanceJobContext jobContext) {
    onJobComplete(jobContext, TenantRebalanceProgressStats.TableStatus.DONE.name());
  }

  private void onJobComplete(TenantRebalancer.TenantTableRebalanceJobContext jobContext, String message) {
    try {
      updateTenantRebalanceContextInZk((ctx, progressStats) -> {
        ctx.getOngoingJobsQueue().remove(jobContext);
        if (progressStats.getTableStatusMap()
            .get(jobContext.getTableName())
            .equals(TenantRebalanceProgressStats.TableStatus.REBALANCING.name())) {
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

  private void updateTenantRebalanceContextInZk(
      BiConsumer<TenantRebalanceContext, TenantRebalanceProgressStats> updater)
      throws AttemptFailureException {
    RetryPolicy retry = RetryPolicies.fixedDelayRetryPolicy(3, 100);
    retry.attempt(() -> {
      Map<String, String> jobMetadata =
          _pinotHelixResourceManager.getControllerJobZKMetadata(_jobId, ControllerJobTypes.TENANT_REBALANCE);
      if (jobMetadata == null) {
        return false;
      }
      TenantRebalanceContext originalContext = TenantRebalanceContext.fromTenantRebalanceJobMetadata(jobMetadata);
      TenantRebalanceProgressStats originalStats =
          TenantRebalanceProgressStats.fromTenantRebalanceJobMetadata(jobMetadata);
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
                  return prevContext.equals(originalContext) && prevStats.equals(originalStats);
                } catch (JsonProcessingException e) {
                  LOGGER.error("Error deserializing rebalance context from ZK for jobId: {}", _jobId, e);
                  return false;
                }
              });
      if (updateSuccessful) {
        return true;
      } else {
        LOGGER.info(
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
