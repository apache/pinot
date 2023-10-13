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
package org.apache.pinot.controller.helix.core.rebalance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to check whether a user triggered rebalance job is completed or not, and retry if failed. The retry
 * job is started with the same rebalance configs provided by the user and does best effort to stop the other jobs
 * for the same table. This task can be configured to just check failures and report metrics, and not to do retry.
 */
public class RebalanceChecker extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceChecker.class);
  private static final double RETRY_DELAY_SCALE_FACTOR = 2.0;
  private final ExecutorService _executorService;

  public RebalanceChecker(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    super(RebalanceChecker.class.getSimpleName(), config.getRebalanceCheckerFrequencyInSeconds(),
        config.getRebalanceCheckerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _executorService = executorService;
  }

  protected void processTables(List<String> tableNamesWithType, Properties periodicTaskProperties) {
    int numTables = tableNamesWithType.size();
    LOGGER.info("Processing {} tables in task: {}", numTables, _taskName);
    int numTablesProcessed = 0;
    for (String tableNameWithType : tableNamesWithType) {
      if (!isStarted()) {
        LOGGER.info("Task: {} is stopped, early terminate the task", _taskName);
        break;
      }
      try {
        processTable(tableNameWithType);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing table: {} in task: {}", tableNameWithType, _taskName, e);
        _controllerMetrics.addMeteredTableValue(tableNameWithType + "." + _taskName,
            ControllerMeter.PERIODIC_TASK_ERROR, 1L);
      }
      numTablesProcessed++;
    }
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, _taskName,
        numTablesProcessed);
    LOGGER.info("Finish processing {}/{} tables in task: {}", numTablesProcessed, numTables, _taskName);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    try {
      LOGGER.info("Start to retry rebalance for table: {}", tableNameWithType);
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);
      // Get all rebalance jobs as tracked in ZK for this table to check if there is any failure to retry.
      Map<String, Map<String, String>> allJobMetadata = _pinotHelixResourceManager.getAllJobsForTable(tableNameWithType,
          Collections.singleton(ControllerJobType.TABLE_REBALANCE));
      if (allJobMetadata.isEmpty()) {
        LOGGER.info("No rebalance job has been triggered for table: {}. Skip retry", tableNameWithType);
        return;
      }
      retryRebalanceTable(tableNameWithType, tableConfig, allJobMetadata);
    } catch (Exception e) {
      LOGGER.error("Failed to retry rebalance for table: {}", tableNameWithType, e);
    }
  }

  @VisibleForTesting
  void retryRebalanceTable(String tableNameWithType, TableConfig tableConfig,
      Map<String, Map<String, String>> allJobMetadata)
      throws Exception {
    // Skip retry for the table if rebalance job is still running or has completed, in specific:
    // 1) Skip retry if any rebalance job is actively running. Being actively running means the job is at IN_PROGRESS
    // status, and has updated its status kept in ZK within the heartbeat timeout. It's possible that more than one
    // rebalance jobs are running for the table, but that's fine with idempotent rebalance algorithm.
    // 2) Skip retry if the most recently started rebalance job has completed with DONE or NO_OP. It's possible that
    // jobs started earlier may be still running, but they are ignored here.
    //
    // Otherwise, we can get a list of failed rebalance jobs, i.e. those at FAILED status; or IN_PROGRESS status but
    // haven't updated their status kept in ZK within the heartbeat timeout. For those candidate jobs to retry:
    // 1) Firstly, group them by the original jobIds they retry for so that we can skip those exceeded maxRetry.
    // 2) For the remaining jobs, we take the one started most recently and retry it with its original configs.
    // 3) If configured, we can abort the other rebalance jobs for the table by setting their status to FAILED.
    Map<String/*original jobId*/, Set<Pair<TableRebalanceAttemptContext/*job attempts*/, Long
        /*startTime*/>>> candidateJobs = getCandidateJobs(tableNameWithType, allJobMetadata);
    if (candidateJobs.isEmpty()) {
      LOGGER.info("Found no failed rebalance jobs for table: {}. Skip retry", tableNameWithType);
      return;
    }
    _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.TABLE_REBALANCE_FAILURE_DETECTED, 1L);
    Pair<TableRebalanceAttemptContext, Long> jobContextAndStartTime = getLatestJob(candidateJobs);
    if (jobContextAndStartTime == null) {
      LOGGER.info("Rebalance has been retried too many times for table: {}. Skip retry", tableNameWithType);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.TABLE_REBALANCE_RETRY_TOO_MANY_TIMES,
          1L);
      return;
    }
    TableRebalanceAttemptContext jobCtx = jobContextAndStartTime.getLeft();
    String prevJobId = jobCtx.getJobId();
    RebalanceConfig rebalanceConfig = jobCtx.getConfig();
    long jobStartTimeMs = jobContextAndStartTime.getRight();
    long retryDelayMs = getRetryDelayInMs(rebalanceConfig.getRetryInitialDelayInMs(), jobCtx.getAttemptId());
    if (jobStartTimeMs + retryDelayMs > System.currentTimeMillis()) {
      LOGGER.info("Delay retry for failed rebalance job: {} that started at: {}, by: {}ms", prevJobId, jobStartTimeMs,
          retryDelayMs);
      return;
    }
    tryStopExistingJobs(tableNameWithType, _pinotHelixResourceManager);
    _executorService.submit(() -> {
      // Retry rebalance in another thread as rebalance can take time.
      retryRebalanceTableWithContext(tableNameWithType, tableConfig, jobCtx);
    });
  }

  private void retryRebalanceTableWithContext(String tableNameWithType, TableConfig tableConfig,
      TableRebalanceAttemptContext jobCtx) {
    String prevJobId = jobCtx.getJobId();
    RebalanceConfig rebalanceConfig = jobCtx.getConfig();
    TableRebalanceAttemptContext retryCtx =
        TableRebalanceAttemptContext.forRetry(jobCtx.getOriginalJobId(), rebalanceConfig, jobCtx.getAttemptId() + 1);
    String attemptJobId = retryCtx.getJobId();
    LOGGER.info("Retry rebalance job: {} for table: {} with attempt job: {}", prevJobId, tableNameWithType,
        attemptJobId);
    _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.TABLE_REBALANCE_RETRY, 1L);
    ZkBasedTableRebalanceObserver observer =
        new ZkBasedTableRebalanceObserver(tableNameWithType, attemptJobId, retryCtx, _pinotHelixResourceManager);
    RebalanceResult result =
        _pinotHelixResourceManager.rebalanceTable(tableNameWithType, tableConfig, attemptJobId, rebalanceConfig,
            observer);
    LOGGER.info("New attempt: {} for table: {} is done with result: {}", attemptJobId, tableNameWithType, result);
  }

  @VisibleForTesting
  static long getRetryDelayInMs(long initDelayMs, int attemptId) {
    // TODO: Just exponential backoff by factor 2 for now. Can add other retry polices as needed.
    // The attemptId starts from 1, so minus one as the exponent.
    double minDelayMs = initDelayMs * Math.pow(RETRY_DELAY_SCALE_FACTOR, attemptId - 1);
    double maxDelayMs = minDelayMs * RETRY_DELAY_SCALE_FACTOR;
    return RandomUtils.nextLong((long) minDelayMs, (long) maxDelayMs);
  }

  private static void tryStopExistingJobs(String tableNameWithType,
      PinotHelixResourceManager pinotHelixResourceManager) {
    pinotHelixResourceManager.updateAllJobsForTable(tableNameWithType,
        ZKMetadataProvider.constructPropertyStorePathForControllerJob(ControllerJobType.TABLE_REBALANCE),
        jobMetadata -> updateJobMetadata(tableNameWithType, jobMetadata));
  }

  private static void updateJobMetadata(String tableNameWithType, Map<String, String> jobMetadata) {
    String jobId = jobMetadata.get(CommonConstants.ControllerJob.JOB_ID);
    try {
      String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
      TableRebalanceProgressStats jobStats = JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
      LOGGER.info("Set status of rebalance job: {} for table: {} from: {} to: FAILED", jobId, tableNameWithType,
          jobStats.getStatus());
      jobStats.setStatus(RebalanceResult.Status.FAILED);
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(jobStats));
    } catch (Exception e) {
      LOGGER.error("Failed to update status of rebalance job: {} for table: {}", jobId, tableNameWithType, e);
    }
  }

  @VisibleForTesting
  static Pair<TableRebalanceAttemptContext, Long> getLatestJob(
      Map<String, Set<Pair<TableRebalanceAttemptContext, Long>>> candidateJobs) {
    Pair<TableRebalanceAttemptContext, Long> candidateJobRun = null;
    for (Map.Entry<String, Set<Pair<TableRebalanceAttemptContext, Long>>> entry : candidateJobs.entrySet()) {
      // The job configs from all retry jobs are same, as the same set of job configs is used to do retry.
      // The job metadata kept in ZK is cleaned by submission time order gradually, so we can't compare Set.size()
      // against maxAttempts, but check retryNum of each run to see if retries have exceeded limit.
      Set<Pair<TableRebalanceAttemptContext, Long>> jobRuns = entry.getValue();
      int maxAttempts = jobRuns.iterator().next().getLeft().getConfig().getMaxAttempts();
      Pair<TableRebalanceAttemptContext, Long> latestJobRun = null;
      for (Pair<TableRebalanceAttemptContext, Long> jobRun : jobRuns) {
        if (jobRun.getLeft().getAttemptId() >= maxAttempts) {
          latestJobRun = null;
          break;
        }
        if (latestJobRun == null || latestJobRun.getRight() < jobRun.getRight()) {
          latestJobRun = jobRun;
        }
      }
      if (latestJobRun == null) {
        LOGGER.info("Rebalance job: {} had exceeded maxAttempts: {}. Skip retry", entry.getKey(), maxAttempts);
        continue;
      }
      if (candidateJobRun == null || candidateJobRun.getRight() < latestJobRun.getRight()) {
        candidateJobRun = latestJobRun;
      }
    }
    return candidateJobRun;
  }

  @VisibleForTesting
  static Map<String, Set<Pair<TableRebalanceAttemptContext, Long>>> getCandidateJobs(String tableNameWithType,
      Map<String, Map<String, String>> allJobMetadata)
      throws Exception {
    long nowMs = System.currentTimeMillis();
    Map<String, Set<Pair<TableRebalanceAttemptContext, Long>>> candidates = new HashMap<>();
    // If the job started most recently has already completed, then skip retry for the table.
    Pair<String, Long> latestStartedJob = null;
    Pair<String, Long> latestCompletedJob = null;
    // The processing order of job metadata from the given Map is not deterministic. Track the completed original
    // jobs so that we can simply skip the retry jobs belonging to the completed original jobs.
    Map<String, String> completedOriginalJobs = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : allJobMetadata.entrySet()) {
      String jobId = entry.getKey();
      Map<String, String> jobMetadata = entry.getValue();
      long statsUpdatedAt = Long.parseLong(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));
      String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
      if (StringUtils.isEmpty(jobStatsInStr)) {
        LOGGER.info("Skip rebalance job: {} as it has no job progress stats", jobId);
        continue;
      }
      String jobCtxInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_ATTEMPT_CONTEXT);
      if (StringUtils.isEmpty(jobCtxInStr)) {
        LOGGER.info("Skip rebalance job: {} as it has no job retry configs", jobId);
        continue;
      }
      TableRebalanceProgressStats jobStats = JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
      TableRebalanceAttemptContext jobCtx = JsonUtils.stringToObject(jobCtxInStr, TableRebalanceAttemptContext.class);
      long jobStartTimeMs = jobStats.getStartTimeMs();
      if (latestStartedJob == null || latestStartedJob.getRight() < jobStartTimeMs) {
        latestStartedJob = Pair.of(jobId, jobStartTimeMs);
      }
      String originalJobId = jobCtx.getOriginalJobId();
      RebalanceResult.Status jobStatus = jobStats.getStatus();
      if (jobStatus == RebalanceResult.Status.DONE || jobStatus == RebalanceResult.Status.NO_OP) {
        LOGGER.info("Skip rebalance job: {} as it has completed with status: {}", jobId, jobStatus);
        completedOriginalJobs.put(originalJobId, jobId);
        if (latestCompletedJob == null || latestCompletedJob.getRight() < jobStartTimeMs) {
          latestCompletedJob = Pair.of(jobId, jobStartTimeMs);
        }
        continue;
      }
      if (jobStatus == RebalanceResult.Status.FAILED) {
        LOGGER.info("Found failed rebalance job: {} for original job: {}", jobId, originalJobId);
        candidates.computeIfAbsent(originalJobId, (k) -> new HashSet<>()).add(Pair.of(jobCtx, jobStartTimeMs));
        continue;
      }
      // Check if an IN_PROGRESS job is still actively running.
      long heartbeatTimeoutMs = jobCtx.getConfig().getHeartbeatTimeoutInMs();
      if (nowMs - statsUpdatedAt < heartbeatTimeoutMs) {
        LOGGER.info("Rebalance job: {} is actively running with status updated at: {} within timeout: {}. Skip "
            + "retry for table: {}", jobId, statsUpdatedAt, heartbeatTimeoutMs, tableNameWithType);
        return Collections.emptyMap();
      }
      // The job is considered failed, but it's possible it is still running, then we might end up with more than one
      // rebalance jobs running in parallel for a table. The rebalance algorithm is idempotent, so this should be fine
      // for the correctness.
      LOGGER.info("Found stuck rebalance job: {} for original job: {}", jobId, originalJobId);
      candidates.computeIfAbsent(originalJobId, (k) -> new HashSet<>()).add(Pair.of(jobCtx, jobStartTimeMs));
    }
    if (latestCompletedJob != null && latestCompletedJob.getLeft().equals(latestStartedJob.getLeft())) {
      LOGGER.info("Rebalance job: {} started most recently has already done. Skip retry for table: {}",
          latestCompletedJob.getLeft(), tableNameWithType);
      return Collections.emptyMap();
    }
    for (Map.Entry<String, String> entry : completedOriginalJobs.entrySet()) {
      LOGGER.info("Skip all jobs for original job: {} as it's completed by: {}", entry.getKey(), entry.getValue());
      candidates.remove(entry.getKey());
    }
    return candidates;
  }
}
