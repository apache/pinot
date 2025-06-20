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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.ws.rs.NotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.RebalanceInProgressException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.util.ControllerZkHelixUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Single entry point for all table rebalance related operations. This class should be used to initiate table rebalance
 * operations, rather than directly instantiating objects of {@link TableRebalancer}.
 */
public class TableRebalanceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRebalanceManager.class);

  private final PinotHelixResourceManager _resourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final RebalancePreChecker _rebalancePreChecker;
  private final TableSizeReader _tableSizeReader;
  private final ExecutorService _executorService;

  public TableRebalanceManager(PinotHelixResourceManager resourceManager, ControllerMetrics controllerMetrics,
      RebalancePreChecker rebalancePreChecker, TableSizeReader tableSizeReader, ExecutorService executorService) {
    _resourceManager = resourceManager;
    _controllerMetrics = controllerMetrics;
    _rebalancePreChecker = rebalancePreChecker;
    _tableSizeReader = tableSizeReader;
    _executorService = executorService;
  }

  /**
   * Rebalance the table with the given name and type synchronously. It's the responsibility of the caller to ensure
   * that this rebalance is run on the rebalance thread pool in the controller that respects the configuration
   * {@link org.apache.pinot.controller.ControllerConf#CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS}.
   *
   * @param tableNameWithType name of the table to rebalance
   * @param rebalanceConfig configuration for the rebalance operation
   * @param rebalanceJobId ID of the rebalance job, which is used to track the progress of the rebalance operation
   * @param trackRebalanceProgress whether to track rebalance progress stats in ZK
   * @param allowRetries whether to allow retries for failed or stuck rebalance operations (through
   *                     {@link RebalanceChecker}). Requires {@code trackRebalanceProgress} to be true.
   * @return result of the rebalance operation
   * @throws TableNotFoundException if the table does not exist
   * @throws RebalanceInProgressException if a rebalance job is already in progress for the table (as per ZK metadata)
   */
  public RebalanceResult rebalanceTable(String tableNameWithType, RebalanceConfig rebalanceConfig,
      String rebalanceJobId, boolean trackRebalanceProgress, boolean allowRetries)
      throws TableNotFoundException, RebalanceInProgressException {
    if (allowRetries && !trackRebalanceProgress) {
      throw new IllegalArgumentException(
          "Rebalance retries are only supported when rebalance progress is tracked in ZK");
    }
    TableConfig tableConfig = _resourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new TableNotFoundException("Failed to find table config for table: " + tableNameWithType);
    }
    Preconditions.checkState(rebalanceJobId != null, "RebalanceId not populated in the rebalanceConfig");
    ZkBasedTableRebalanceObserver zkBasedTableRebalanceObserver = null;
    if (trackRebalanceProgress) {
      zkBasedTableRebalanceObserver = new ZkBasedTableRebalanceObserver(tableNameWithType, rebalanceJobId,
          TableRebalanceContext.forInitialAttempt(rebalanceJobId, rebalanceConfig, allowRetries),
          _resourceManager.getPropertyStore());
    }
    return rebalanceTable(tableNameWithType, tableConfig, rebalanceJobId, rebalanceConfig,
        zkBasedTableRebalanceObserver);
  }

  /**
   * Rebalance the table with the given name and type asynchronously. The number of concurrent rebalances permitted
   * on this controller is configured by
   * {@link org.apache.pinot.controller.ControllerConf#CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS}
   *
   * @param tableNameWithType name of the table to rebalance
   * @param rebalanceConfig configuration for the rebalance operation
   * @param rebalanceJobId ID of the rebalance job, which is used to track the progress of the rebalance operation
   * @param trackRebalanceProgress whether to track rebalance progress stats in ZK
   * @param allowRetries whether to allow retries for failed or stuck rebalance operations (through
   *                     {@link RebalanceChecker}). Requires {@code trackRebalanceProgress} to be true.
   * @return a CompletableFuture that will complete with the result of the rebalance operation
   * @throws TableNotFoundException if the table does not exist
   * @throws RebalanceInProgressException if a rebalance job is already in progress for the table (as per ZK metadata)
   */
  public CompletableFuture<RebalanceResult> rebalanceTableAsync(String tableNameWithType,
      RebalanceConfig rebalanceConfig, String rebalanceJobId, boolean trackRebalanceProgress, boolean allowRetries)
      throws TableNotFoundException, RebalanceInProgressException {
    TableConfig tableConfig = _resourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new TableNotFoundException("Failed to find table config for table: " + tableNameWithType);
    }
    if (!rebalanceConfig.isDryRun()) {
      checkRebalanceJobInProgress(tableNameWithType);
    }
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return rebalanceTable(tableNameWithType, rebalanceConfig, rebalanceJobId, trackRebalanceProgress,
                allowRetries);
          } catch (TableNotFoundException e) {
            // Should not happen since we already checked for table existence
            throw new RuntimeException(e);
          } catch (RebalanceInProgressException e) {
            throw new RuntimeException(e);
          }
        },
        _executorService);
  }

  /**
   * Rebalance the table with the given name and type asynchronously. The number of concurrent rebalances permitted
   * on this controller is configured by
   * {@link org.apache.pinot.controller.ControllerConf#CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS}
   *
   * @param tableNameWithType name of the table to rebalance
   * @param tableConfig configuration for the table to rebalance
   * @param rebalanceJobId ID of the rebalance job, which is used to track the progress of the rebalance operation
   * @param rebalanceConfig configuration for the rebalance operation
   * @param zkBasedTableRebalanceObserver observer to track rebalance progress in ZK
   * @return a CompletableFuture that will complete with the result of the rebalance operation
   * @throws RebalanceInProgressException if a rebalance job is already in progress for the table (as per ZK metadata)
   */
  public CompletableFuture<RebalanceResult> rebalanceTableAsync(String tableNameWithType, TableConfig tableConfig,
      String rebalanceJobId, RebalanceConfig rebalanceConfig,
      @Nullable ZkBasedTableRebalanceObserver zkBasedTableRebalanceObserver)
      throws RebalanceInProgressException {
    if (!rebalanceConfig.isDryRun()) {
      checkRebalanceJobInProgress(tableNameWithType);
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return rebalanceTable(tableNameWithType, tableConfig, rebalanceJobId, rebalanceConfig,
                zkBasedTableRebalanceObserver);
          } catch (RebalanceInProgressException e) {
            throw new RuntimeException(e);
          }
        },
        _executorService);
  }

  @VisibleForTesting
  RebalanceResult rebalanceTable(String tableNameWithType, TableConfig tableConfig, String rebalanceJobId,
      RebalanceConfig rebalanceConfig, @Nullable ZkBasedTableRebalanceObserver zkBasedTableRebalanceObserver)
      throws RebalanceInProgressException {

    if (!rebalanceConfig.isDryRun()) {
      checkRebalanceJobInProgress(tableNameWithType);
    }

    Map<String, Set<String>> tierToSegmentsMap;
    if (rebalanceConfig.isUpdateTargetTier()) {
      tierToSegmentsMap = _resourceManager.updateTargetTier(rebalanceJobId, tableNameWithType, tableConfig);
    } else {
      tierToSegmentsMap = null;
    }
    TableRebalancer tableRebalancer =
        new TableRebalancer(_resourceManager.getHelixZkManager(), zkBasedTableRebalanceObserver, _controllerMetrics,
            _rebalancePreChecker, _tableSizeReader);

    return tableRebalancer.rebalance(tableConfig, rebalanceConfig, rebalanceJobId, tierToSegmentsMap);
  }

  /**
   * Cancels ongoing rebalance jobs (if any) for the given table.
   *
   * @param tableNameWithType name of the table for which to cancel any ongoing rebalance job
   * @return the list of job IDs that were cancelled
   */
  public List<String> cancelRebalance(String tableNameWithType) {
    List<String> cancelledJobIds = new ArrayList<>();
    boolean updated = _resourceManager.updateJobsForTable(tableNameWithType, ControllerJobTypes.TABLE_REBALANCE,
        jobMetadata -> {
          String jobId = jobMetadata.get(CommonConstants.ControllerJob.JOB_ID);
          try {
            String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
            TableRebalanceProgressStats jobStats =
                JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
            if (jobStats.getStatus() != RebalanceResult.Status.IN_PROGRESS) {
              return;
            }

            LOGGER.info("Cancelling rebalance job: {} for table: {}", jobId, tableNameWithType);
            jobStats.setStatus(RebalanceResult.Status.CANCELLED);
            jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
                JsonUtils.objectToString(jobStats));
            cancelledJobIds.add(jobId);
          } catch (Exception e) {
            LOGGER.error("Failed to cancel rebalance job: {} for table: {}", jobId, tableNameWithType, e);
          }
        });
    LOGGER.info("Tried to cancel existing rebalance jobs for table: {} at best effort and done: {}", tableNameWithType,
        updated);
    return cancelledJobIds;
  }

  /**
   * Gets the status of the rebalance job with the given ID.
   *
   * @param jobId ID of the rebalance job to get the status for
   * @return response containing the status of the rebalance job
   * @throws JsonProcessingException if there is an error processing the rebalance progress stats from ZK
   * @throws NotFoundException if the rebalance job with the given ID does not exist
   */
  public ServerRebalanceJobStatusResponse getRebalanceStatus(String jobId)
      throws JsonProcessingException {
    Map<String, String> controllerJobZKMetadata =
        _resourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TABLE_REBALANCE);
    if (controllerJobZKMetadata == null) {
      LOGGER.warn("Rebalance job with ID: {} not found", jobId);
      throw new NotFoundException("Rebalance job with ID: " + jobId + " not found");
    }
    ServerRebalanceJobStatusResponse serverRebalanceJobStatusResponse = new ServerRebalanceJobStatusResponse();
    TableRebalanceProgressStats tableRebalanceProgressStats = JsonUtils.stringToObject(
        controllerJobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
        TableRebalanceProgressStats.class);
    serverRebalanceJobStatusResponse.setTableRebalanceProgressStats(tableRebalanceProgressStats);

    long timeSinceStartInSecs = 0L;
    if (RebalanceResult.Status.DONE != tableRebalanceProgressStats.getStatus()) {
      timeSinceStartInSecs = (System.currentTimeMillis() - tableRebalanceProgressStats.getStartTimeMs()) / 1000;
    }
    serverRebalanceJobStatusResponse.setTimeElapsedSinceStartInSeconds(timeSinceStartInSecs);

    String jobCtxInStr = controllerJobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT);
    if (StringUtils.isNotEmpty(jobCtxInStr)) {
      TableRebalanceContext jobCtx = JsonUtils.stringToObject(jobCtxInStr, TableRebalanceContext.class);
      serverRebalanceJobStatusResponse.setTableRebalanceContext(jobCtx);
    }
    return serverRebalanceJobStatusResponse;
  }

  private void checkRebalanceJobInProgress(String tableNameWithType)
      throws RebalanceInProgressException {
    String rebalanceJobInProgress = rebalanceJobInProgress(tableNameWithType, _resourceManager.getPropertyStore());
    if (rebalanceJobInProgress != null) {
      String errorMsg = "Rebalance job is already in progress for table: " + tableNameWithType + ", jobId: "
          + rebalanceJobInProgress + ". Please wait for the job to complete or cancel it before starting a new one.";
      throw new RebalanceInProgressException(errorMsg);
    }
  }

  /**
   * Checks if there is an ongoing rebalance job for the given table.
   *
   * @param tableNameWithType name of the table to check for ongoing rebalance jobs
   * @param propertyStore ZK property store to read the job metadata from
   * @return jobId of the ongoing rebalance job if one exists, {@code null} otherwise
   */
  @Nullable
  public static String rebalanceJobInProgress(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    // Get all jobMetadata for the given table with a single ZK read.
    Map<String, Map<String, String>> allJobMetadataByJobId =
        ControllerZkHelixUtils.getAllControllerJobs(Set.of(ControllerJobTypes.TABLE_REBALANCE),
            jobMetadata -> tableNameWithType.equals(
                jobMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE)), propertyStore);

    for (Map.Entry<String, Map<String, String>> entry : allJobMetadataByJobId.entrySet()) {
      String jobId = entry.getKey();
      Map<String, String> jobMetadata = entry.getValue();
      String jobStatsInStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);

      TableRebalanceProgressStats jobStats;
      try {
        jobStats = JsonUtils.stringToObject(jobStatsInStr, TableRebalanceProgressStats.class);
      } catch (Exception e) {
        // If the job stats cannot be parsed, let's assume that the job is not in progress.
        continue;
      }

      if (jobStats.getStatus() == RebalanceResult.Status.IN_PROGRESS) {
        return jobId;
      }
    }

    return null;
  }
}
