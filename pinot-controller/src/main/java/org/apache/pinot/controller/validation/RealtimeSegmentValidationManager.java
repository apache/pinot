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
package org.apache.pinot.controller.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Validates realtime ideal states and segment metadata, fixing any partitions which have stopped consuming,
 * and uploading segments to deep store if segment download url is missing in the metadata.
 */
public class RealtimeSegmentValidationManager extends ControllerPeriodicTask<RealtimeSegmentValidationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentValidationManager.class);

  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;
  private final ControllerMetrics _controllerMetrics;
  private final StorageQuotaChecker _storageQuotaChecker;

  private final int _segmentLevelValidationIntervalInSeconds;
  private long _lastSegmentLevelValidationRunTimeMs = 0L;
  private final boolean _segmentAutoResetOnErrorAtValidation;

  public static final String OFFSET_CRITERIA = "offsetCriteria";

  public RealtimeSegmentValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ValidationMetrics validationMetrics, ControllerMetrics controllerMetrics, StorageQuotaChecker quotaChecker) {
    super("RealtimeSegmentValidationManager", config.getRealtimeSegmentValidationFrequencyInSeconds(),
        config.getRealtimeSegmentValidationManagerInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _controllerMetrics = controllerMetrics;
    _storageQuotaChecker = quotaChecker;

    _segmentLevelValidationIntervalInSeconds = config.getSegmentLevelValidationIntervalInSeconds();
    _segmentAutoResetOnErrorAtValidation = config.isAutoResetErrorSegmentsOnValidationEnabled();
    Preconditions.checkState(_segmentLevelValidationIntervalInSeconds > 0);
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    Context context = new Context();
    // Run segment level validation only if certain time has passed after previous run
    long currentTimeMs = System.currentTimeMillis();
    if (TimeUnit.MILLISECONDS.toSeconds(currentTimeMs - _lastSegmentLevelValidationRunTimeMs)
        >= _segmentLevelValidationIntervalInSeconds) {
      LOGGER.info("Run segment-level validation");
      context._runSegmentLevelValidation = true;
      _lastSegmentLevelValidationRunTimeMs = currentTimeMs;
    }
    String offsetCriteriaStr = periodicTaskProperties.getProperty(OFFSET_CRITERIA);
    if (offsetCriteriaStr != null) {
      context._offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString(offsetCriteriaStr);
    }
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.warn("Failed to find table config for table: {}, skipping validation", tableNameWithType);
      return;
    }
    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig).stream().map(
        streamConfig -> new StreamConfig(tableConfig.getTableName(), streamConfig)
    ).collect(Collectors.toList());

    if (shouldEnsureConsuming(tableNameWithType)) {
      _llcRealtimeSegmentManager.ensureAllPartitionsConsuming(tableConfig, streamConfigs, context._offsetCriteria);
    }

    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      if (!_llcRealtimeSegmentManager.cleanUpCommittedSegments(tableNameWithType)) {
        LOGGER.error("Failed to cleanup committed segments for table: {}", tableNameWithType);
      }
    }

    if (context._runSegmentLevelValidation) {
      runSegmentLevelValidation(tableConfig);
    } else {
      LOGGER.info("Skipping segment-level validation for table: {}", tableConfig.getTableName());
    }
  }

  /**
   *
   * Updates the table paused state based on pause validations (e.g. storage quota being exceeded).
   * Skips updating the pause state if table is paused by admin.
   * Returns true if table is not paused
   */
  private boolean shouldEnsureConsuming(String tableNameWithType) {
    PauseStatusDetails pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
    boolean isTablePaused = pauseStatus.getPauseFlag();
    // if table is paused by admin then don't compute
    if (isTablePaused && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.ADMINISTRATIVE)) {
      return false;
    }
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    boolean isQuotaExceeded = _storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig);
    if (isQuotaExceeded == isTablePaused) {
      return !isTablePaused;
    }
    // if quota breach and pause flag is not in sync, update the IS
    if (isQuotaExceeded) {
      String storageQuota = tableConfig.getQuotaConfig() != null ? tableConfig.getQuotaConfig().getStorage() : "NA";
      // as quota is breached pause the consumption right away
      _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED,
          "Storage quota of " + storageQuota + " exceeded.");
    } else {
      // as quota limit is being honored, unset the pause state and allow consuming segment recreation.
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, "Table storage within quota limits");
    }
    return !isQuotaExceeded;
  }

  private void runSegmentLevelValidation(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();

    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName);

    // Delete tmp segments
    if (_llcRealtimeSegmentManager.isTmpSegmentAsyncDeletionEnabled()) {
      try {
        long startTimeMs = System.currentTimeMillis();
        int numDeletedTmpSegments = _llcRealtimeSegmentManager.deleteTmpSegments(realtimeTableName, segmentsZKMetadata);
        LOGGER.info("Deleted {} tmp segments for table: {} in {}ms", numDeletedTmpSegments, realtimeTableName,
            System.currentTimeMillis() - startTimeMs);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.DELETED_TMP_SEGMENT_COUNT,
            numDeletedTmpSegments);
      } catch (Exception e) {
        LOGGER.error("Failed to delete tmp segments for table: {}", realtimeTableName, e);
      }
    }

    // Update the total document count gauge
    _validationMetrics.updateTotalDocumentCountGauge(realtimeTableName, computeTotalDocumentCount(segmentsZKMetadata));

    // Ensures all segments in COMMITTING state are properly tracked in ZooKeeper.
    // Acts as a recovery mechanism for segments that may have failed to register during start of commit protocol.
    addSegmentsInCommittingStatus(realtimeTableName, segmentsZKMetadata);

    // Check missing segments and upload them to the deep store
    if (_llcRealtimeSegmentManager.isDeepStoreLLCSegmentUploadRetryEnabled()) {
      _llcRealtimeSegmentManager.uploadToDeepStoreIfMissing(tableConfig, segmentsZKMetadata);
    }

    if (_segmentAutoResetOnErrorAtValidation) {
      _pinotHelixResourceManager.resetSegments(realtimeTableName, null, true);
    }
  }

  private void addSegmentsInCommittingStatus(String realtimeTableName,
      List<SegmentZKMetadata> segmentsZKMetadata) {
    List<String> committingSegments = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (CommonConstants.Segment.Realtime.Status.COMMITTING.equals(segmentZKMetadata.getStatus())) {
        committingSegments.add(segmentZKMetadata.getSegmentName());
      }
    }
    if (!_llcRealtimeSegmentManager.addCommittingSegments(realtimeTableName, committingSegments)) {
      LOGGER.error("Failed to add committing segments for table: {}", realtimeTableName);
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        _validationMetrics.cleanupTotalDocumentCountGauge(tableNameWithType);
        _controllerMetrics.removeTableMeter(tableNameWithType, ControllerMeter.DELETED_TMP_SEGMENT_COUNT);
      }
    }
  }

  @VisibleForTesting
  static long computeTotalDocumentCount(List<SegmentZKMetadata> segmentsZKMetadata) {
    long numTotalDocs = 0;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      numTotalDocs += segmentZKMetadata.getTotalDocs();
    }
    return numTotalDocs;
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Unregister all the validation metrics.");
    _validationMetrics.unregisterAllMetrics();
  }

  public static final class Context {
    private boolean _runSegmentLevelValidation;
    private OffsetCriteria _offsetCriteria;
  }

  @VisibleForTesting
  public ValidationMetrics getValidationMetrics() {
    return _validationMetrics;
  }
}
