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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Validates realtime ideal states and segment metadata, fixing any partitions which have stopped consuming,
/// and uploading segments to deep store if segment download url is missing in the metadata.
public class RealtimeSegmentValidationManager extends ControllerPeriodicTask<RealtimeSegmentValidationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentValidationManager.class);
  public static final String OFFSET_CRITERIA = "offsetCriteria";
  public static final String REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP =
      "repairErrorSegmentsForPartialUpsertOrDedup";

  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;
  private final ControllerMetrics _controllerMetrics;
  private final StorageQuotaChecker _storageQuotaChecker;
  private final PrimaryKeyQuotaChecker _primaryKeyQuotaChecker;
  private final ResourceUtilizationManager _resourceUtilizationManager;
  private final int _segmentLevelValidationIntervalInSeconds;
  private final boolean _segmentAutoResetOnErrorAtValidation;

  private long _lastSegmentLevelValidationRunTimeMs = 0L;
  private volatile DisasterRecoveryMode _disasterRecoveryMode;

  public RealtimeSegmentValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ValidationMetrics validationMetrics, ControllerMetrics controllerMetrics, StorageQuotaChecker quotaChecker,
      PrimaryKeyQuotaChecker primaryKeyQuotaChecker, ResourceUtilizationManager resourceUtilizationManager) {
    super("RealtimeSegmentValidationManager", config.getRealtimeSegmentValidationFrequencyInSeconds(),
        config.getRealtimeSegmentValidationManagerInitialDelaySeconds(),
        config.getRealtimeSegmentValidationCronExpression(),
        pinotHelixResourceManager, leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _controllerMetrics = controllerMetrics;
    _storageQuotaChecker = quotaChecker;
    _primaryKeyQuotaChecker = primaryKeyQuotaChecker;
    _resourceUtilizationManager = resourceUtilizationManager;

    _segmentLevelValidationIntervalInSeconds = config.getSegmentLevelValidationIntervalInSeconds();
    _segmentAutoResetOnErrorAtValidation = config.isAutoResetErrorSegmentsOnValidationEnabled();
    _disasterRecoveryMode = config.getDisasterRecoveryMode();
    Preconditions.checkState(_segmentLevelValidationIntervalInSeconds > 0);
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    Context context = new Context();
    // Run segment level validation only if certain time has passed after previous run
    long currentTimeMs = System.currentTimeMillis();
    if (shouldRunSegmentValidation(periodicTaskProperties, currentTimeMs)) {
      LOGGER.info("Run segment-level validation");
      context._runSegmentLevelValidation = true;
      _lastSegmentLevelValidationRunTimeMs = currentTimeMs;
    }
    context._repairErrorSegmentsForPartialUpsertOrDedup =
        shouldRepairErrorSegmentsForPartialUpsertOrDedup(periodicTaskProperties);
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
    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(tableConfig);

    if (shouldEnsureConsuming(tableNameWithType)) {
      _llcRealtimeSegmentManager.ensureAllPartitionsConsuming(tableConfig, streamConfigs, context._offsetCriteria);
    }

    if (context._runSegmentLevelValidation) {
      runSegmentLevelValidation(tableConfig);
    } else {
      LOGGER.info("Skipping segment-level validation for table: {}", tableConfig.getTableName());
    }

    boolean isPauselessTable = PauselessConsumptionUtils.isPauselessEnabled(tableConfig);
    if (isPauselessTable || _segmentAutoResetOnErrorAtValidation) {
      // Repair segments in error state. Dedup and partial upsert tables skip the repairs that consume from the stream
      // again unless overridden (see repairSegmentsInErrorState).
      // When pauseless consumption is disabled, this behavior remains gated by
      // _segmentAutoResetOnErrorAtValidation to preserve legacy semantics.
      _llcRealtimeSegmentManager.repairSegmentsInErrorState(tableConfig,
          context._repairErrorSegmentsForPartialUpsertOrDedup);
    } else {
      LOGGER.debug(
          "Skipping segment error repair for table {} because pauseless consumption is disabled and "
              + "_segmentAutoResetOnErrorAtValidation=false",
          tableConfig.getTableName());
    }
  }

  /// Updates the table paused state based on pause validations (e.g. storage quota being exceeded).
  /// Skips updating the pause state if table is paused by admin.
  /// Returns true if table is not paused
  @VisibleForTesting
  boolean shouldEnsureConsuming(String tableNameWithType) {
    PauseStatusDetails pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
    boolean isTablePaused = pauseStatus.getPauseFlag();
    if (isTablePaused && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.ADMINISTRATIVE)) {
      return false; // if table is paused by admin, then skip subsequent checks
    }
    // Perform resource utilization checks.
    UtilizationChecker.CheckResult isResourceUtilizationWithinLimits =
        _resourceUtilizationManager.isResourceUtilizationWithinLimits(tableNameWithType,
            UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    if (isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.FAIL) {
      LOGGER.warn("Resource utilization limit exceeded for table: {}", tableNameWithType);
      _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
          1L);
      // update IS when table is not paused or paused with another reason code
      if (!isTablePaused || !pauseStatus.getReasonCode()
          .equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
        _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType,
            PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
            "Resource utilization limit exceeded. Check ResourceUtilizationManager log on controllers for detail "
                + "reasons");
      }
      return false; // if resource utilization check failed, then skip subsequent checks
    } else if ((isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.PASS) && isTablePaused
        && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
      // within limits and table previously paused by resource utilization --> unpause
      LOGGER.info("Resource utilization limit is back within limits for table: {}", tableNameWithType);
      // unset the pause state and allow consuming segment recreation.
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, "Resource utilization within limits");
      pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
      isTablePaused = pauseStatus.getPauseFlag();
    } else if ((isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.UNDETERMINED) && isTablePaused
        && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
      // The table was previously paused due to exceeding resource utilization, but the current status cannot be
      // determined. To be safe, leave it as paused and once the status is available take the correct action
      LOGGER.warn("Resource utilization limit could not be determined for table: {}, and it is paused, leave it as "
          + "paused", tableNameWithType);
      return false;
    }
    _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
        0L);
    // Perform storage and primary key quota checks. Both pause realtime consumption on breach but share a single
    // IdealState pause flag, so the logic below is pause-reason-aware: it never resumes a table paused for one reason
    // just because the other reason is clear, and it resumes only when ALL quota reasons are within limits.
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);

    // Only an enforced breach pauses consumption; a breach in SHADOW or DISABLED mode is treated as within quota.
    boolean isStorageQuotaBreached = _storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig);
    QuotaEnforcementMode storageMode = _storageQuotaChecker.getEnforcementMode(tableConfig);
    if (isStorageQuotaBreached && storageMode == QuotaEnforcementMode.SHADOW) {
      // The breach meter is already emitted by StorageQuotaChecker
      LOGGER.warn("Storage quota exceeded for table: {}, but enforcement mode is SHADOW, not pausing consumption",
          tableNameWithType);
    }
    boolean storageExceeded = isStorageQuotaBreached && storageMode == QuotaEnforcementMode.ENFORCED;

    // Primary key quota: same gating. UNDETERMINED (counts could not be read) is handled below.
    PrimaryKeyQuotaChecker.Status pkStatus = _primaryKeyQuotaChecker.checkPrimaryKeyQuota(tableConfig);
    QuotaEnforcementMode pkMode = _primaryKeyQuotaChecker.getEnforcementMode(tableConfig);
    boolean isPkQuotaBreached = pkStatus == PrimaryKeyQuotaChecker.Status.EXCEEDED;
    if (isPkQuotaBreached && pkMode == QuotaEnforcementMode.SHADOW) {
      // The breach meter is already emitted by PrimaryKeyQuotaChecker
      LOGGER.warn("Primary key quota exceeded for table: {}, but enforcement mode is SHADOW, not pausing consumption",
          tableNameWithType);
    }
    boolean pkExceeded = isPkQuotaBreached && pkMode == QuotaEnforcementMode.ENFORCED;

    // By this point the table is either not paused, or paused for a quota reason (ADMINISTRATIVE returned early and
    // RESOURCE_UTILIZATION was fully resolved above).
    PauseState.ReasonCode currentReason = isTablePaused ? pauseStatus.getReasonCode() : null;
    boolean pausedByQuota = currentReason == PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED
        || currentReason == PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED;
    // Storage takes precedence over primary key for the single stored pause reason when both are exceeded.
    PauseState.ReasonCode desiredReason = storageExceeded ? PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED
        : pkExceeded ? PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED : null;

    if (desiredReason == null) {
      // No quota is known to be exceeded, but an enforced primary key quota that couldn't be determined may still be.
      // Leave a quota-paused table paused whichever quota it was paused for: resuming it would only re-pause it (with
      // another force-commit) once the counts are readable again.
      if (pausedByQuota && pkMode == QuotaEnforcementMode.ENFORCED
          && pkStatus == PrimaryKeyQuotaChecker.Status.UNDETERMINED) {
        LOGGER.warn("Primary key quota could not be determined for table: {}, and it is paused for: {}, leave it as "
            + "paused", tableNameWithType, currentReason);
        return false;
      }
      if (pausedByQuota) {
        // All quota reasons are within limits --> resume, clearing the reason the table was paused with.
        _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false, currentReason,
            "Table quotas within limits");
      }
      // No quota reason is blocking consumption (ADMINISTRATIVE/RESOURCE_UTILIZATION were handled above).
      return true;
    }

    // A quota is exceeded --> the table must be paused with desiredReason.
    if (!isTablePaused) {
      _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType, desiredReason,
          quotaExceededComment(desiredReason, tableConfig));
    } else if (currentReason != desiredReason) {
      // Already paused by a quota but the active reason changed (e.g. storage now also breached): relabel the pause
      // state without issuing another force-commit.
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, true, desiredReason,
          quotaExceededComment(desiredReason, tableConfig));
    }
    return false;
  }

  private static String quotaExceededComment(PauseState.ReasonCode reasonCode, TableConfig tableConfig) {
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    if (reasonCode == PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED) {
      String storageQuota = quotaConfig != null ? quotaConfig.getStorage() : "NA";
      return "Storage quota of " + storageQuota + " exceeded.";
    }
    Object cap = quotaConfig != null ? quotaConfig.getMaxPrimaryKeysPerPartition() : "NA";
    return "Primary key quota of " + cap + " per partition exceeded.";
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
    boolean pauselessEnabled = PauselessConsumptionUtils.isPauselessEnabled(tableConfig);
    if (pauselessEnabled) {
      syncCommittingSegmentsFromMetadata(realtimeTableName, segmentsZKMetadata);
    }

    // Check missing segments and upload them to the deep store
    // If pauseless consumption is enabled, always run uploadToDeepStoreIfMissing step because in pauseless
    // consumption, the segment commit can be marked completed even if segment was not uploaded to the deep store.
    if (pauselessEnabled || _llcRealtimeSegmentManager.isDeepStoreLLCSegmentUploadRetryEnabled()) {
      _llcRealtimeSegmentManager.uploadToDeepStoreIfMissing(tableConfig, segmentsZKMetadata);
    }
  }

  private void syncCommittingSegmentsFromMetadata(String realtimeTableName,
      List<SegmentZKMetadata> segmentsZKMetadata) {
    List<String> committingSegments = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (CommonConstants.Segment.Realtime.Status.COMMITTING.equals(segmentZKMetadata.getStatus())) {
        committingSegments.add(segmentZKMetadata.getSegmentName());
      }
    }
    LOGGER.info("Adding committing segments to ZK: {}", committingSegments);
    if (!_llcRealtimeSegmentManager.syncCommittingSegments(realtimeTableName, committingSegments)) {
      LOGGER.error("Failed to add committing segments for table: {}", realtimeTableName);
    }
  }

  private boolean shouldRunSegmentValidation(Properties periodicTaskProperties, long currentTimeMs) {
    boolean runValidation = Optional.ofNullable(
            periodicTaskProperties.getProperty(RUN_SEGMENT_LEVEL_VALIDATION))
        .map(value -> {
          try {
            return Boolean.parseBoolean(value);
          } catch (Exception e) {
            return false;
          }
        })
        .orElse(false);

    boolean timeThresholdMet = TimeUnit.MILLISECONDS.toSeconds(currentTimeMs - _lastSegmentLevelValidationRunTimeMs)
        >= _segmentLevelValidationIntervalInSeconds;

    return runValidation || timeThresholdMet;
  }

  private boolean shouldRepairErrorSegmentsForPartialUpsertOrDedup(Properties periodicTaskProperties) {
    String property = periodicTaskProperties.getProperty(REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP);

    if (property == null) {
      return _llcRealtimeSegmentManager.shouldRepairErrorSegmentsForPartialUpsertOrDedup(_disasterRecoveryMode);
    }

    try {
      return Boolean.parseBoolean(property);
    } catch (Exception e) {
      LOGGER.warn("Failed to parse property '{}' for '{}'. Returning false.", property,
          REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP, e);
      return false;
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

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(ControllerConf.ControllerPeriodicTasksConf.DISASTER_RECOVERY_MODE_CONFIG_KEY)) {
      String disasterRecoveryModeString =
          clusterConfigs.get(ControllerConf.ControllerPeriodicTasksConf.DISASTER_RECOVERY_MODE_CONFIG_KEY);
      _disasterRecoveryMode = ControllerConf.getDisasterRecoveryMode(disasterRecoveryModeString);
    }
  }

  @VisibleForTesting
  DisasterRecoveryMode getDisasterRecoveryMode() {
    return _disasterRecoveryMode;
  }

  public static final class Context {
    private boolean _runSegmentLevelValidation;
    private boolean _repairErrorSegmentsForPartialUpsertOrDedup;
    private OffsetCriteria _offsetCriteria;
  }

  @VisibleForTesting
  public ValidationMetrics getValidationMetrics() {
    return _validationMetrics;
  }
}
