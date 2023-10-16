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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
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

  private final int _segmentLevelValidationIntervalInSeconds;
  private long _lastSegmentLevelValidationRunTimeMs = 0L;

  public static final String RECREATE_DELETED_CONSUMING_SEGMENT_KEY = "recreateDeletedConsumingSegment";
  public static final String OFFSET_CRITERIA = "offsetCriteria";

  public RealtimeSegmentValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ValidationMetrics validationMetrics, ControllerMetrics controllerMetrics) {
    super("RealtimeSegmentValidationManager", config.getRealtimeSegmentValidationFrequencyInSeconds(),
        config.getRealtimeSegmentValidationManagerInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _controllerMetrics = controllerMetrics;

    _segmentLevelValidationIntervalInSeconds = config.getSegmentLevelValidationIntervalInSeconds();
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
    context._recreateDeletedConsumingSegment =
        Boolean.parseBoolean(periodicTaskProperties.getProperty(RECREATE_DELETED_CONSUMING_SEGMENT_KEY));
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
    StreamConfig streamConfig =
        new StreamConfig(tableConfig.getTableName(), IngestionConfigUtils.getStreamConfigMap(tableConfig));
    if (context._runSegmentLevelValidation) {
      runSegmentLevelValidation(tableConfig, streamConfig);
    }
    _llcRealtimeSegmentManager.ensureAllPartitionsConsuming(tableConfig, streamConfig,
        context._recreateDeletedConsumingSegment, context._offsetCriteria);
  }

  private void runSegmentLevelValidation(TableConfig tableConfig, StreamConfig streamConfig) {
    String realtimeTableName = tableConfig.getTableName();

    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName);

    // Delete tmp segments
    try {
      long numDeleteTmpSegments = _llcRealtimeSegmentManager.deleteTmpSegments(realtimeTableName, segmentsZKMetadata);
      LOGGER.info("Deleted {} tmp segments for table: {}", numDeleteTmpSegments, realtimeTableName);
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.DELETED_TMP_SEGMENT_COUNT,
          numDeleteTmpSegments);
    } catch (Exception e) {
      LOGGER.error("Failed to delete tmp segments for table: {}", realtimeTableName, e);
    }

    // Update the total document count gauge
    _validationMetrics.updateTotalDocumentCountGauge(realtimeTableName, computeTotalDocumentCount(segmentsZKMetadata));

    // Check missing segments and upload them to the deep store
    if (_llcRealtimeSegmentManager.isDeepStoreLLCSegmentUploadRetryEnabled()) {
      _llcRealtimeSegmentManager.uploadToDeepStoreIfMissing(tableConfig, segmentsZKMetadata);
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
    private boolean _recreateDeletedConsumingSegment;
    private OffsetCriteria _offsetCriteria;
  }

  @VisibleForTesting
  public ValidationMetrics getValidationMetrics() {
    return _validationMetrics;
  }
}
