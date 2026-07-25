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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.Obfuscator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertCompactionTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskExecutor.class);

  /** Bounded retries for Check C bitmap fetch to ride out server reload races without re-downloading the segment. */
  @VisibleForTesting
  static final int DEFAULT_VALID_DOC_IDS_FETCH_MAX_ATTEMPTS = 3;

  @VisibleForTesting
  static final long DEFAULT_VALID_DOC_IDS_FETCH_RETRY_DELAY_MS = 500L;

  @VisibleForTesting
  int _validDocIdsFetchMaxAttempts = DEFAULT_VALID_DOC_IDS_FETCH_MAX_ATTEMPTS;

  @VisibleForTesting
  long _validDocIdsFetchRetryDelayMs = DEFAULT_VALID_DOC_IDS_FETCH_RETRY_DELAY_MS;

  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    _eventObserver.notifyProgress(pinotTaskConfig, "Compacting segment: " + indexDir);
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String taskType = pinotTaskConfig.getTaskType();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }
    long startMillis = System.currentTimeMillis();

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);

    String validDocIdsTypeStr = MinionTaskUtils.getValidDocIdsType(tableConfig.getUpsertConfig(), configs,
        UpsertCompactionTask.VALID_DOC_IDS_TYPE).toString();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    String originalSegmentCrcFromTaskGenerator = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    String crcFromDeepStorageSegment = segmentMetadata.getCrc();
    boolean ignoreCrcMismatch = Boolean.parseBoolean(configs.getOrDefault(UpsertCompactionTask.IGNORE_CRC_MISMATCH_KEY,
        String.valueOf(UpsertCompactionTask.DEFAULT_IGNORE_CRC_MISMATCH)));
    validateDeepStoreCrc(tableNameWithType, segmentName, originalSegmentCrcFromTaskGenerator, crcFromDeepStorageSegment,
        segmentMetadata.getDataCrc(), ignoreCrcMismatch);

    // Executor-only: read comparison mode string from task config (no auth resolution or URL hits).
    Map<String, String> taskConfigs =
        tableConfig.getTaskConfig() != null ? tableConfig.getTaskConfig().getConfigsForTaskType(taskType) : null;
    String consensusMode = taskConfigs != null
        ? taskConfigs.getOrDefault(MinionConstants.UpsertCompactionTask.VALID_DOC_IDS_CONSENSUS_MODE_KEY,
            MinionConstants.UpsertCompactionTask.DEFAULT_VALID_DOC_IDS_CONSENSUS_MODE)
        : MinionConstants.UpsertCompactionTask.DEFAULT_VALID_DOC_IDS_CONSENSUS_MODE;
    RoaringBitmap validDocIds =
        fetchValidDocIdsWithRetry(pinotTaskConfig, tableNameWithType, segmentName, validDocIdsTypeStr,
            originalSegmentCrcFromTaskGenerator, segmentMetadata.getDataCrc(), consensusMode);
    if (validDocIds.isEmpty()) {
      // prevents empty segment generation
      String skipMessage =
          String.format("Skipped: validDocIds is empty. Table: %s, segment: %s", tableNameWithType, segmentName);
      LOGGER.info(skipMessage);
      _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.COMPACTION_SKIP_EMPTY_VALID_DOCS, 1L);
      _eventObserver.notifyProgress(pinotTaskConfig, skipMessage);
      if (indexDir.exists() && !FileUtils.deleteQuietly(indexDir)) {
        LOGGER.warn("Failed to delete input segment: {}", indexDir.getAbsolutePath());
      }
      if (!FileUtils.deleteQuietly(workingDir)) {
        LOGGER.warn("Failed to delete working directory: {}", workingDir.getAbsolutePath());
      }
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
          .build();
    }

    int totalDocsAfterCompaction;
    try (CompactedPinotSegmentRecordReader compactedRecordReader = new CompactedPinotSegmentRecordReader(validDocIds)) {
      compactedRecordReader.init(indexDir, null, null);
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName,
          getSchema(tableNameWithType));
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, compactedRecordReader);
      driver.build();
      _eventObserver.notifyProgress(pinotTaskConfig,
          "Segment processing stats - incomplete rows:" + driver.getIncompleteRowsFound() + ", dropped rows:"
              + driver.getSkippedRowsFound() + ", sanitized rows:" + driver.getSanitizedRowsFound());
      totalDocsAfterCompaction = driver.getSegmentStats().getTotalDocCount();
    }

    File compactedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result =
        new SegmentConversionResult.Builder().setFile(compactedSegmentFile).setTableNameWithType(tableNameWithType)
            .setSegmentName(segmentName).build();
    _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.COMPACTED_RECORDS_COUNT,
            segmentMetadata.getTotalDocs() - totalDocsAfterCompaction);

    long endMillis = System.currentTimeMillis();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms. Total docs before compaction: {}. "
              + "Total docs after compaction: {}.", taskType, Obfuscator.DEFAULT.toJsonString(configs),
          (endMillis - startMillis), segmentMetadata.getTotalDocs(), totalDocsAfterCompaction);
    }

    return result;
  }

  /**
   * Check B: task-generation (ZK) segment CRC must match the downloaded deepstore/on-disk copy, with the same
   * data-CRC fallback used by server matching ({@link MinionTaskUtils#crcMatches}).
   */
  @VisibleForTesting
  void validateDeepStoreCrc(String tableNameWithType, String segmentName, String expectedSegmentCrc,
      String deepstoreSegmentCrc, String deepstoreDataCrc, boolean ignoreCrcMismatch) {
    if (ignoreCrcMismatch) {
      return;
    }
    long zkDataCrc = getZkDataCrc(tableNameWithType, segmentName);
    if (MinionTaskUtils.crcMatches(MinionTaskUtils.parseCrc(expectedSegmentCrc), zkDataCrc,
        MinionTaskUtils.parseCrc(deepstoreSegmentCrc), MinionTaskUtils.parseCrc(deepstoreDataCrc))) {
      return;
    }
    String message = "Crc mismatched between ZK and deepstore copy of segment: " + segmentName
        + ". Expected crc from ZK: " + expectedSegmentCrc + ", crc from deepstore: " + deepstoreSegmentCrc
        + ", zkDataCrc: " + zkDataCrc + ", deepstoreDataCrc: " + deepstoreDataCrc;
    LOGGER.error(message);
    _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.CRC_MISMATCH_DEEPSTORE, 1L);
    throw new IllegalStateException(message);
  }

  /**
   * Check C with a short bounded retry so transient server reload races (segment uploaded while a replica is still
   * rebuilding upsert metadata) do not fail the task on the first attempt. Does not re-download the segment.
   */
  @VisibleForTesting
  RoaringBitmap fetchValidDocIdsWithRetry(PinotTaskConfig pinotTaskConfig, String tableNameWithType, String segmentName,
      String validDocIdsTypeStr, String expectedSegmentCrc, String expectedDataCrc, String consensusMode)
      throws InterruptedException {
    int maxAttempts = Math.max(1, _validDocIdsFetchMaxAttempts);
    long delayMs = Math.max(0L, _validDocIdsFetchRetryDelayMs);
    IllegalStateException lastFailure = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        RoaringBitmap validDocIds =
            MinionTaskUtils.getValidDocIdFromServerMatchingCrc(tableNameWithType, segmentName, validDocIdsTypeStr,
                MINION_CONTEXT, expectedSegmentCrc, expectedDataCrc, consensusMode);
        if (validDocIds != null) {
          if (attempt > 1) {
            LOGGER.info("Obtained validDocIds for segment: {} on attempt {}/{}", segmentName, attempt, maxAttempts);
          }
          return validDocIds;
        }
        // All servers skipped (UNSAFE) or returned nothing usable.
        lastFailure = new IllegalStateException(
            "No validDocIds found from all servers. They either failed to download or did not match crc from"
                + " segment copy obtained from deepstore / servers. Expected crc: " + expectedSegmentCrc);
        LOGGER.warn("validDocIds unavailable for segment: {} on attempt {}/{}", segmentName, attempt, maxAttempts);
      } catch (IllegalStateException e) {
        lastFailure = e;
        LOGGER.warn("validDocIds fetch failed for segment: {} on attempt {}/{}: {}", segmentName, attempt, maxAttempts,
            e.getMessage());
      }

      if (attempt < maxAttempts) {
        String progress = String.format(
            "Retrying validDocIds fetch for segment: %s (attempt %d/%d) after CRC/server mismatch", segmentName,
            attempt + 1, maxAttempts);
        _eventObserver.notifyProgress(pinotTaskConfig, progress);
        if (delayMs > 0L) {
          try {
            Thread.sleep(delayMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
          }
        }
      }
    }

    String message = lastFailure != null ? lastFailure.getMessage()
        : "No validDocIds found from all servers. Expected crc: " + expectedSegmentCrc;
    LOGGER.error(message);
    MinionMeter meter = classifyValidDocIdsFailureMeter(message);
    _minionMetrics.addMeteredTableValue(tableNameWithType, meter, 1L);
    throw lastFailure != null ? lastFailure : new IllegalStateException(message);
  }

  @VisibleForTesting
  static MinionMeter classifyValidDocIdsFailureMeter(String message) {
    if (message != null && message.contains("CRC mismatch")) {
      return MinionMeter.CRC_MISMATCH_SERVER_BITMAP;
    }
    return MinionMeter.VALID_DOC_IDS_UNAVAILABLE;
  }

  /**
   * ZK data CRC for Check B data-CRC fallback. Returns -1 when metadata is missing or data CRC is not reported.
   */
  @VisibleForTesting
  long getZkDataCrc(String tableNameWithType, String segmentName) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType, segmentName);
    if (segmentZKMetadata == null) {
      return -1;
    }
    // Prefer data CRC whenever ZK reports a non-negative value (completed segments may still carry dataCrc after
    // commit even when useDataCrc is unset). Mirrors MinionTaskUtils.crcMatches availability rules.
    long dataCrc = segmentZKMetadata.getDataCrc();
    return dataCrc >= 0 ? dataCrc : -1;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(workingDir.getPath());
    config.setSegmentName(segmentName);

    // Keep index creation time the same as original segment because both segments use the same raw data.
    // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
    // identify if the new pushed segment has newer data than the existing one.
    config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

    // The time column type info is not stored in the segment metadata.
    // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
    if (segmentMetadata.getTimeInterval() != null) {
      config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
      config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
      config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
      config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
    }
    return config;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Map.of(UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
