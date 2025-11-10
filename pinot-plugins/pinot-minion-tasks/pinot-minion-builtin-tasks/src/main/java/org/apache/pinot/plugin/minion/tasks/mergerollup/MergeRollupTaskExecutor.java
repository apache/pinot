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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.AdaptiveSegmentNumRowProvider;
import org.apache.pinot.core.segment.processing.framework.DefaultSegmentNumRowProvider;
import org.apache.pinot.core.segment.processing.framework.PercentileAdaptiveSegmentNumRowProvider;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentNumRowProvider;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MergeTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.Obfuscator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task executor that provides merge and rollup service.
 */
public class MergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskExecutor.class);

  public MergeRollupTaskExecutor(MinionConf minionConf) {
    super(minionConf);
  }

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
      File workingDir)
      throws Exception {
    int numInputSegments = segmentDirs.size();
    _eventObserver.notifyProgress(pinotTaskConfig, "Converting segments: " + numInputSegments);
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }
    long startMillis = System.currentTimeMillis();

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    Set<String> dimensionsToErase = MergeRollupTaskUtils.getDimensionsToErase(configs);
    List<RecordTransformer> customRecordTransformers = new ArrayList<>();
    if (!dimensionsToErase.isEmpty()) {
      customRecordTransformers.add(new DimensionValueTransformer(schema, dimensionsToErase));
    }

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Time handler config
    segmentProcessorConfigBuilder
        .setTimeHandlerConfig(MergeTaskUtils.getTimeHandlerConfig(tableConfig, schema, configs));

    // Partitioner config
    segmentProcessorConfigBuilder
        .setPartitionerConfigs(MergeTaskUtils.getPartitionerConfigs(tableConfig, schema, configs));

    // Merge type
    segmentProcessorConfigBuilder.setMergeType(MergeTaskUtils.getMergeType(configs));

    // Aggregation types
    segmentProcessorConfigBuilder.setAggregationTypes(MergeTaskUtils.getAggregationTypes(configs));

    // Aggregation function parameters
    segmentProcessorConfigBuilder.setAggregationFunctionParameters(
        MergeRollupTaskUtils.getAggregationFunctionParameters(configs));

    // Segment config
    segmentProcessorConfigBuilder.setSegmentConfig(MergeTaskUtils.getSegmentConfig(configs));

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    List<RecordReader> recordReaders = new ArrayList<>(numInputSegments);
    int count = 1;
    for (File segmentDir : segmentDirs) {
      _eventObserver.notifyProgress(_pinotTaskConfig, "Creating RecordReader for: " + segmentDir + " (" + (count++)
          + " out of " + numInputSegments + ")");
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(segmentDir, null, null, true);
      recordReaders.add(recordReader);
    }
    List<File> outputSegmentDirs;
    try {
      _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segments");
      SegmentNumRowProvider segmentNumRowProvider =
          createSegmentNumRowProvider(configs, segmentProcessorConfig.getSegmentConfig());
      SegmentProcessorFramework framework = new SegmentProcessorFramework(segmentProcessorConfig, workingDir,
          SegmentProcessorFramework.convertRecordReadersToRecordReaderFileConfig(recordReaders),
          customRecordTransformers, segmentNumRowProvider);
      outputSegmentDirs = framework.process();
      _eventObserver.notifyProgress(pinotTaskConfig,
          "Segment processing stats - incomplete rows:" + framework.getIncompleteRowsFound() + ", dropped rows:"
              + framework.getSkippedRowsFound() + ", sanitized rows:" + framework.getSanitizedRowsFound());
    } finally {
      for (RecordReader recordReader : recordReaders) {
        recordReader.close();
      }
    }

    long endMillis = System.currentTimeMillis();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType,
          Obfuscator.DEFAULT.toJsonString(configs), (endMillis - startMillis));
    }
    List<SegmentConversionResult> results = new ArrayList<>();
    for (File outputSegmentDir : outputSegmentDirs) {
      String outputSegmentName = outputSegmentDir.getName();
      results.add(new SegmentConversionResult.Builder().setFile(outputSegmentDir).setSegmentName(outputSegmentName)
          .setTableNameWithType(tableNameWithType).build());
    }
    return results;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    Map<String, String> updateMap = new TreeMap<>();
    updateMap.put(MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY,
        pinotTaskConfig.getConfigs().get(MergeRollupTask.MERGE_LEVEL_KEY));
    updateMap.put(MergeRollupTask.SEGMENT_ZK_METADATA_TIME_KEY, String.valueOf(System.currentTimeMillis()));
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, updateMap);
  }

  /**
   * Creates the appropriate SegmentNumRowProvider based on the task configuration for merge rollup tasks.
   * <p>
   * This method enables adaptive segment sizing by examining the task configuration and creating
   * the corresponding provider:
   * <ul>
   *   <li>If desiredSegmentSizeBytes is not configured: uses DefaultSegmentNumRowProvider (static sizing)</li>
   *   <li>If ADAPTIVE strategy: uses AdaptiveSegmentNumRowProvider with EMA-based learning</li>
   *   <li>If PERCENTILE strategy: uses PercentileAdaptiveSegmentNumRowProvider for heterogeneous data</li>
   *   <li>If DEFAULT strategy: uses DefaultSegmentNumRowProvider even if size configured</li>
   * </ul>
   *
   * <p>Similar to how eraseDimensionValues is handled, this reads directly from the task config map
   * rather than going through SegmentConfig, since it's specific to MergeRollupTask.</p>
   *
   * @param taskConfig the task configuration map
   * @param segmentConfig the segment configuration (used for maxNumRecordsPerSegment)
   * @return the appropriate SegmentNumRowProvider instance
   */
  private static SegmentNumRowProvider createSegmentNumRowProvider(Map<String, String> taskConfig,
      SegmentConfig segmentConfig) {
    int maxNumRecordsPerSegment = segmentConfig.getMaxNumRecordsPerSegment();

    // Read adaptive sizing configuration from task config
    String desiredSegmentSizeBytesStr = taskConfig.get(MergeRollupTask.DESIRED_SEGMENT_SIZE_BYTES_KEY);
    if (desiredSegmentSizeBytesStr == null) {
      LOGGER.info("Using DefaultSegmentNumRowProvider with maxRecordsPerSegment={}", maxNumRecordsPerSegment);
      return new DefaultSegmentNumRowProvider(maxNumRecordsPerSegment);
    }

    long desiredSegmentSizeBytes = Long.parseLong(desiredSegmentSizeBytesStr);

    // Read strategy (defaults to PERCENTILE for adaptive sizing)
    String strategyStr = taskConfig.get(MergeRollupTask.SEGMENT_SIZING_STRATEGY_KEY);
    String strategy = strategyStr != null ? strategyStr : "PERCENTILE";

    switch (strategy) {
      case "ADAPTIVE":
        String learningRateStr = taskConfig.get(MergeRollupTask.SIZING_LEARNING_RATE_KEY);
        double learningRate = learningRateStr != null ? Double.parseDouble(learningRateStr) : 0.3;
        LOGGER.info("Using AdaptiveSegmentNumRowProvider for merge rollup with desiredSegmentSize={}MB, "
                + "maxRecordsPerSegment={}, learningRate={}",
            desiredSegmentSizeBytes / (1024 * 1024), maxNumRecordsPerSegment, learningRate);
        return new AdaptiveSegmentNumRowProvider(desiredSegmentSizeBytes, maxNumRecordsPerSegment,
            learningRate, 5000.0);

      case "PERCENTILE":
        String percentileStr = taskConfig.get(MergeRollupTask.SIZING_PERCENTILE_KEY);
        int percentile = percentileStr != null ? Integer.parseInt(percentileStr) : 75;
        LOGGER.info("Using PercentileAdaptiveSegmentNumRowProvider for merge rollup with desiredSegmentSize={}MB, "
                + "maxRecordsPerSegment={}, percentile=P{}",
            desiredSegmentSizeBytes / (1024 * 1024), maxNumRecordsPerSegment, percentile);
        return new PercentileAdaptiveSegmentNumRowProvider(desiredSegmentSizeBytes, maxNumRecordsPerSegment,
            percentile);

      case "DEFAULT":
      default:
        LOGGER.info("Using DefaultSegmentNumRowProvider with maxRecordsPerSegment={} "
            + "(desiredSegmentSizeBytes configured but strategy is DEFAULT)", maxNumRecordsPerSegment);
        return new DefaultSegmentNumRowProvider(maxNumRecordsPerSegment);
    }
  }
}
