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
package org.apache.pinot.minion.executor;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task executor that provides merge and rollup service
 *
 * TODO:
 *   1. Add the support for roll-up
 *   2. Add the support for time split to provide backfill support for merged segments
 */
public class MergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskExecutor.class);

  private static final String MERGED_SEGMENT_PREFIX = "merged_";
  private static final String DEFAULT_SEGMENT_NAME_GENERATOR_TYPE = "normalizedDate";
  private static final String DEFAULT_EXCLUDE_SEQUENCE_ID = "true";

  private static final String INPUT_SEGMENTS_DIR = "input_segments";
  private static final String OUTPUT_SEGMENTS_DIR = "output_segments";

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> originalIndexDirs,
      File workingDir)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String collectorTypeStr = configs.get(MinionConstants.MergeRollupTask.COLLECTOR_TYPE_KEY);
    Preconditions.checkNotNull(collectorTypeStr, "Collector type cannot be null.");
    CollectorFactory.CollectorType collectorType =
        CollectorFactory.CollectorType.valueOf(collectorTypeStr.toUpperCase());

    Preconditions.checkState(collectorType == CollectorFactory.CollectorType.CONCAT,
        "Only 'CONCAT' mode is currently supported.");

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String maxNumRecordsPerSegment = configs.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);

    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    // Generate segment processor config builder
    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder = new SegmentProcessorConfig.Builder();
    segmentProcessorConfigBuilder.setTableConfig(tableConfig).setSchema(schema);

    // Fill collector config
    CollectorConfig collectorConfig = new CollectorConfig.Builder().setCollectorType(collectorType).build();
    segmentProcessorConfigBuilder.setCollectorConfig(collectorConfig);

    // Fill segment config
    SegmentConfig.Builder segmentConfigBuilder = new SegmentConfig.Builder();

    // Default segment name convention for merge-rollup task is the following:
    // 'merged_<table_name>_<start_time>_<end_time>_<timestamp>'
    String segmentNameGeneratorType = configs
        .getOrDefault(MinionConstants.MergeRollupTask.SEGMENT_NAME_GENERATOR_TYPE_KEY,
            DEFAULT_SEGMENT_NAME_GENERATOR_TYPE);
    String defaultSegmentPrefix = MERGED_SEGMENT_PREFIX + TableNameBuilder.extractRawTableName(tableNameWithType);
    String segmentPrefix =
        configs.getOrDefault(MinionConstants.MergeRollupTask.SEGMENT_PREFIX_KEY, defaultSegmentPrefix);
    String segmentPostfix = configs
        .getOrDefault(MinionConstants.MergeRollupTask.SEGMENT_POSTFIX_KEY, Long.toString(System.currentTimeMillis()));
    String excludeSequenceIdStr =
        configs.getOrDefault(MinionConstants.MergeRollupTask.EXCLUDE_SEQUENCE_ID_KEY, DEFAULT_EXCLUDE_SEQUENCE_ID);

    boolean excludeSequenceId;
    try {
      excludeSequenceId = Boolean.parseBoolean(excludeSequenceIdStr);
    } catch (Exception e) {
      LOGGER.warn("Failed to parse the 'excludeSequenceId' configs: {} for table {}. Falling back to the default value "
          + "(excludeSequenceId = {}}).", excludeSequenceIdStr, DEFAULT_EXCLUDE_SEQUENCE_ID, tableNameWithType);
      excludeSequenceId = true;
    }

    segmentConfigBuilder.setSegmentNameGeneratorType(segmentNameGeneratorType).setSegmentPrefix(segmentPrefix)
        .setSegmentPostfix(segmentPostfix).setExcludeSequenceId(excludeSequenceId);
    if (maxNumRecordsPerSegment != null) {
      segmentConfigBuilder.setMaxNumRecordsPerSegment(Integer.parseInt(maxNumRecordsPerSegment));
    }
    segmentProcessorConfigBuilder.setSegmentConfig(segmentConfigBuilder.build());

    // Generate segment processor config
    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    // Prepare input directory
    File inputSegmentsDir = new File(workingDir, INPUT_SEGMENTS_DIR);
    Preconditions.checkState(inputSegmentsDir.mkdirs(), "Failed to create input directory: %s for task: %s",
        inputSegmentsDir.getAbsolutePath(), taskType);
    for (File indexDir : originalIndexDirs) {
      FileUtils.copyDirectoryToDirectory(indexDir, inputSegmentsDir);
    }

    // Prepare output directory
    File outputSegmentsDir = new File(workingDir, OUTPUT_SEGMENTS_DIR);
    Preconditions.checkState(outputSegmentsDir.mkdirs(), "Failed to create output directory: %s for task: %s",
        outputSegmentsDir.getAbsolutePath(), taskType);

    // Execute segment processor framework for concatenating / rolling up the segments
    SegmentProcessorFramework segmentProcessorFramework =
        new SegmentProcessorFramework(inputSegmentsDir, segmentProcessorConfig, outputSegmentsDir);
    try {
      segmentProcessorFramework.processSegments();
    } finally {
      segmentProcessorFramework.cleanup();
    }
    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs, (endMillis - startMillis));

    List<SegmentConversionResult> results = new ArrayList<>();
    for (File file : outputSegmentsDir.listFiles()) {
      String outputSegmentName = file.getName();
      results.add(new SegmentConversionResult.Builder().setFile(file).setSegmentName(outputSegmentName)
          .setTableNameWithType(tableNameWithType).build());
    }
    return results;
  }
}
