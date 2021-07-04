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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task executor that provides merge and rollup service
 *
 * TODO:
 *   1. Add the support for roll-up
 *   2. Add the support for time split to provide backfill support for merged segments
 *   3. Add merge/rollup name prefixes for generated segments
 *   4. Add the support for realtime table
 */
public class MergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskExecutor.class);
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

    Preconditions.checkState(
        MergeType.CONCAT.name().equalsIgnoreCase(configs.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY)),
        "Only 'CONCAT' mode is currently supported.");

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    Map<String, AggregationFunctionType> aggregationTypes = MergeRollupTaskUtils.getRollupAggregationTypes(configs);
    String numRecordsPerSegmentString = configs.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT);

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
            .setMergeType(MergeType.CONCAT);
    if (!aggregationTypes.isEmpty()) {
      segmentProcessorConfigBuilder.setAggregationTypes(aggregationTypes);
    }
    if (numRecordsPerSegmentString != null) {
      segmentProcessorConfigBuilder.setSegmentConfig(
          new SegmentConfig.Builder().setMaxNumRecordsPerSegment(Integer.parseInt(numRecordsPerSegmentString)).build());
    }

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    File inputSegmentsDir = new File(workingDir, INPUT_SEGMENTS_DIR);
    Preconditions.checkState(inputSegmentsDir.mkdirs(), "Failed to create input directory: %s for task: %s",
        inputSegmentsDir.getAbsolutePath(), taskType);
    for (File indexDir : originalIndexDirs) {
      FileUtils.copyDirectoryToDirectory(indexDir, inputSegmentsDir);
    }
    File outputSegmentsDir = new File(workingDir, OUTPUT_SEGMENTS_DIR);
    Preconditions.checkState(outputSegmentsDir.mkdirs(), "Failed to create output directory: %s for task: %s",
        outputSegmentsDir.getAbsolutePath(), taskType);

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

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections
        .singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE + MinionConstants.TASK_BUCKET_GRANULARITY_SUFFIX,
            pinotTaskConfig.getConfigs().get(MinionConstants.MergeRollupTask.GRANULARITY_KEY).toUpperCase()));
  }
}
