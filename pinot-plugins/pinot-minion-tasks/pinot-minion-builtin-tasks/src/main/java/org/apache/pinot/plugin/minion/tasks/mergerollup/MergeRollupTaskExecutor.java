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
import java.util.TreeMap;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
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
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

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

    // Segment config
    segmentProcessorConfigBuilder.setSegmentConfig(MergeTaskUtils.getSegmentConfig(configs));

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    // Set the number of concurrent tasks per instance
    segmentProcessorConfigBuilder.setNumConcurrentTasksPerInstance(
        Integer.parseInt(configs.getOrDefault(MinionConstants.NUM_CONCURRENT_TASKS_PER_INSTANCE_KEY, "1")));

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    List<RecordReader> recordReaders = new ArrayList<>(numInputSegments);
    int count = 1;
    for (File segmentDir : segmentDirs) {
      _eventObserver.notifyProgress(_pinotTaskConfig,
          String.format("Creating RecordReader for: %s (%d out of %d)", segmentDir, count++, numInputSegments));
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(segmentDir, null, null, true);
      recordReaders.add(recordReader);
    }
    List<File> outputSegmentDirs;
    try {
      _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segments");
      outputSegmentDirs = new SegmentProcessorFramework(recordReaders, segmentProcessorConfig, workingDir).process();
    } finally {
      for (RecordReader recordReader : recordReaders) {
        recordReader.close();
      }
    }

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs, (endMillis - startMillis));
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
}
