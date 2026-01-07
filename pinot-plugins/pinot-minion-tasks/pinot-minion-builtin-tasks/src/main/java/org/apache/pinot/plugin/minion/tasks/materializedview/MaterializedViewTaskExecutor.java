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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.MaterializedViewProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.MaterializedViewProcessorFramework;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MergeTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task to convert segments from a REALTIME table to segments for its corresponding Agg table.
 * The realtime segments could span across multiple time windows.
 * This task extracts data and creates segments for a configured time window.
 * The {@link MaterializedViewProcessorFramework} is used for the segment conversion, which does the following steps
 * 1. Filter records based on the time window
 * 2. Round the time value in the records (optional)
 * 3. Partition the records if partitioning is enabled in the table config
 * 4. Merge records based on the merge type
 * 5. Sort records if sorting is enabled in the table config
 *
 * Before beginning the task, the <code>watermarkMs</code> is checked in the minion task metadata ZNode,
 * located at MINION_TASK_METADATA/MaterializedViewTask/<tableNameWithType>
 * It should match the <code>windowStartMs</code>.
 * The version of the znode is cached.
 *
 * After the segments are uploaded, this task updates the <code>watermarkMs</code> in the minion task metadata ZNode.
 * The znode version is checked during update,
 * and update only succeeds if version matches with the previously cached version
 */
public class MaterializedViewTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private int _expectedVersion = Integer.MIN_VALUE;
  private static final String COMMA_SEPARATOR = ",";


  public MaterializedViewTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf) {
    super(minionConf);
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
  }

  /**
   * Fetches the MaterializedViewTask metadata ZNode for the realtime table.
   * Checks that the <code>watermarkMs</code> from the ZNode matches the windowStartMs in the task configs.
   * If yes, caches the ZNode version to check during update.
   */
  @Override
  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String taskType = pinotTaskConfig.getTaskType();
    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String mvTableName = configs.get(MaterializedViewTask.MATERIALIZED_VIEW_MARK);

    ZNRecord materializedViewTaskZNRecord =
        _minionTaskZkMetadataManager.getTaskMetadataZNRecord(realtimeTableName, taskType);
    Preconditions.checkState(materializedViewTaskZNRecord != null,
        "MaterializedViewTaskMetadata ZNRecord for table: %s should not be null. Exiting task.",
        realtimeTableName);

    MaterializedViewTaskMetadata materializedViewTaskMetadata =
        MaterializedViewTaskMetadata.fromZNRecord(materializedViewTaskZNRecord);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    Preconditions.checkState(materializedViewTaskMetadata.getWatermarkMap().get(mvTableName) == windowStartMs,
        "watermarkMs in MaterializedViewTask metadata: %s does not match windowStartMs: %d in task configs "
            + "for table: %s. "
            + "ZNode may have been modified by another task", materializedViewTaskMetadata, windowStartMs,
        realtimeTableName);

    _expectedVersion = materializedViewTaskZNRecord.getVersion();
  }

  @Override
  public String getOutputTableNameWithType(Map<String, String> taskConfigs) {
    return taskConfigs.get(MaterializedViewTask.MATERIALIZED_VIEW_NAME);
  }

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
      File workingDir)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String mvNameWithType =
        TableNameBuilder.OFFLINE.tableNameWithType(configs.get(MaterializedViewTask.MATERIALIZED_VIEW_NAME));
    TableConfig tableConfig = getTableConfig(realtimeTableName);
    Schema schema = getSchema(realtimeTableName);

    TableConfig mvTableConfig = getTableConfig(mvNameWithType);
    Schema mvSchema = getSchema(mvNameWithType);
    String selectedDimensionListStr = configs.get(MaterializedViewTask.SELECTED_DIMENSION_LIST);
    Preconditions.checkArgument(StringUtils.isNotBlank(selectedDimensionListStr),
        "Please specify selectedDimensionList explicitly!");
    Set<String> selectedDimensionList =
        Arrays.asList(StringUtils.split(selectedDimensionListStr, COMMA_SEPARATOR))
            .stream().map(s -> s.trim()).filter(s -> StringUtils.isNotBlank(s))
            .collect(Collectors.toSet());
    FilterConfig filterConfig = new FilterConfig(configs.getOrDefault(MaterializedViewTask.FILTER_FUNCTION, null));

    MaterializedViewProcessorConfig.Builder mvProcessorConfigBuilder =
        new MaterializedViewProcessorConfig.Builder()
            .setMvTableConfig(mvTableConfig).setMvSchema(mvSchema)
            .setSelectedDimensions(selectedDimensionList).setFilterConfig(filterConfig)
            .setAggregationFunctionSetMap(MergeTaskUtils.getAggregationTypesMap(configs));

    MaterializedViewProcessorConfig mvProcessorConfig = mvProcessorConfigBuilder.build();

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Time handler config
    segmentProcessorConfigBuilder
        .setTimeHandlerConfig(MergeTaskUtils.getTimeHandlerConfig(tableConfig, schema, configs));

    // Partitioner config
    segmentProcessorConfigBuilder
        .setPartitionerConfigs(MergeTaskUtils.getPartitionerConfigs(tableConfig, schema, configs));

    // Merge type
    MergeType mergeType = MergeTaskUtils.getMergeType(configs);
    segmentProcessorConfigBuilder.setMergeType(mergeType);

    // Materialize View config
    segmentProcessorConfigBuilder
        .setMaterializedViewProcessorConfig(mvProcessorConfig);

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    List<RecordReader> recordReaders = new ArrayList<>(segmentDirs.size());
    for (File segmentDir : segmentDirs) {
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(segmentDir, null, null, true);
      recordReaders.add(recordReader);
    }
    List<File> outputSegmentDirs;
    try {
      outputSegmentDirs =
          new MaterializedViewProcessorFramework(recordReaders, segmentProcessorConfig, workingDir).process();
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
          .setTableNameWithType(mvNameWithType).build());
    }
    return results;
  }

  /**
   * Fetches the MaterializedViewTask metadata ZNode for the realtime table.
   * Checks that the version of the ZNode matches with the version cached earlier. If yes, proceeds to update
   * watermark in the ZNode
   * TODO: Making the minion task update the ZK metadata is an anti-pattern, however cannot see another way to do it
   */
  @Override
  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String taskType = pinotTaskConfig.getTaskType();
    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String mvName = configs.get(MaterializedViewTask.MATERIALIZED_VIEW_MARK);
    ZNRecord materializedViewTaskZNRecord =
        _minionTaskZkMetadataManager.getTaskMetadataZNRecord(realtimeTableName, taskType);
    Preconditions.checkState(materializedViewTaskZNRecord != null,
        "MaterializedViewTask ZNRecord for table: %s should not be null. Exiting task.",
        realtimeTableName);
    MaterializedViewTaskMetadata newMinionMetadata =
        MaterializedViewTaskMetadata.fromZNRecord(materializedViewTaskZNRecord);
    long waterMarkMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));
    newMinionMetadata.getWatermarkMap().put(mvName, waterMarkMs);
    _minionTaskZkMetadataManager.setTaskMetadataZNRecord(newMinionMetadata, taskType, _expectedVersion);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.emptyMap());
  }
}
