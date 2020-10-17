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
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A task to convert segments from a REALTIME table to segments for its corresponding OFFLINE table.
 * The realtime segments could span across multiple time windows.
 * This task extracts data and creates segments for a configured time range.
 * The {@link SegmentProcessorFramework} is used for the segment conversion, which also does
 * 1. time window extraction using filter function
 * 2. time column rollup
 * 3. partitioning using table config's segmentPartitioningConfig
 * 4. aggregations and rollup
 * 5. data sorting
 *
 * Before beginning the task, the <code>watermarkMs</code> is checked in the minion task metadata ZNode,
 * located at MINION_TASK_METADATA/RealtimeToOfflineSegmentsTask/<tableNameWithType>
 * It should match the <code>windowStartMs</code>.
 * The version of the znode is cached.
 *
 * After the segments are uploaded, this task updates the <code>watermarkMs</code> in the minion task metadata ZNode.
 * The znode version is checked during update,
 * and update only succeeds if version matches with the previously cached version
 */
public class RealtimeToOfflineSegmentsTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeToOfflineSegmentsTaskExecutor.class);
  private static final String INPUT_SEGMENTS_DIR = "input_segments";
  private static final String OUTPUT_SEGMENTS_DIR = "output_segments";

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private int _expectedVersion = Integer.MIN_VALUE;
  private long _nextWatermark;

  public RealtimeToOfflineSegmentsTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager) {
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
  }

  /**
   * Fetches the RealtimeToOfflineSegmentsTask metadata ZNode for the realtime table.
   * Checks that the <code>watermarkMs</code> from the ZNode matches the windowStartMs in the task configs.
   * If yes, caches the ZNode version to check during update.
   */
  @Override
  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);

    ZNRecord realtimeToOfflineSegmentsTaskZNRecord =
        _minionTaskZkMetadataManager.getRealtimeToOfflineSegmentsTaskZNRecord(realtimeTableName);
    Preconditions.checkState(realtimeToOfflineSegmentsTaskZNRecord != null,
        "RealtimeToOfflineSegmentsTaskMetadata ZNRecord for table: %s should not be null. Exiting task.",
        realtimeTableName);

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(realtimeToOfflineSegmentsTaskZNRecord);
    long windowStartMs = Long.parseLong(configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY));
    Preconditions.checkState(realtimeToOfflineSegmentsTaskMetadata.getWatermarkMs() == windowStartMs,
        "watermarkMs in RealtimeToOfflineSegmentsTask metadata: %s does not match windowStartMs: %d in task configs for table: %s. "
            + "ZNode may have been modified by another task", realtimeToOfflineSegmentsTaskMetadata, windowStartMs,
        realtimeTableName);

    _expectedVersion = realtimeToOfflineSegmentsTaskZNRecord.getVersion();
  }

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> originalIndexDirs,
      File workingDir)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig = getTableConfig(offlineTableName);
    Schema schema = getSchema(offlineTableName);
    Set<String> schemaColumns = schema.getPhysicalColumnNames();
    String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumn);
    Preconditions
        .checkState(dateTimeFieldSpec != null, "No valid spec found for time column: %s in schema for table: %s",
            timeColumn, offlineTableName);

    long windowStartMs = Long.parseLong(configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY));
    _nextWatermark = windowEndMs;

    String timeColumnTransformFunction =
        configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY);
    String collectorTypeStr = configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY);
    Map<String, String> aggregatorConfigs = new HashMap<>();
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      String key = entry.getKey();
      if (key.endsWith(MinionConstants.RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
        String column = key.split(MinionConstants.RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX)[0];
        aggregatorConfigs.put(column, entry.getValue());
      }
    }
    String numRecordsPerSegment =
        configs.get(MinionConstants.RealtimeToOfflineSegmentsTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Time rollup using configured time transformation function
    if (timeColumnTransformFunction != null) {
      RecordTransformerConfig recordTransformerConfig =
          getRecordTransformerConfigForTime(timeColumnTransformFunction, timeColumn);
      segmentProcessorConfigBuilder.setRecordTransformerConfig(recordTransformerConfig);
    }

    // Filter function for extracting data between start and end time window
    RecordFilterConfig recordFilterConfig =
        getRecordFilterConfigForWindow(windowStartMs, windowEndMs, dateTimeFieldSpec, timeColumn);
    segmentProcessorConfigBuilder.setRecordFilterConfig(recordFilterConfig);

    // Partitioner config from tableConfig
    if (tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap =
          tableConfig.getIndexingConfig().getSegmentPartitionConfig().getColumnPartitionMap();
      PartitionerConfig partitionerConfig = getPartitionerConfig(columnPartitionMap, offlineTableName, schemaColumns);
      segmentProcessorConfigBuilder.setPartitionerConfigs(Lists.newArrayList(partitionerConfig));
    }

    // Aggregations using configured Collector
    List<String> sortedColumns = tableConfig.getIndexingConfig().getSortedColumn();
    CollectorConfig collectorConfig =
        getCollectorConfig(collectorTypeStr, aggregatorConfigs, schemaColumns, sortedColumns);
    segmentProcessorConfigBuilder.setCollectorConfig(collectorConfig);

    // Segment config
    if (numRecordsPerSegment != null) {
      SegmentConfig segmentConfig = getSegmentConfig(numRecordsPerSegment);
      segmentProcessorConfigBuilder.setSegmentConfig(segmentConfig);
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
          .setTableNameWithType(offlineTableName).build());
    }
    return results;
  }

  /**
   * Fetches the RealtimeToOfflineSegmentsTask metadata ZNode for the realtime table.
   * Checks that the version of the ZNode matches with the version cached earlier. If yes, proceeds to update watermark in the ZNode
   * TODO: Making the minion task update the ZK metadata is an anti-pattern, however cannot see another way to do it
   */
  @Override
  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    String realtimeTableName = pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY);
    RealtimeToOfflineSegmentsTaskMetadata newMinionMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata(realtimeTableName, _nextWatermark);
    _minionTaskZkMetadataManager.setRealtimeToOfflineSegmentsTaskMetadata(newMinionMetadata, _expectedVersion);
  }

  /**
   * Construct a {@link RecordTransformerConfig} for time column transformation
   */
  private RecordTransformerConfig getRecordTransformerConfigForTime(String timeColumnTransformFunction,
      String timeColumn) {
    Map<String, String> transformationsMap = new HashMap<>();
    transformationsMap.put(timeColumn, timeColumnTransformFunction);
    return new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformationsMap).build();
  }

  /**
   * Construct a {@link RecordFilterConfig} by setting a filter function on the time column, for extracting data between window start/end
   */
  private RecordFilterConfig getRecordFilterConfigForWindow(long windowStartMs, long windowEndMs,
      DateTimeFieldSpec dateTimeFieldSpec, String timeColumn) {
    String filterFunction;
    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
    TimeUnit timeUnit = dateTimeFormatSpec.getColumnUnit();
    DateTimeFieldSpec.TimeFormat timeFormat = dateTimeFormatSpec.getTimeFormat();
    if (timeUnit.equals(TimeUnit.MILLISECONDS) && timeFormat.equals(DateTimeFieldSpec.TimeFormat.EPOCH)) {
      // If time column is in EPOCH millis, use windowStart and windowEnd directly to filter
      filterFunction = getFilterFunctionLong(windowStartMs, windowEndMs, timeColumn);
    } else {
      // Convert windowStart and windowEnd to time format of the data
      if (dateTimeFormatSpec.getTimeFormat().equals(DateTimeFieldSpec.TimeFormat.EPOCH)) {
        long windowStart = dateTimeFormatSpec.fromMillisToFormat(windowStartMs, Long.class);
        long windowEnd = dateTimeFormatSpec.fromMillisToFormat(windowEndMs, Long.class);
        filterFunction = getFilterFunctionLong(windowStart, windowEnd, timeColumn);
      } else {
        String windowStart = dateTimeFormatSpec.fromMillisToFormat(windowStartMs, String.class);
        String windowEnd = dateTimeFormatSpec.fromMillisToFormat(windowEndMs, String.class);
        if (dateTimeFieldSpec.getDataType().isNumeric()) {
          filterFunction = getFilterFunctionLong(Long.parseLong(windowStart), Long.parseLong(windowEnd), timeColumn);
        } else {
          filterFunction = getFilterFunctionString(windowStart, windowEnd, timeColumn);
        }
      }
    }
    return new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
        .setFilterFunction(filterFunction).build();
  }

  /**
   * Construct a {@link PartitionerConfig} using {@link org.apache.pinot.spi.config.table.SegmentPartitionConfig} from the table config
   */
  private PartitionerConfig getPartitionerConfig(Map<String, ColumnPartitionConfig> columnPartitionMap,
      String tableNameWithType, Set<String> schemaColumns) {
    Preconditions.checkState(columnPartitionMap.size() == 1,
        "Cannot partition using more than 1 ColumnPartitionConfig for table: %s", tableNameWithType);
    String partitionColumn = columnPartitionMap.keySet().iterator().next();
    Preconditions.checkState(schemaColumns.contains(partitionColumn),
        "Partition column: %s is not a physical column in the schema", partitionColumn);
    return new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
        .setColumnName(partitionColumn).setColumnPartitionConfig(columnPartitionMap.get(partitionColumn)).build();
  }

  /**
   * Construct a {@link CollectorConfig} using configured collector configs and sorted columns from table config
   */
  private CollectorConfig getCollectorConfig(String collectorTypeStr, Map<String, String> aggregateConfigs,
      Set<String> schemaColumns, List<String> sortedColumns) {
    CollectorFactory.CollectorType collectorType = collectorTypeStr == null ? CollectorFactory.CollectorType.CONCAT
        : CollectorFactory.CollectorType.valueOf(collectorTypeStr.toUpperCase());

    Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap = new HashMap<>();
    for (Map.Entry<String, String> entry : aggregateConfigs.entrySet()) {
      String column = entry.getKey();
      Preconditions
          .checkState(schemaColumns.contains(column), "Aggregate column: %s is not a physical column in the schema",
              column);
      aggregatorTypeMap.put(column, ValueAggregatorFactory.ValueAggregatorType.valueOf(entry.getValue().toUpperCase()));
    }

    if (sortedColumns != null) {
      for (String column : sortedColumns) {
        Preconditions
            .checkState(schemaColumns.contains(column), "Sorted column: %s is not a physical column in the schema",
                column);
      }
    }
    return new CollectorConfig.Builder().setCollectorType(collectorType).setAggregatorTypeMap(aggregatorTypeMap)
        .setSortOrder(sortedColumns).build();
  }

  /**
   * Construct a {@link SegmentConfig} using config values
   */
  private SegmentConfig getSegmentConfig(String numRecordsPerSegment) {
    return new SegmentConfig.Builder().setMaxNumRecordsPerSegment(Integer.parseInt(numRecordsPerSegment)).build();
  }

  /**
   * Construct a {@link org.apache.pinot.core.operator.transform.function.GroovyTransformFunction} string for extracting records between
   * windowStart inclusive and windowEnd exclusive using the time column, where time column is a INT/LONG column
   */
  private String getFilterFunctionLong(long windowStart, long windowEnd, String timeColumn) {
    return String
        .format("Groovy({%s < %d || %s >= %d}, %s)", timeColumn, windowStart, timeColumn, windowEnd, timeColumn);
  }

  /**
   * Construct a {@link org.apache.pinot.core.operator.transform.function.GroovyTransformFunction} string for extracting records between
   * windowStart inclusive and windowEnd exclusive using the time column, where time column is a STRING column
   */
  private String getFilterFunctionString(String windowStart, String windowEnd, String timeColumn) {
    return String.format("Groovy({%s < \"%s\" || %s >= \"%s\"}, %s)", timeColumn, windowStart, timeColumn, windowEnd,
        timeColumn);
  }
}
