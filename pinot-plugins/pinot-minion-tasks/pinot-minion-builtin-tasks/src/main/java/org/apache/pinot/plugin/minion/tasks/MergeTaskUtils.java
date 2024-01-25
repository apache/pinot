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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.common.MinionConstants.MergeTask;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;


/**
 * Common utils for segment merge tasks.
 */
public class MergeTaskUtils {
  private MergeTaskUtils() {
  }

  private static final int AGGREGATION_TYPE_KEY_SUFFIX_LENGTH = MergeTask.AGGREGATION_TYPE_KEY_SUFFIX.length();

  /**
   * Creates the time handler config based on the given table config, schema and task config. Returns {@code null} if
   * the table does not have a time column.
   */
  @Nullable
  public static TimeHandlerConfig getTimeHandlerConfig(TableConfig tableConfig, Schema schema,
      Map<String, String> taskConfig) {
    String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    if (timeColumn == null) {
      return null;
    }
    DateTimeFieldSpec fieldSpec = schema.getSpecForTimeColumn(timeColumn);
    Preconditions
        .checkState(fieldSpec != null, "No valid spec found for time column: %s in schema for table: %s", timeColumn,
            tableConfig.getTableName());

    TimeHandlerConfig.Builder timeHandlerConfigBuilder = new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH);

    String windowStartMs = taskConfig.get(MergeTask.WINDOW_START_MS_KEY);
    String windowEndMs = taskConfig.get(MergeTask.WINDOW_END_MS_KEY);
    if (windowStartMs != null && windowEndMs != null) {
      timeHandlerConfigBuilder.setTimeRange(Long.parseLong(windowStartMs), Long.parseLong(windowEndMs))
          .setNegateWindowFilter(Boolean.parseBoolean(taskConfig.get(MergeTask.NEGATE_WINDOW_FILTER)));
    }

    String roundBucketTimePeriod = taskConfig.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY);
    if (roundBucketTimePeriod != null) {
      timeHandlerConfigBuilder.setRoundBucketMs(TimeUtils.convertPeriodToMillis(roundBucketTimePeriod));
    }

    String partitionBucketTimePeriod = taskConfig.get(MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY);
    if (partitionBucketTimePeriod != null) {
      timeHandlerConfigBuilder.setPartitionBucketMs(TimeUtils.convertPeriodToMillis(partitionBucketTimePeriod));
    }

    return timeHandlerConfigBuilder.build();
  }

  /**
   * Creates the partitioner configs based on the given table config, schema and task config.
   */
  public static List<PartitionerConfig> getPartitionerConfigs(TableConfig tableConfig, Schema schema,
      Map<String, String> taskConfig) {
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig == null) {
      return Collections.emptyList();
    }
    List<PartitionerConfig> partitionerConfigs = new ArrayList<>();
    Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
    for (Map.Entry<String, ColumnPartitionConfig> entry : columnPartitionMap.entrySet()) {
      String partitionColumn = entry.getKey();
      Preconditions.checkState(schema.hasColumn(partitionColumn),
          "Partition column: %s does not exist in the schema for table: %s", partitionColumn,
          tableConfig.getTableName());
      PartitionerConfig partitionerConfig =
          new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
              .setColumnName(partitionColumn).setColumnPartitionConfig(entry.getValue()).build();
      partitionerConfigs.add(partitionerConfig);
    }
    return partitionerConfigs;
  }

  /**
   * Returns the merge type based on the task config. Returns {@code null} if it is not configured.
   */
  @Nullable
  public static MergeType getMergeType(Map<String, String> taskConfig) {
    String mergeType = taskConfig.get(MergeTask.MERGE_TYPE_KEY);
    return mergeType != null ? MergeType.valueOf(mergeType.toUpperCase()) : null;
  }

  /**
   * Returns the map from column name to the aggregation type associated with it based on the task config.
   */
  public static Map<String, AggregationFunctionType> getAggregationTypes(Map<String, String> taskConfig) {
    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    for (Map.Entry<String, String> entry : taskConfig.entrySet()) {
      String key = entry.getKey();
      if (key.endsWith(MergeTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
        String column = key.substring(0, key.length() - AGGREGATION_TYPE_KEY_SUFFIX_LENGTH);
        aggregationTypes.put(column, AggregationFunctionType.getAggregationFunctionType(entry.getValue()));
      }
    }
    return aggregationTypes;
  }

  /**
   * Returns the segment config based on the task config.
   */
  public static SegmentConfig getSegmentConfig(Map<String, String> taskConfig) {
    SegmentConfig.Builder segmentConfigBuilder = new SegmentConfig.Builder();
    String maxNumRecordsPerSegment = taskConfig.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    if (maxNumRecordsPerSegment != null) {
      segmentConfigBuilder.setMaxNumRecordsPerSegment(Integer.parseInt(maxNumRecordsPerSegment));
    }
    String segmentMapperFileSizeThreshold = taskConfig.get(MergeTask.SEGMENT_MAPPER_FILE_SIZE_IN_BYTES);
    if (segmentMapperFileSizeThreshold != null) {
      segmentConfigBuilder.setIntermediateFileSizeThreshold(Long.parseLong(segmentMapperFileSizeThreshold));
    }
    segmentConfigBuilder.setSegmentNamePrefix(taskConfig.get(MergeTask.SEGMENT_NAME_PREFIX_KEY));
    segmentConfigBuilder.setSegmentNamePostfix(taskConfig.get(MergeTask.SEGMENT_NAME_POSTFIX_KEY));
    segmentConfigBuilder.setFixedSegmentName(taskConfig.get(MergeTask.FIXED_SEGMENT_NAME_KEY));
    return segmentConfigBuilder.build();
  }

  /**
   * Check if the segment can be merged. Only skip merging the segment if 'shouldNotMerge'
   * field exists and is set to true in its segment metadata custom map.
   */
  public static boolean allowMerge(SegmentZKMetadata segmentZKMetadata) {
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    return MapUtils.isEmpty(customMap) || !Boolean
        .parseBoolean(customMap.get(MergeTask.SEGMENT_ZK_METADATA_SHOULD_NOT_MERGE_KEY));
  }
}
