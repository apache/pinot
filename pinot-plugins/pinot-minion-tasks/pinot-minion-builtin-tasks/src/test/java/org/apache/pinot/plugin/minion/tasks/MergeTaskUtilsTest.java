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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MergeTaskUtilsTest {

  @Test
  public void testGetTimeHandlerConfig() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("dateTime").build();
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("dateTime", DataType.LONG, "1:SECONDS:SIMPLE_DATE_FORMAT:yyyyMMddHHmmss", "1:SECONDS").build();
    Map<String, String> taskConfig = new HashMap<>();
    long expectedWindowStartMs = 1625097600000L;
    long expectedWindowEndMs = 1625184000000L;
    taskConfig.put(MergeTask.WINDOW_START_MS_KEY, Long.toString(expectedWindowStartMs));
    taskConfig.put(MergeTask.WINDOW_END_MS_KEY, Long.toString(expectedWindowEndMs));
    long expectedRoundBucketMs = 6 * 3600 * 1000;
    taskConfig.put(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY, "6h");
    long expectedPartitionBucketMs = 24 * 3600 * 1000;
    taskConfig.put(MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY, "1d");

    TimeHandlerConfig timeHandlerConfig = MergeTaskUtils.getTimeHandlerConfig(tableConfig, schema, taskConfig);
    assertNotNull(timeHandlerConfig);
    assertEquals(timeHandlerConfig.getType(), TimeHandler.Type.EPOCH);
    assertEquals(timeHandlerConfig.getStartTimeMs(), expectedWindowStartMs);
    assertEquals(timeHandlerConfig.getEndTimeMs(), expectedWindowEndMs);
    assertEquals(timeHandlerConfig.getRoundBucketMs(), expectedRoundBucketMs);
    assertEquals(timeHandlerConfig.getPartitionBucketMs(), expectedPartitionBucketMs);

    // No time column in table config
    TableConfig tableConfigWithoutTimeColumn =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    assertNull(MergeTaskUtils.getTimeHandlerConfig(tableConfigWithoutTimeColumn, schema, taskConfig));

    // Time column does not exist in schema
    Schema schemaWithoutTimeColumn = new Schema.SchemaBuilder().build();
    try {
      MergeTaskUtils.getTimeHandlerConfig(tableConfig, schemaWithoutTimeColumn, taskConfig);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testGetPartitionerConfigs() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setSegmentPartitionConfig(
            new SegmentPartitionConfig(Collections.singletonMap("memberId", new ColumnPartitionConfig("murmur", 10))))
        .build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("memberId", DataType.LONG).build();
    Map<String, String> taskConfig = Collections.emptyMap();

    List<PartitionerConfig> partitionerConfigs = MergeTaskUtils.getPartitionerConfigs(tableConfig, schema, taskConfig);
    assertEquals(partitionerConfigs.size(), 1);
    PartitionerConfig partitionerConfig = partitionerConfigs.get(0);
    assertEquals(partitionerConfig.getPartitionerType(), PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG);
    assertEquals(partitionerConfig.getColumnName(), "memberId");
    ColumnPartitionConfig columnPartitionConfig = partitionerConfig.getColumnPartitionConfig();
    assertEquals(columnPartitionConfig.getFunctionName(), "murmur");
    assertEquals(columnPartitionConfig.getNumPartitions(), 10);

    // Table with multiple partition columns.
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put("memberId", new ColumnPartitionConfig("murmur", 10));
    columnPartitionConfigMap.put("memberName", new ColumnPartitionConfig("HashCode", 5));
    TableConfig tableConfigWithMultiplePartitionColumns =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
            .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap)).build();
    Schema schemaWithMultipleColumns = new Schema.SchemaBuilder().addSingleValueDimension("memberId", DataType.LONG)
        .addSingleValueDimension("memberName", DataType.STRING).build();
    partitionerConfigs =
        MergeTaskUtils.getPartitionerConfigs(tableConfigWithMultiplePartitionColumns, schemaWithMultipleColumns,
            taskConfig);
    assertEquals(partitionerConfigs.size(), 2);

    // No partition column in table config
    TableConfig tableConfigWithoutPartitionColumn =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    assertTrue(MergeTaskUtils.getPartitionerConfigs(tableConfigWithoutPartitionColumn, schema, taskConfig).isEmpty());

    // Partition column does not exist in schema
    Schema schemaWithoutPartitionColumn = new Schema.SchemaBuilder().build();
    try {
      MergeTaskUtils.getPartitionerConfigs(tableConfig, schemaWithoutPartitionColumn, taskConfig);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testGetMergeType() {
    assertEquals(MergeTaskUtils.getMergeType(Collections.singletonMap(MergeTask.MERGE_TYPE_KEY, "concat")),
        MergeType.CONCAT);
    assertEquals(MergeTaskUtils.getMergeType(Collections.singletonMap(MergeTask.MERGE_TYPE_KEY, "Rollup")),
        MergeType.ROLLUP);
    assertEquals(MergeTaskUtils.getMergeType(Collections.singletonMap(MergeTask.MERGE_TYPE_KEY, "DeDuP")),
        MergeType.DEDUP);
    assertNull(MergeTaskUtils.getMergeType(Collections.emptyMap()));

    try {
      MergeTaskUtils.getMergeType(Collections.singletonMap(MergeTask.MERGE_TYPE_KEY, "unsupported"));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testGetAggregationTypes() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("colA.aggregationType", "sum");
    taskConfig.put("colB.aggregationType", "Min");
    taskConfig.put("colC.aggregationType", "MaX");

    Map<String, AggregationFunctionType> aggregationTypes = MergeTaskUtils.getAggregationTypes(taskConfig);
    assertEquals(aggregationTypes.size(), 3);
    assertEquals(aggregationTypes.get("colA"), AggregationFunctionType.SUM);
    assertEquals(aggregationTypes.get("colB"), AggregationFunctionType.MIN);
    assertEquals(aggregationTypes.get("colC"), AggregationFunctionType.MAX);

    taskConfig.put("colD.aggregationType", "unsupported");
    try {
      MergeTaskUtils.getAggregationTypes(taskConfig);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testGetSegmentConfig() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "10000");
    taskConfig.put(MergeTask.SEGMENT_NAME_PREFIX_KEY, "myPrefix");
    taskConfig.put(MergeTask.SEGMENT_NAME_POSTFIX_KEY, "myPostfix");
    taskConfig.put(MergeTask.FIXED_SEGMENT_NAME_KEY, "mySegment");
    taskConfig.put(MergeTask.SEGMENT_MAPPER_FILE_SIZE_IN_BYTES, "1000000000");
    SegmentConfig segmentConfig = MergeTaskUtils.getSegmentConfig(taskConfig);
    assertEquals(segmentConfig.getMaxNumRecordsPerSegment(), 10000);
    assertEquals(segmentConfig.getSegmentNamePrefix(), "myPrefix");
    assertEquals(segmentConfig.getSegmentNamePostfix(), "myPostfix");
    assertEquals(segmentConfig.getSegmentNamePostfix(), "myPostfix");
    assertEquals(segmentConfig.getFixedSegmentName(), "mySegment");
    assertEquals(segmentConfig.getIntermediateFileSizeThreshold(), 1000000000L);
    assertEquals(segmentConfig.toString(),
        "SegmentConfig{_maxNumRecordsPerSegment=10000, _segmentMapperFileSizeThresholdInBytes=1000000000, "
            + "_segmentNamePrefix='myPrefix', _segmentNamePostfix='myPostfix', _fixedSegmentName='mySegment'}");

    segmentConfig = MergeTaskUtils.getSegmentConfig(Collections.emptyMap());
    assertEquals(segmentConfig.getMaxNumRecordsPerSegment(), SegmentConfig.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT);
    assertNull(segmentConfig.getSegmentNamePrefix());
    assertNull(segmentConfig.getSegmentNamePostfix());
    assertNull(segmentConfig.getFixedSegmentName());
  }

  @Test
  public void testAllowMerge() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("seg01");
    assertNull(segmentZKMetadata.getCustomMap());
    assertTrue(MergeTaskUtils.allowMerge(segmentZKMetadata));

    segmentZKMetadata.setCustomMap(Collections.emptyMap());
    assertTrue(MergeTaskUtils.allowMerge(segmentZKMetadata));

    segmentZKMetadata
        .setCustomMap(Collections.singletonMap(MergeTask.SEGMENT_ZK_METADATA_SHOULD_NOT_MERGE_KEY, "false"));
    assertTrue(MergeTaskUtils.allowMerge(segmentZKMetadata));

    segmentZKMetadata
        .setCustomMap(Collections.singletonMap(MergeTask.SEGMENT_ZK_METADATA_SHOULD_NOT_MERGE_KEY, "true"));
    assertFalse(MergeTaskUtils.allowMerge(segmentZKMetadata));
  }
}
