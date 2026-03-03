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

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for MaterializedViewTaskGenerator
 *
 */
public class MaterializedViewTaskGeneratorTest {

  private static final String RAW_TABLE_NAME = "baseTable";
  private static final String REALTIME_TABLE_NAME = "baseTable_REALTIME";
  private static final String TIME_COLUMN_NAME = "eventTime";
  private static final String MV_TASK_TYPE = "MaterializedViewTask";

  private final Map<String, String> _streamConfigs = new HashMap<>();

  @BeforeClass
  public void setup() {
    _streamConfigs.put(StreamConfigProperties.STREAM_TYPE, "kafka");
    _streamConfigs.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
        "baseTableTopic");
    _streamConfigs.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.foo.Decoder");
  }

  private TableConfig getRealtimeTableConfig(Map<String, Map<String, String>> taskConfigsMap) {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setStreamConfigs(_streamConfigs)
        .setTaskConfig(new TableTaskConfig(taskConfigsMap))
        .build();
  }

  @Test
  public void testGenerateTasksCheckConfigs() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);

    when(mockClusterInfoAccessor.getTaskStates(MV_TASK_TYPE)).thenReturn(new HashMap<>());

    SegmentZKMetadata segment =
        getSegmentZKMetadata("baseTable__0__0__12345", Status.DONE, 1000, 2000,
            TimeUnit.MILLISECONDS, "downloadUrl");

    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segment));
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, List.of(segment.getSegmentName())));

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    // offline table should be skipped
    TableConfig offlineTable =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    assertTrue(generator.generateTasks(List.of(offlineTable)).isEmpty());

    // null TableTaskConfig
    Assert.expectThrows(IllegalStateException.class, () -> {
      TableConfig realtimeTable = getRealtimeTableConfig(new HashMap<>());
      realtimeTable.setTaskConfig(null);
      generator.generateTasks(List.of(realtimeTable));
    });

    // missing MV task config
    Assert.expectThrows(IllegalStateException.class, () -> {
      TableConfig realtimeTable = getRealtimeTableConfig(new HashMap<>());
      generator.generateTasks(List.of(realtimeTable));
    });
  }

  @Test
  public void testGenerateTasksWithInvalidMvName() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getTaskStates(MV_TASK_TYPE)).thenReturn(new HashMap<>());

    // Prepare one completed realtime segment to avoid early returns in generator
    SegmentZKMetadata segment =
        getSegmentZKMetadata("baseTable__0__0__12345", Status.DONE,
            1000, 2000, TimeUnit.MILLISECONDS, "downloadUrl");

    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(List.of(segment));
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, List.of(segment.getSegmentName())));

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    // ---------- case 1: mvName is missing ----------
    Assert.expectThrows(IllegalStateException.class, () -> {
      Map<String, Map<String, String>> taskConfigs = new HashMap<>();
      taskConfigs.put(MV_TASK_TYPE, new HashMap<>()); // mvName is not specified

      TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigs);
      generator.generateTasks(List.of(realtimeTableConfig));
    });

    // ---------- case 2: mvName is specified, but MV offline table does not exist ----------
    Assert.expectThrows(IllegalStateException.class, () -> {
      when(mockClusterInfoAccessor.hasOfflineTable("mvTable")).thenReturn(false);

      Map<String, Map<String, String>> taskConfigs = new HashMap<>();
      taskConfigs.put(MV_TASK_TYPE,
          Map.of(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable"));

      TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigs);
      generator.generateTasks(List.of(realtimeTableConfig));
    });
  }

  @Test
  public void testGenerateTasksNoSegments() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(MV_TASK_TYPE, Map.of("targetTables", "mvTable1"));

    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getTaskStates(MV_TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(List.of());
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, List.of()));

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    assertTrue(generator.generateTasks(List.of(realtimeTableConfig)).isEmpty());
  }

  /**
   * Test cold start for MaterializedViewTask. No minion metadata exists.
   * Window is calculated based on existing segments. Ensure MV task is generated with required configs:
   * - mvName (and mv offline table exists)
   * - selectedDimensionList (group by fields)
   */
  @Test
  public void testGenerateTasksNoMinionMetadata() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getTaskStates(MinionConstants.MaterializedViewTask.TASK_TYPE))
        .thenReturn(new HashMap<>());
    when(mockClusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MinionConstants.MaterializedViewTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(null);

    // Two DONE segments with download urls (timestamps in milliseconds)
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("baseTable__0__0__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download1");
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("baseTable__1__0__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download2");

    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    // mv offline table must exist before triggering MV task
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable")).thenReturn(true);

    // MV task config (use test-friendly column names)
    Map<String, String> mvTaskConfig = new HashMap<>();
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable");
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,Dim2");
    mvTaskConfig.put("bucketTimePeriod", "1d");
    mvTaskConfig.put("bufferTimePeriod", "0d");
    mvTaskConfig.put("roundBucketTimePeriod", "1d");
    mvTaskConfig.put("mergeType", "MV_ROLLUP");
    mvTaskConfig.put("Metric1.aggregationType", "sum");
    mvTaskConfig.put("maxNumRecordsPerSegment", "4000");
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.FILTER_FUNCTION,
        "Groovy({Dim2 != \"X\"}, Dim2)");

    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(MinionConstants.MaterializedViewTask.TASK_TYPE, mvTaskConfig);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    // Generate tasks and validate
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), MinionConstants.MaterializedViewTask.TASK_TYPE);

    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "baseTable__0__0__12345,baseTable__1__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");

    // MV required configs
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME), "mvTable_OFFLINE");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST), "Dim1,Dim2");

    // Optional passthrough checks (if your generator copies them into task configs)
    assertEquals(configs.get("mergeType"), "MV_ROLLUP");
    assertEquals(configs.get("Metric1.aggregationType"), "sum");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.FILTER_FUNCTION),
        "Groovy({Dim2 != \"X\"}, Dim2)");

    // Window keys: replace these two keys if your generator uses different names/constants
    assertEquals(configs.get("windowStartMs"), "1590019200000");
    assertEquals(configs.get("windowEndMs"), "1590105600000");

    // ---------------- Segment metadata encoded in HOURS ----------------
    segmentZKMetadata1 = getSegmentZKMetadata("baseTable__0__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
            "download1"); // 21 May 2020 8am to 22 May 2020 8am UTC
    segmentZKMetadata2 = getSegmentZKMetadata("baseTable__1__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
            "download2"); // 21 May 2020 8am to 22 May 2020 8am UTC

    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);

    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "baseTable__0__0__12345,baseTable__1__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME), "mvTable_OFFLINE");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST), "Dim1,Dim2");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.FILTER_FUNCTION),
        "Groovy({Dim2 != \"X\"}, Dim2)");
    assertEquals(configs.get("windowStartMs"), "1590019200000");
    assertEquals(configs.get("windowEndMs"), "1590105600000");
  }

  /**
   * Tests for subsequent runs after cold start for MaterializedViewTask, with watermark map stored in
   * MaterializedViewTaskMetadata (keyed by mv offline table name).
   */
  @Test
  public void testGenerateTasksWithMinionMetadata() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getTaskStates(MinionConstants.MaterializedViewTask.TASK_TYPE))
        .thenReturn(new HashMap<>());

    // Config uses raw mvName (no suffix). Depending on implementation, generator may check raw or offline name.
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable")).thenReturn(true);
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable_OFFLINE")).thenReturn(true);

    // Two DONE segments
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("baseTable__0__0__12345", Status.DONE, 1589972400000L, 1590048000000L,
            TimeUnit.MILLISECONDS, "download1"); // ends at 05-21 08:00 UTC
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("baseTable__0__1__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download2"); // 05-21 08:00 -> 05-22 08:00 UTC

    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    // ---------------- Watermark map: mvTable_OFFLINE -> 21 May 2020 00:00 UTC ----------------
    Map<String, Long> watermarkMap = new HashMap<>();
    watermarkMap.put("mvTable", 1590019200000L);

    when(mockClusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MinionConstants.MaterializedViewTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new MaterializedViewTaskMetadata(REALTIME_TABLE_NAME, watermarkMap).toZNRecord());

    // ---------------- Default configs ----------------
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> mvTaskConfig = new HashMap<>();
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable"); // raw
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,Dim2");
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.FILTER_FUNCTION, "Groovy({Dim1 != \"X\"}, Dim1)");
    mvTaskConfig.put("Column1.aggregationType", "sum");
    taskConfigsMap.put(MinionConstants.MaterializedViewTask.TASK_TYPE, mvTaskConfig);

    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), MinionConstants.MaterializedViewTask.TASK_TYPE);

    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "baseTable__0__0__12345,baseTable__0__1__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME), "mvTable_OFFLINE");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST), "Dim1,Dim2");
    assertEquals(configs.get(MinionConstants.MaterializedViewTask.FILTER_FUNCTION),
        "Groovy({Dim1 != \"X\"}, Dim1)");

    // Window assertions (adjust keys if your generator uses different ones)
    assertEquals(configs.get("windowStartMs"), "1590019200000");
    assertEquals(configs.get("windowEndMs"), "1590105600000");

    // ---------------- No segments match: watermark is far ahead ----------------
    Map<String, Long> watermarkMapNoMatch = new HashMap<>();
    watermarkMapNoMatch.put("mvTable", 1590490800000L); // 26 May 2020 UTC

    when(mockClusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MinionConstants.MaterializedViewTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new MaterializedViewTaskMetadata(REALTIME_TABLE_NAME, watermarkMapNoMatch).toZNRecord());

    generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);

    // ---------------- Some segments match: smaller bucket ----------------
    // Reset watermark back
    when(mockClusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MinionConstants.MaterializedViewTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new MaterializedViewTaskMetadata(REALTIME_TABLE_NAME, watermarkMap).toZNRecord());

    taskConfigsMap = new HashMap<>();
    mvTaskConfig = new HashMap<>();
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable");
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,Dim2");
    mvTaskConfig.put("bucketTimePeriod", "2h");
    mvTaskConfig.put("Column1.aggregationType", "sum");
    taskConfigsMap.put(MinionConstants.MaterializedViewTask.TASK_TYPE, mvTaskConfig);
    realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    // reuse generator (same style as RealtimeToOffline test)
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);

    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "baseTable__0__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1");
    assertEquals(configs.get("windowStartMs"), "1590019200000");
    assertEquals(configs.get("windowEndMs"), "1590026400000");

    // ---------------- Segment processor configs passthrough ----------------
    taskConfigsMap = new HashMap<>();
    mvTaskConfig = new HashMap<>();
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable");
    mvTaskConfig.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,Dim2");
    mvTaskConfig.put("roundBucketTimePeriod", "1h");
    mvTaskConfig.put("mergeType", "MV_ROLLUP");
    mvTaskConfig.put("Column1.aggregationType", "SUM");
    taskConfigsMap.put(MinionConstants.MaterializedViewTask.TASK_TYPE, mvTaskConfig);
    realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);

    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "baseTable__0__0__12345,baseTable__0__1__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get("roundBucketTimePeriod"), "1h");
    assertEquals(configs.get("mergeType"), "MV_ROLLUP");
    assertEquals(configs.get("Column1.aggregationType"), "SUM");
    assertEquals(configs.get("windowStartMs"), "1590019200000");
    assertEquals(configs.get("windowEndMs"), "1590105600000");
  }

  @Test
  public void testMaterializedViewTaskConfig() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockClusterInfoAccessor);

    // ---------------- Base schema ----------------
    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName("baseTable")
        .addSingleValueDimension("Dim1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Dim2", FieldSpec.DataType.STRING)
        .addMetric("Column1", FieldSpec.DataType.LONG)
        .build();

    // ---------------- MV schema (offline) ----------------
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName("mvTable_OFFLINE")
        .addSingleValueDimension("Dim1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Dim2", FieldSpec.DataType.STRING)
        .addMetric("Column1", FieldSpec.DataType.LONG) // output column exists by default naming rule
        .build();

    // Common valid task config
    Map<String, String> validConfig = new HashMap<>();
    validConfig.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable"); // raw, validate will append _OFFLINE
    validConfig.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,Dim2");
    validConfig.put(MinionConstants.MaterializedViewTask.FILTER_FUNCTION, "Groovy({Dim1 != \"X\"}, Dim1)");
    validConfig.put(MinionConstants.MaterializedViewTask.MERGE_TYPE_KEY, "MV_ROLLUP");
    validConfig.put(MinionConstants.MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    validConfig.put(MinionConstants.MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "0d");
    validConfig.put(MinionConstants.MaterializedViewTask.ROUND_BUCKET_TIME_PERIOD_KEY, "1h");
    validConfig.put("Column1" + MinionConstants.MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX, "sum");

    // Dummy table config (not used by current validate logic besides signature)
    TableConfig dummyTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("baseTable").build();

    // ---------- case 0: valid config ----------
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable_OFFLINE")).thenReturn(true);
    when(mockClusterInfoAccessor.getTableSchema("mvTable_OFFLINE")).thenReturn(mvSchema);

    generator.validateTaskConfigs(dummyTableConfig, baseSchema, validConfig);

    // ---------- case 1: selectedDimensionList column not found in base schema ----------
    Map<String, String> invalidSelectedDim = new HashMap<>(validConfig);
    invalidSelectedDim.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, "Dim1,NotExistDim");
    Assert.expectThrows(IllegalStateException.class,
        () -> generator.validateTaskConfigs(dummyTableConfig, baseSchema, invalidSelectedDim));

    // ---------- case 2: aggregation source column not found in base schema ----------
    Map<String, String> invalidAggSource = new HashMap<>(validConfig);
    invalidAggSource.remove("Column1" + MinionConstants.MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX);
    invalidAggSource.put("NotExistMetric" + MinionConstants.MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX, "sum");
    Assert.expectThrows(IllegalStateException.class,
        () -> generator.validateTaskConfigs(dummyTableConfig, baseSchema, invalidAggSource));

    // ---------- case 3: aggregation output column not found in MV schema ----------
    Schema mvSchemaMissingOutput = new Schema.SchemaBuilder()
        .setSchemaName("mvTable_OFFLINE")
        .addSingleValueDimension("Dim1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Dim2", FieldSpec.DataType.STRING)
        // Column1 missing on purpose
        .build();
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable_OFFLINE")).thenReturn(true);
    when(mockClusterInfoAccessor.getTableSchema("mvTable_OFFLINE")).thenReturn(mvSchemaMissingOutput);

    Assert.expectThrows(IllegalStateException.class,
        () -> generator.validateTaskConfigs(dummyTableConfig, baseSchema, validConfig));

    // ---------- case 4: mv offline table does not exist ----------
    when(mockClusterInfoAccessor.hasOfflineTable("mvTable_OFFLINE")).thenReturn(false);

    Assert.expectThrows(IllegalStateException.class,
        () -> generator.validateTaskConfigs(dummyTableConfig, baseSchema, validConfig));

    // ---------- case 5: invalid mergeType (only MV_ROLLUP is supported) ----------
    Map<String, String> invalidMergeTypeConfig = new HashMap<>(validConfig);
    invalidMergeTypeConfig.put(
        MinionConstants.MaterializedViewTask.MERGE_TYPE_KEY, "ROLLUP"); // or any random value

    Assert.expectThrows(IllegalStateException.class,
        () -> generator.validateTaskConfigs(dummyTableConfig, baseSchema, invalidMergeTypeConfig));
  }

  // ---------------- helper methods ----------------

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName, Status status,
      long startTime, long endTime, TimeUnit timeUnit, String downloadURL) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
    metadata.setStatus(status);
    metadata.setStartTime(startTime);
    metadata.setEndTime(endTime);
    metadata.setTimeUnit(timeUnit);
    metadata.setDownloadUrl(downloadURL);
    return metadata;
  }

  private IdealState getIdealState(String tableName, List<String> segmentNames) {
    IdealState idealState = new IdealState(tableName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    for (String segmentName : segmentNames) {
      idealState.setPartitionState(segmentName, "Server_0", "ONLINE");
    }
    return idealState;
  }
}