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
package org.apache.pinot.core.data.manager;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


@Test
public class BaseTableDataManagerNeedRefreshTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BaseTableDataManagerNeedRefreshTest");
  private static final String DEFAULT_TABLE_NAME = "mytable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(DEFAULT_TABLE_NAME);
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, OFFLINE_TABLE_NAME);

  private static final String DEFAULT_TIME_COLUMN_NAME = "DaysSinceEpoch";
  private static final String MS_SINCE_EPOCH_COLUMN_NAME = "MilliSecondsSinceEpoch";
  private static final String H3_INDEX_COLUMN = "h3Column";
  private static final Map<String, String> H3_INDEX_PROPERTIES = Collections.singletonMap("resolutions", "5");
  private static final String TEXT_INDEX_COLUMN = "textColumn";
  private static final String TEXT_INDEX_COLUMN_MV = "textColumnMV";
  private static final String PARTITIONED_COLUMN_NAME = "partitionedColumn";
  private static final int NUM_PARTITIONS = 20; // For modulo function
  private static final int PARTITION_DIVISOR = 5; // Allowed partition values
  private static final String PARTITION_FUNCTION_NAME = "MoDuLo";

  private static final String NULL_INDEX_COLUMN = "nullField";

  private static final String JSON_INDEX_COLUMN = "jsonField";
  private static final String FST_TEST_COLUMN = "DestCityName";

  private static final TableConfig TABLE_CONFIG;
  private static final Schema SCHEMA;
  private static final ImmutableSegmentDataManager IMMUTABLE_SEGMENT_DATA_MANAGER;
  private static final BaseTableDataManager BASE_TABLE_DATA_MANAGER;

  static {
    try {
      TABLE_CONFIG = getTableConfigBuilder().build();
      SCHEMA = getSchema();
      IMMUTABLE_SEGMENT_DATA_MANAGER =
          createImmutableSegmentDataManager(TABLE_CONFIG, SCHEMA, "basicSegment", generateRows());
      BASE_TABLE_DATA_MANAGER = BaseTableDataManagerTest.createTableManager();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static TableConfigBuilder getTableConfigBuilder() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME)
        .setTimeColumnName(DEFAULT_TIME_COLUMN_NAME).setNullHandlingEnabled(true)
        .setNoDictionaryColumns(Collections.singletonList(TEXT_INDEX_COLUMN));
  }

  protected static Schema getSchema()
      throws IOException {
    return new Schema.SchemaBuilder().addDateTime(DEFAULT_TIME_COLUMN_NAME, FieldSpec.DataType.INT, "1:DAYS:EPOCH",
            "1:DAYS")
        .addDateTime(MS_SINCE_EPOCH_COLUMN_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addSingleValueDimension(PARTITIONED_COLUMN_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(TEXT_INDEX_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(TEXT_INDEX_COLUMN_MV, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_INDEX_COLUMN, FieldSpec.DataType.JSON)
        .addSingleValueDimension(FST_TEST_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(H3_INDEX_COLUMN, FieldSpec.DataType.STRING).build();
  }

  protected static List<GenericRow> generateRows() {
    GenericRow row0 = new GenericRow();
    row0.putValue(DEFAULT_TIME_COLUMN_NAME, 20000);
    row0.putValue(MS_SINCE_EPOCH_COLUMN_NAME, 20000L * 86400 * 1000);
    row0.putValue(TEXT_INDEX_COLUMN, "text_index_column_0");
    row0.putValue(TEXT_INDEX_COLUMN_MV, "text_index_column_0");
    row0.putValue(JSON_INDEX_COLUMN, "{\"a\":\"b\"}");
    row0.putValue(FST_TEST_COLUMN, "fst_test_column_0");
    row0.putValue(H3_INDEX_COLUMN, "h3_index_column_0");
    row0.putValue(PARTITIONED_COLUMN_NAME, 0);

    GenericRow row1 = new GenericRow();
    row1.putValue(DEFAULT_TIME_COLUMN_NAME, 20001);
    row1.putValue(MS_SINCE_EPOCH_COLUMN_NAME, 20001L * 86400 * 1000);
    row1.putValue(TEXT_INDEX_COLUMN, "text_index_column_0");
    row1.putValue(TEXT_INDEX_COLUMN_MV, "text_index_column_1");
    row1.putValue(JSON_INDEX_COLUMN, "{\"a\":\"b\"}");
    row1.putValue(FST_TEST_COLUMN, "fst_test_column_1");
    row1.putValue(H3_INDEX_COLUMN, "h3_index_column_1");
    row0.putValue(PARTITIONED_COLUMN_NAME, 1);

    GenericRow row2 = new GenericRow();
    row2.putValue(DEFAULT_TIME_COLUMN_NAME, 20002);
    row2.putValue(MS_SINCE_EPOCH_COLUMN_NAME, 20002L * 86400 * 1000);
    row2.putValue(TEXT_INDEX_COLUMN, "text_index_column_0");
    row1.putValue(TEXT_INDEX_COLUMN_MV, "text_index_column_2");
    row1.putValue(JSON_INDEX_COLUMN, "{\"a\":\"b\"}");
    row1.putValue(FST_TEST_COLUMN, "fst_test_column_2");
    row1.putValue(H3_INDEX_COLUMN, "h3_index_column_2");
    row0.putValue(PARTITIONED_COLUMN_NAME, 2);

    return List.of(row0, row2, row1);
  }

  private static File createSegment(SegmentVersion segmentVersion, TableConfig tableConfig, Schema schema,
      String segmentName, List<GenericRow> rows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    config.setSegmentVersion(segmentVersion);

    //Create ONE row
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, segmentName);
  }

  private static ImmutableSegmentDataManager createImmutableSegmentDataManager(TableConfig tableConfig, Schema schema,
      String segmentName, List<GenericRow> rows)
      throws Exception {
    ImmutableSegmentDataManager segmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    File indexDir = createSegment(SegmentVersion.v3, tableConfig, schema, segmentName, rows);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig);
    when(segmentDataManager.getSegment()).thenReturn(immutableSegment);
    return segmentDataManager;
  }

  @Test
  void testAddTimeColumn()
      throws Exception {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).setNullHandlingEnabled(true)
            .setNoDictionaryColumns(Collections.singletonList(TEXT_INDEX_COLUMN)).build();

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(TEXT_INDEX_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_INDEX_COLUMN, FieldSpec.DataType.JSON)
        .addSingleValueDimension(FST_TEST_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(H3_INDEX_COLUMN, FieldSpec.DataType.STRING).build();

    GenericRow row = new GenericRow();
    row.putValue(TEXT_INDEX_COLUMN, "text_index_column");
    row.putValue(JSON_INDEX_COLUMN, "{\"a\":\"b\"}");
    row.putValue(FST_TEST_COLUMN, "fst_test_column");
    row.putValue(H3_INDEX_COLUMN, "h3_index_column");

    ImmutableSegmentDataManager segmentDataManager =
        createImmutableSegmentDataManager(tableConfig, schema, "noChanges", List.of(row));
    BaseTableDataManager tableDataManager = BaseTableDataManagerTest.createTableManager();

    TableDataManager.NeedRefreshResponse response =
        tableDataManager.needRefresh(tableConfig, schema, segmentDataManager);
    assertFalse(response.isNeedRefresh());

    // Test new time column
    response = tableDataManager.needRefresh(getTableConfigBuilder().build(), getSchema(), segmentDataManager);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "time column");
  }

  @Test
  void testChangeTimeColumn() {
    TableDataManager.NeedRefreshResponse response = BASE_TABLE_DATA_MANAGER.needRefresh(
        getTableConfigBuilder().setTimeColumnName(MS_SINCE_EPOCH_COLUMN_NAME).build(), SCHEMA,
        IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "time column");
  }

  @Test
  void testRemoveColumn()
      throws Exception {
    Schema schema = getSchema();
    schema.removeField(TEXT_INDEX_COLUMN);
    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, schema, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "column deleted: textColumn");
  }

  @Test
  void testFieldType()
      throws Exception {
    Schema schema = getSchema();
    schema.removeField(TEXT_INDEX_COLUMN);
    schema.addField(new MetricFieldSpec(TEXT_INDEX_COLUMN, FieldSpec.DataType.STRING, true));

    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, schema, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "field type changed: textColumn");
  }

  @Test
  void testChangeDataType()
      throws Exception {
    Schema schema = getSchema();
    schema.removeField(TEXT_INDEX_COLUMN);
    schema.addField(new DimensionFieldSpec(TEXT_INDEX_COLUMN, FieldSpec.DataType.INT, true));

    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, schema, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "data type changed: textColumn");
  }

  @Test
  void testChangeToMV()
      throws Exception {
    Schema schema = getSchema();
    schema.removeField(TEXT_INDEX_COLUMN);
    schema.addField(new DimensionFieldSpec(TEXT_INDEX_COLUMN, FieldSpec.DataType.STRING, false));

    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, schema, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "single / multi value changed: textColumn");
  }

  @Test
  void testChangeToSV()
      throws Exception {
    Schema schema = getSchema();
    schema.removeField(TEXT_INDEX_COLUMN_MV);
    schema.addField(new DimensionFieldSpec(TEXT_INDEX_COLUMN_MV, FieldSpec.DataType.STRING, true));

    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, schema, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "single / multi value changed: textColumnMV");
  }

  @Test
  void testSortColumnMismatch() {
    // Check with a column that is not sorted
    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(getTableConfigBuilder().setSortedColumn(MS_SINCE_EPOCH_COLUMN_NAME).build(),
            SCHEMA, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "sort column changed: MilliSecondsSinceEpoch");
    // Check with a column that is sorted
    assertFalse(
        BASE_TABLE_DATA_MANAGER.needRefresh(getTableConfigBuilder().setSortedColumn(TEXT_INDEX_COLUMN).build(), SCHEMA,
            IMMUTABLE_SEGMENT_DATA_MANAGER).isNeedRefresh());
  }

  @DataProvider(name = "testFilterArgs")
  private Object[][] testFilterArgs() {
    return new Object[][] {
        {"withBloomFilter", getTableConfigBuilder().setBloomFilterColumns(List.of(TEXT_INDEX_COLUMN)).build(), "bloom filter changed: textColumn"},
        {"withJsonIndex", getTableConfigBuilder().setJsonIndexColumns(List.of(JSON_INDEX_COLUMN)).build(), "json index changed: jsonField"},
        {"withTextIndex", getTableConfigBuilder().setFieldConfigList(List.of(new FieldConfig(TEXT_INDEX_COLUMN,
            FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.TEXT),
            null, null))).build(), "text index changed: textColumn"},
        {"withFstIndex", getTableConfigBuilder().setFieldConfigList(List.of(new FieldConfig(TEXT_INDEX_COLUMN,
            FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.FST),
            null, null))).build(), ""},
        {"withH3Index", getTableConfigBuilder().setFieldConfigList(List.of(new FieldConfig(TEXT_INDEX_COLUMN,
            FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3),
            null, H3_INDEX_PROPERTIES))).build(), ""},
        {"withRangeFilter", getTableConfigBuilder().setRangeIndexColumns(List.of(MS_SINCE_EPOCH_COLUMN_NAME)).build(), "range index changed: MilliSecondsSinceEpoch"}
    };
  }

  @Test(dataProvider = "testFilterArgs")
  void testFilter(String segmentName, TableConfig tableConfigWithFilter, String expectedReason)
      throws Exception {
    ImmutableSegmentDataManager segmentWithFilter = createImmutableSegmentDataManager(tableConfigWithFilter, SCHEMA, segmentName, generateRows());

    // When TableConfig has a bloom filter but segment does not have, needRefresh is true.
    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(tableConfigWithFilter, SCHEMA, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), expectedReason);

    // When TableConfig does not have bloom filter but segment has, needRefresh is true
    response = BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, SCHEMA, segmentWithFilter);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), expectedReason);

    // When TableConfig has bloom filter AND segment also has bloom filter, needRefresh is false
    assertFalse(BASE_TABLE_DATA_MANAGER.needRefresh(tableConfigWithFilter, SCHEMA, segmentWithFilter).isNeedRefresh());
  }

  @Test
  void testPartition()
      throws Exception {
    TableConfig partitionedTableConfig = getTableConfigBuilder().setSegmentPartitionConfig(new SegmentPartitionConfig(
        Map.of(PARTITIONED_COLUMN_NAME, new ColumnPartitionConfig(PARTITION_FUNCTION_NAME, NUM_PARTITIONS)))).build();
    ImmutableSegmentDataManager segmentWithPartition =
        createImmutableSegmentDataManager(partitionedTableConfig, SCHEMA, "partitionWithModulo", generateRows());

    // when segment has no partition AND tableConfig has partitions then needRefresh = true
    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(partitionedTableConfig, SCHEMA, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "partition function added: partitionedColumn");

    // when segment has partitions AND tableConfig has no partitions, then needRefresh = false
    assertFalse(BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, SCHEMA, segmentWithPartition).isNeedRefresh());

    // when # of partitions is different, then needRefresh = true
    TableConfig partitionedTableConfig40 = getTableConfigBuilder().setSegmentPartitionConfig(new SegmentPartitionConfig(
        Map.of(PARTITIONED_COLUMN_NAME, new ColumnPartitionConfig(PARTITION_FUNCTION_NAME, 40)))).build();

    response = BASE_TABLE_DATA_MANAGER.needRefresh(partitionedTableConfig40, SCHEMA, segmentWithPartition);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "num partitions changed: partitionedColumn");

    // when partition function is different, then needRefresh = true
    TableConfig partitionedTableConfigMurmur = getTableConfigBuilder().setSegmentPartitionConfig(
        new SegmentPartitionConfig(Map.of(PARTITIONED_COLUMN_NAME, new ColumnPartitionConfig("murmur", NUM_PARTITIONS)))).build();

    response = BASE_TABLE_DATA_MANAGER.needRefresh(partitionedTableConfigMurmur, SCHEMA, segmentWithPartition);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "partition function name changed: partitionedColumn");
  }

  @Test
  void testNullValueVector()
      throws Exception {
    TableConfig withoutNullHandling = getTableConfigBuilder().setNullHandlingEnabled(false).build();
    ImmutableSegmentDataManager segmentWithoutNullHandling = createImmutableSegmentDataManager(withoutNullHandling, SCHEMA, "withoutNullHandling", generateRows());

    // If null handling is removed from table config AND segment has NVV, then NVV can be removed. needRefresh = true
    TableDataManager.NeedRefreshResponse response =
        BASE_TABLE_DATA_MANAGER.needRefresh(withoutNullHandling, SCHEMA, IMMUTABLE_SEGMENT_DATA_MANAGER);
    assertTrue(response.isNeedRefresh());
    assertEquals(response.getReason(), "null value vector index removed from column: DestCityName");

    // if NVV is added to table config AND segment does not have NVV, then it cannot be added. needRefresh = false
    assertFalse(BASE_TABLE_DATA_MANAGER.needRefresh(TABLE_CONFIG, SCHEMA, segmentWithoutNullHandling).isNeedRefresh());
  }
}
