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
package org.apache.pinot.segment.local.realtime.converter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshManager;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeSegmentConverterTest implements PinotBuffersAfterMethodCheckRule {

  private static final String STRING_COLUMN1 = "string_col1";
  private static final String STRING_COLUMN2 = "string_col2";
  private static final String STRING_COLUMN3 = "string_col3";
  private static final String STRING_COLUMN4 = "string_col4";
  private static final String LONG_COLUMN1 = "long_col1";
  private static final String LONG_COLUMN2 = "long_col2";
  private static final String LONG_COLUMN3 = "long_col3";
  private static final String LONG_COLUMN4 = "long_col4";
  private static final String MV_INT_COLUMN = "mv_col";
  private static final String DATE_TIME_COLUMN = "date_time_col";

  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), RealtimeSegmentConverterTest.class.getName());

  @BeforeClass
  public void setup() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @Test
  public void testNoVirtualColumnsInSchema() {
    // @formatter:off
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("someName")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "col1"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "col2"))
        .build();
    // @formatter:on
    String segmentName = "segment1";
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    Set<FieldSpec> initialVirtualColumns = getVirtualColumns(schema);
    assertNotEquals(initialVirtualColumns, Collections.emptySet(), "Initial virtual columns should not be empty");
    Schema newSchema = RealtimeSegmentConverter.getUpdatedSchema(schema);
    Set<FieldSpec> newVirtualColumns = getVirtualColumns(newSchema);
    assertEquals(newVirtualColumns, Collections.emptySet(), "Virtual columns should be removed");
    assertEquals(newSchema.getSchemaName(), schema.getSchemaName(), "Schema name should be the same");
    assertEquals(newSchema.isEnableColumnBasedNullHandling(), schema.isEnableColumnBasedNullHandling(),
        "Column based null handling should be the same");
  }

  private Set<FieldSpec> getVirtualColumns(Schema schema) {
    return schema.getAllFieldSpecs().stream()
        .filter(FieldSpec::isVirtualColumn)
        .collect(Collectors.toSet());
  }

  @Test
  public void testNoRecordsIndexedRowMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1)).setSortedColumn(LONG_COLUMN1)
            .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
            .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
            .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3)).setColumnMajorSegmentBuilderEnabled(false)
            .build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT).addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    DictionaryIndexConfig varLengthDictConf = new DictionaryIndexConfig(false, true);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setIndex(Sets.newHashSet(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
            .setIndex(Sets.newHashSet(Sets.newHashSet(STRING_COLUMN3)), StandardIndexes.dictionary(), varLengthDictConf)
            .setIndex(Sets.newHashSet(STRING_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getTotalDocs(), 0);
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
      assertEquals(segmentMetadata.getStartTime(), segmentMetadata.getEndTime());
      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @Test
  public void test10RecordsIndexedRowMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1, LONG_COLUMN1))
            .setSortedColumn(LONG_COLUMN1)
            .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
            .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
            .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3))
            .setColumnMajorSegmentBuilderEnabled(false)
            .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT)
        .addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    DictionaryIndexConfig varLengthDictConf = new DictionaryIndexConfig(false, true);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setIndex(Sets.newHashSet(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
            .setIndex(Sets.newHashSet(Sets.newHashSet(STRING_COLUMN3)), StandardIndexes.dictionary(), varLengthDictConf)
            .setIndex(Sets.newHashSet(STRING_COLUMN1, LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = generateTestData();

      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
      assertEquals(segmentMetadata.getTotalDocs(), rows.size());
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);

      long expectedStartTime = (long) rows.get(0).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getStartTime(), expectedStartTime);
      long expectedEndTime = (long) rows.get(rows.size() - 1).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getEndTime(), expectedEndTime);

      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");

      testSegment(rows, indexDir, tableConfig, segmentMetadata);
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @Test
  public void testNoRecordsIndexedColumnMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1)).setSortedColumn(LONG_COLUMN1)
            .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
            .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
            .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3)).setColumnMajorSegmentBuilderEnabled(true)
            .build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT)
        .addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    DictionaryIndexConfig varLengthDictConf = new DictionaryIndexConfig(false, true);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setIndex(Sets.newHashSet(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
            .setIndex(Sets.newHashSet(Sets.newHashSet(STRING_COLUMN3)), StandardIndexes.dictionary(), varLengthDictConf)
            .setIndex(Sets.newHashSet(STRING_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getTotalDocs(), 0);
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
      assertEquals(segmentMetadata.getStartTime(), segmentMetadata.getEndTime());
      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @Test
  public void test10RecordsIndexedColumnMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1, LONG_COLUMN1))
            .setSortedColumn(LONG_COLUMN1)
            .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
            .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
            .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3))
            .setColumnMajorSegmentBuilderEnabled(true)
            .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT)
        .addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    DictionaryIndexConfig varLengthDictConf = new DictionaryIndexConfig(false, true);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setIndex(Sets.newHashSet(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
            .setIndex(Sets.newHashSet(Sets.newHashSet(STRING_COLUMN3)), StandardIndexes.dictionary(), varLengthDictConf)
            .setIndex(Sets.newHashSet(STRING_COLUMN1, LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = generateTestData();

      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
      assertEquals(segmentMetadata.getTotalDocs(), rows.size());
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);

      long expectedStartTime = (long) rows.get(0).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getStartTime(), expectedStartTime);
      long expectedEndTime = (long) rows.get(rows.size() - 1).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getEndTime(), expectedEndTime);

      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");

      testSegment(rows, indexDir, tableConfig, segmentMetadata);
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  private void testSegment(List<GenericRow> rows, File indexDir,
      TableConfig tableConfig, SegmentMetadataImpl segmentMetadata)
      throws IOException {
    SegmentLocalFSDirectory segmentDir = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
    SegmentDirectory.Reader segmentReader = segmentDir.createReader();

    Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
    Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      indexContainerMap.put(entry.getKey(),
          new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig));
    }
    ImmutableSegmentImpl segmentFile = new ImmutableSegmentImpl(segmentDir, segmentMetadata, indexContainerMap, null);

    GenericRow readRow = new GenericRow();
    int docId = 0;
    for (GenericRow row : rows) {
      segmentFile.getRecord(docId, readRow);
      assertEquals(readRow.getValue(STRING_COLUMN1), row.getValue(STRING_COLUMN1));
      assertEquals(readRow.getValue(STRING_COLUMN2), row.getValue(STRING_COLUMN2));
      assertEquals(readRow.getValue(STRING_COLUMN3), row.getValue(STRING_COLUMN3));
      assertEquals(readRow.getValue(STRING_COLUMN4), row.getValue(STRING_COLUMN4));
      assertEquals(readRow.getValue(LONG_COLUMN1), row.getValue(LONG_COLUMN1));
      assertEquals(readRow.getValue(LONG_COLUMN2), row.getValue(LONG_COLUMN2));
      assertEquals(readRow.getValue(LONG_COLUMN3), row.getValue(LONG_COLUMN3));
      assertEquals(readRow.getValue(LONG_COLUMN4), row.getValue(LONG_COLUMN4));
      assertEquals(readRow.getValue(MV_INT_COLUMN), row.getValue(MV_INT_COLUMN));
      assertEquals(readRow.getValue(DATE_TIME_COLUMN), row.getValue(DATE_TIME_COLUMN));

      docId += 1;
    }

    segmentFile.destroy();
  }

  @DataProvider
  public static Object[][] reuseParams() {
    List<Boolean> enabledColumnMajorSegmentBuildParams = Arrays.asList(false, true);
    List<String> sortedColumnParams = Arrays.asList(null, LONG_COLUMN1);
    List<Boolean> reuseMutableIndex = Arrays.asList(true, false);
    List<Integer> luceneNRTCachingDirectoryMaxBufferSizeMB = Arrays.asList(0, 5);
    List<String> rawValueForTextIndexParams = Arrays.asList(null, "n");
    List<DictionaryIndexConfig> dictionaryIndexConfigs =
        Arrays.asList(DictionaryIndexConfig.DISABLED, DictionaryIndexConfig.DEFAULT);

    return enabledColumnMajorSegmentBuildParams.stream().flatMap(columnMajor -> sortedColumnParams.stream().flatMap(
        sortedColumn -> dictionaryIndexConfigs.stream().flatMap(
            dictionaryIndexConfig -> rawValueForTextIndexParams.stream().flatMap(
                rawValueForTextIndex -> reuseMutableIndex.stream().flatMap(
                    reuseIndex -> luceneNRTCachingDirectoryMaxBufferSizeMB.stream().map(cacheSize -> new Object[]{
                        columnMajor, sortedColumn, reuseIndex, cacheSize, rawValueForTextIndex, dictionaryIndexConfig
                    })))))).toArray(Object[][]::new);
  }

  // Test the realtime segment conversion of a table with an index that reuses mutable index artifacts during conversion
  @Test(dataProvider = "reuseParams")
  public void testSegmentBuilderWithReuse(boolean columnMajorSegmentBuilder, String sortedColumn,
      boolean reuseMutableIndex, int luceneNRTCachingDirectoryMaxBufferSizeMB, String rawValueForTextIndex,
      DictionaryIndexConfig dictionaryIndexConfig)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.nanoTime());

    Map<String, String> fieldConfigColumnProperties = new HashMap<>();
    fieldConfigColumnProperties.put(FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX,
        String.valueOf(reuseMutableIndex));
    fieldConfigColumnProperties.put(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, "true");
    if (rawValueForTextIndex != null) {
      fieldConfigColumnProperties.put(FieldConfig.TEXT_INDEX_RAW_VALUE, rawValueForTextIndex);
    }
    FieldConfig textIndexFieldConfig =
        new FieldConfig.Builder(STRING_COLUMN1).withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexTypes(Collections.singletonList(FieldConfig.IndexType.TEXT))
            .withProperties(fieldConfigColumnProperties).build();
    List<FieldConfig> fieldConfigList = Collections.singletonList(textIndexFieldConfig);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(LONG_COLUMN1))
            .setNoDictionaryColumns(Lists.newArrayList(STRING_COLUMN1))
            .setSortedColumn(sortedColumn)
            .setColumnMajorSegmentBuilderEnabled(columnMajorSegmentBuilder)
            .setFieldConfigList(fieldConfigList)
            .build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";
    TextIndexConfig textIndexConfig =
        new TextIndexConfigBuilder().withUseANDForMultiTermQueries(false).withReuseMutableIndex(reuseMutableIndex)
            .withLuceneNRTCachingDirectoryMaxBufferSizeMB(luceneNRTCachingDirectoryMaxBufferSizeMB)
            .withRawValueForTextIndex(rawValueForTextIndex).build();

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setIndex(Sets.newHashSet(LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
            .setIndex(Sets.newHashSet(STRING_COLUMN1), StandardIndexes.text(), textIndexConfig)
            .setIndex(Sets.newHashSet(STRING_COLUMN1), StandardIndexes.dictionary(), dictionaryIndexConfig)
            .setFieldConfigList(fieldConfigList).setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
            .setOffHeap(true).setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    RealtimeLuceneTextIndexSearcherPool.init(1);
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    ImmutableSegmentImpl segmentFile = null;
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = generateTestDataForReusePath();

      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      // build converted segment
      File outputDir = new File(new File(tmpDir, segmentName), "tmp-" + segmentName + "-" + System.currentTimeMillis());
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
      assertEquals(segmentMetadata.getTotalDocs(), rows.size());
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);

      long expectedStartTime = (long) rows.get(0).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getStartTime(), expectedStartTime);
      long expectedEndTime = (long) rows.get(rows.size() - 1).getValue(DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getEndTime(), expectedEndTime);

      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");

      // read converted segment
      SegmentLocalFSDirectory segmentDir = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
      SegmentDirectory.Reader segmentReader = segmentDir.createReader();

      Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
      Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
      for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
        indexContainerMap.put(entry.getKey(),
            new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig));
      }
      segmentFile = new ImmutableSegmentImpl(segmentDir, segmentMetadata, indexContainerMap, null);

      // test forward index contents
      GenericRow readRow = new GenericRow();
      int docId = 0;
      for (int i = 0; i < rows.size(); i++) {
        GenericRow row;
        if (sortedColumn == null) {
          row = rows.get(i);
        } else {
          row = rows.get(rows.size() - i - 1);
        }

        segmentFile.getRecord(docId, readRow);

        // if rawValueForTextIndex is set and mutable index is reused, the forward index should return the dummy value
        if (rawValueForTextIndex != null && reuseMutableIndex) {
          assertEquals(readRow.getValue(STRING_COLUMN1), rawValueForTextIndex);
          assertEquals(readRow.getValue(DATE_TIME_COLUMN), row.getValue(DATE_TIME_COLUMN));
        } else {
          assertEquals(readRow.getValue(STRING_COLUMN1), row.getValue(STRING_COLUMN1));
          assertEquals(readRow.getValue(DATE_TIME_COLUMN), row.getValue(DATE_TIME_COLUMN));
        }
        docId += 1;
      }

      // test docId conversion
      TextIndexReader textIndexReader = segmentFile.getIndex(STRING_COLUMN1, StandardIndexes.text());
      if (sortedColumn == null) {
        assertEquals(textIndexReader.getDocIds("str-8"), ImmutableRoaringBitmap.bitmapOf(0));
        assertEquals(textIndexReader.getDocIds("str-4"), ImmutableRoaringBitmap.bitmapOf(4));
      } else {
        assertEquals(textIndexReader.getDocIds("str-8"), ImmutableRoaringBitmap.bitmapOf(7));
        assertEquals(textIndexReader.getDocIds("str-4"), ImmutableRoaringBitmap.bitmapOf(3));
      }
    } finally {
      mutableSegmentImpl.destroy();
      if (segmentFile != null) {
        segmentFile.destroy();
      }
    }
  }

  private List<GenericRow> generateTestData() {
    LinkedList<GenericRow> rows = new LinkedList<>();

    for (int i = 0; i < 10; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN1, "Hello" + i);
      row.putValue(STRING_COLUMN2, "World" + i);
      row.putValue(STRING_COLUMN3, "F1" + i);
      row.putValue(STRING_COLUMN4, "F2" + i);
      row.putValue(LONG_COLUMN1, 67L + i);
      row.putValue(LONG_COLUMN2, 66L + i);
      row.putValue(LONG_COLUMN3, 65L + i);
      row.putValue(LONG_COLUMN4, 64L + i);
      List<Integer> intList = new ArrayList<>();
      intList.add(100 + i);
      intList.add(200 + i);
      row.putValue(MV_INT_COLUMN, intList.toArray());
      row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
      rows.add(row);
    }

    return rows;
  }

  private List<GenericRow> generateTestDataForReusePath() {
    List<GenericRow> rows = new LinkedList<>();

    for (int i = 0; i < 8; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN1, "str" + (i - 8));
      row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
      row.putValue(LONG_COLUMN1, 8L - i);
      rows.add(row);
    }

    return rows;
  }

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    return segmentZKMetadata;
  }

  @AfterMethod
  public void tearDownTest() {
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
