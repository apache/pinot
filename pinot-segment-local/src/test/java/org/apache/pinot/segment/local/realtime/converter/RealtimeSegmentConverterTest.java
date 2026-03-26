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
import java.math.BigDecimal;
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
import org.apache.commons.configuration2.ex.ConfigurationException;
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
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


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
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
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
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
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
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
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
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
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
      throws IOException, ConfigurationException {
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
  public static Object[][] optimizeDictionaryTypeParams() {
    // Format: {optimizeDictionaryType, expectedCRC}, crc is used here to check the correct dictionary type was used
    return new Object[][]{
        {true, "2653526366"},
        {false, "2948830084"},
    };
  }

  @Test(dataProvider = "optimizeDictionaryTypeParams")
  public void testOptimizeDictionaryTypeConversion(Object[] params)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setColumnMajorSegmentBuilderEnabled(true)
            .setOptimizeDictionaryType((boolean) params[0])
            .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setIndex(Sets.newHashSet(Sets.newHashSet(STRING_COLUMN1, STRING_COLUMN2)), StandardIndexes.dictionary(),
                DictionaryIndexConfig.DEFAULT)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        GenericRow row = new GenericRow();
        row.putValue(STRING_COLUMN1, "string" + i * 10); // var length
        row.putValue(STRING_COLUMN2, "string" + i % 10); // fixed length
        row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
        rows.add(row);
      }
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
      assertEquals(segmentMetadata.getCrc(), params[1]);

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
    } finally {
      mutableSegmentImpl.destroy();
    }
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
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
            .setOffHeap(true).setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
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

  @Test
  public void testPartitionFunctionConfigPreservedInConvertedSegment()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());

    // Create a partition function config with BoundedColumnValue
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("columnValues", "Hello0,Hello1,Hello2");
    functionConfig.put("columnValuesDelimiter", ",");

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(STRING_COLUMN1,
            new ColumnPartitionConfig("BoundedColumnValue", 4, functionConfig)));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setSegmentPartitionConfig(segmentPartitionConfig)
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

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "BoundedColumnValue", 4, functionConfig);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setPartitionColumn(STRING_COLUMN1)
            .setPartitionFunction(partitionFunction)
            .setPartitionId(0)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = generateTestData();
      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      // Verify the mutable segment's partition config includes function config
      SegmentPartitionConfig mutablePartitionConfig = mutableSegmentImpl.getSegmentPartitionConfig();
      assertNotNull(mutablePartitionConfig);
      ColumnPartitionConfig columnPartitionConfig =
          mutablePartitionConfig.getColumnPartitionMap().get(STRING_COLUMN1);
      assertNotNull(columnPartitionConfig);
      assertEquals(columnPartitionConfig.getFunctionName(), "BoundedColumnValue");
      assertEquals(columnPartitionConfig.getNumPartitions(), 4);
      assertEquals(columnPartitionConfig.getFunctionConfig(), functionConfig);

      // Convert to immutable segment
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      // Verify the converted segment metadata preserves the partition function config
      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      ColumnMetadata col1Meta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertNotNull(col1Meta.getPartitionFunction(), "Partition function should be present in converted segment");
      assertEquals(col1Meta.getPartitionFunction().getName(), "BoundedColumnValue");
      assertEquals(col1Meta.getPartitionFunction().getNumPartitions(), 4);
      assertEquals(col1Meta.getPartitionFunction().getFunctionConfig(), functionConfig,
          "Function config should be preserved in converted segment metadata");
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @Test
  public void testMurmur3PartitionFunctionConfigPreservedInConvertedSegment()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());

    // Create a Murmur3 partition function config with seed and variant
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("seed", "9001");
    functionConfig.put("variant", "x64_32");

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(STRING_COLUMN1,
            new ColumnPartitionConfig("Murmur3", 5, functionConfig)));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setSegmentPartitionConfig(segmentPartitionConfig)
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

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "Murmur3", 5, functionConfig);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setPartitionColumn(STRING_COLUMN1)
            .setPartitionFunction(partitionFunction)
            .setPartitionId(0)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    try {
      List<GenericRow> rows = generateTestData();
      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      // Convert to immutable segment
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      // Verify the converted segment metadata preserves the Murmur3 function config
      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      ColumnMetadata col1Meta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertNotNull(col1Meta.getPartitionFunction(), "Partition function should be present in converted segment");
      assertEquals(col1Meta.getPartitionFunction().getName(), "Murmur3");
      assertEquals(col1Meta.getPartitionFunction().getNumPartitions(), 5);
      assertEquals(col1Meta.getPartitionFunction().getFunctionConfig(), functionConfig,
          "Murmur3 function config (seed, variant) should be preserved in converted segment metadata");
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @Test
  public void testFnvPartitionFunctionConfigPreservedInConvertedSegment()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());

    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("variant", "fnv1_64");
    functionConfig.put("negativePartitionHandling", "abs");

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(STRING_COLUMN1,
            new ColumnPartitionConfig("FNV", 5, functionConfig)));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setTimeColumnName(DATE_TIME_COLUMN)
            .setSegmentPartitionConfig(segmentPartitionConfig)
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

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "FNV", 5, functionConfig);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3)
            .setPartitionColumn(STRING_COLUMN1)
            .setPartitionFunction(partitionFunction)
            .setPartitionId(0)
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

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

      ColumnMetadata col1Meta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertNotNull(col1Meta.getPartitionFunction(), "Partition function should be present in converted segment");
      assertEquals(col1Meta.getPartitionFunction().getName(), "FNV");
      assertEquals(col1Meta.getPartitionFunction().getNumPartitions(), 5);
      assertEquals(col1Meta.getPartitionFunction().getFunctionConfig(), functionConfig,
          "FNV function config should be preserved in converted segment metadata");
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @DataProvider
  public static Object[][] sortedColumnParams() {
    FieldSpec.DataType[] dataTypes = {
        FieldSpec.DataType.INT,
        FieldSpec.DataType.LONG,
        FieldSpec.DataType.FLOAT,
        FieldSpec.DataType.DOUBLE,
        FieldSpec.DataType.BIG_DECIMAL,
        FieldSpec.DataType.BOOLEAN,
        FieldSpec.DataType.TIMESTAMP,
        FieldSpec.DataType.STRING,
        FieldSpec.DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (FieldSpec.DataType dataType : dataTypes) {
      params.add(new Object[]{dataType, true});   // dictionary-encoded
      params.add(new Object[]{dataType, false});  // no-dictionary
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that the isSorted metadata flag is correctly set on both the mutable segment (via sorted doc ID order)
  /// and the converted immutable segment (via ColumnMetadata) for all supported single-value data types, both
  /// dictionary-encoded and no-dictionary.
  @Test(dataProvider = "sortedColumnParams")
  public void testSortedColumnMetadata(FieldSpec.DataType dataType, boolean dictionaryEncoded)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_sorted_" + dataType + "_" + dictionaryEncoded + "_" + System.nanoTime());
    String sortedColumn = "sorted_col";
    String dupColumn = "sorted_col2";

    List<String> noDictColumns = dictionaryEncoded ? List.of() : List.of(sortedColumn, dupColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSortedColumn(sortedColumn)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(sortedColumn, dataType)
        .addSingleValueDimension(dupColumn, dataType)
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    // Use Builder(tableConfig, schema) so the inverted index is automatically added for the
    // dictionary-encoded sorted column (required by getSortedDocIdsWithInvertedIndex).
    RealtimeSegmentConfig.Builder configBuilder = new RealtimeSegmentConfig.Builder(tableConfig, schema)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(configBuilder.build(), null);
    try {
      // Insert rows in REVERSE sorted order to confirm conversion actually reorders them.
      // dupColumn carries the same values as sortedColumn — it must also be detected as sorted.
      // STRING_COLUMN1 values are intentionally correlated inversely with the sorted column so that when rows are
      // sorted ascending, STRING_COLUMN1 ends up descending (i.e. not sorted).
      int numRows = 10;
      for (int i = numRows - 1; i >= 0; i--) {
        GenericRow row = new GenericRow();
        Object sortedValue = sortedValue(dataType, i, numRows);
        row.putValue(sortedColumn, sortedValue);
        row.putValue(dupColumn, sortedValue);
        row.putValue(STRING_COLUMN1, "str" + (numRows - 1 - i));
        row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
        mutableSegment.index(row, null);
      }

      // -- Mutable segment: getSortedDocIdIterationOrderWithSortedColumn must return doc IDs in non-decreasing
      //    value order regardless of encoding --
      int[] sortedDocIds = mutableSegment.getSortedDocIdIterationOrderWithSortedColumn(sortedColumn);
      assertEquals(sortedDocIds.length, numRows);
      FieldSpec.DataType storedType = dataType.getStoredType();
      Object prevValue = null;
      for (int docId : sortedDocIds) {
        Object currValue = mutableSegment.getValue(docId, sortedColumn);
        if (prevValue != null) {
          assertTrue(compareValues(storedType, currValue, prevValue) >= 0,
              "Mutable segment: " + sortedColumn + " values must be non-decreasing in sorted doc ID order"
                  + " (dataType=" + dataType + ", dictionaryEncoded=" + dictionaryEncoded + ")");
        }
        prevValue = currValue;
      }

      // -- Immutable segment: ColumnMetadata.isSorted() must be true for the sorted column --
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegment, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(outputDir, segmentName));

      ColumnMetadata sortedColMeta = segmentMetadata.getColumnMetadataFor(sortedColumn);
      assertTrue(sortedColMeta.isSorted(),
          "Immutable segment: sorted column must have isSorted=true (dataType=" + dataType + ", dictionaryEncoded="
              + dictionaryEncoded + ")");
      assertEquals(sortedColMeta.hasDictionary(), dictionaryEncoded,
          "Immutable segment: dictionary encoding must match config (dataType=" + dataType + ", dictionaryEncoded="
              + dictionaryEncoded + ")");

      // A non-sorted column must not be marked as sorted
      ColumnMetadata nonSortedColMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertFalse(nonSortedColMeta.isSorted(), "Immutable segment: non-sorted column must have isSorted=false");

      // Duplicate column with same values must also be detected as sorted
      ColumnMetadata dupColMeta = segmentMetadata.getColumnMetadataFor(dupColumn);
      assertTrue(dupColMeta.isSorted(),
          "Immutable segment: duplicate column with same values must also have isSorted=true (dataType=" + dataType
              + ", dictionaryEncoded=" + dictionaryEncoded + ")");
      assertEquals(dupColMeta.hasDictionary(), dictionaryEncoded,
          "Immutable segment: dictionary encoding must match config for duplicate column (dataType=" + dataType
              + ", dictionaryEncoded=" + dictionaryEncoded + ")");

      // Verify the physical row order in the converted segment: values must be non-decreasing
      File indexDir = new File(outputDir, segmentName);
      SegmentLocalFSDirectory segmentDir = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
      SegmentDirectory.Reader segmentReader = segmentDir.createReader();
      Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
      for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
        indexContainerMap.put(entry.getKey(),
            new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig));
      }
      ImmutableSegmentImpl immutableSegment =
          new ImmutableSegmentImpl(segmentDir, segmentMetadata, indexContainerMap, null);
      try {
        prevValue = null;
        for (int docId = 0; docId < numRows; docId++) {
          Object currValue = immutableSegment.getValue(docId, sortedColumn);
          if (prevValue != null) {
            assertTrue(compareValues(storedType, currValue, prevValue) >= 0,
                "Immutable segment rows must be in non-decreasing " + sortedColumn + " order at docId=" + docId
                    + " (dataType=" + dataType + ", dictionaryEncoded=" + dictionaryEncoded + ")");
          }
          prevValue = currValue;
        }
      } finally {
        immutableSegment.destroy();
      }
    } finally {
      mutableSegment.destroy();
    }
  }

  /// Verifies that when records are inserted already in sorted order on a column that is NOT configured as the
  /// sorted column, the converted immutable segment still detects isSorted=true for that column.
  @Test(dataProvider = "sortedColumnParams")
  public void testPreSortedRecordsWithoutSortedColumn(FieldSpec.DataType dataType, boolean dictionaryEncoded)
      throws Exception {
    File tmpDir =
        new File(TMP_DIR, "tmp_presorted_" + dataType + "_" + dictionaryEncoded + "_" + System.nanoTime());
    String preSortedColumn = "sorted_col";

    List<String> noDictColumns = dictionaryEncoded ? List.of() : List.of(preSortedColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(preSortedColumn, dataType)
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig.Builder configBuilder = new RealtimeSegmentConfig.Builder(tableConfig, schema)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(configBuilder.build(), null);
    try {
      // Insert rows in ascending order on preSortedColumn.
      // STRING_COLUMN1 is descending so it is not sorted.
      int numRows = 10;
      for (int i = 0; i < numRows; i++) {
        GenericRow row = new GenericRow();
        row.putValue(preSortedColumn, sortedValue(dataType, i, numRows));
        row.putValue(STRING_COLUMN1, "str" + (numRows - 1 - i));
        row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
        mutableSegment.index(row, null);
      }

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegment, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3, null);

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(outputDir, segmentName));

      ColumnMetadata preSortedColMeta = segmentMetadata.getColumnMetadataFor(preSortedColumn);
      assertTrue(preSortedColMeta.isSorted(),
          "Pre-sorted column must have isSorted=true even without sortedColumn config (dataType=" + dataType
              + ", dictionaryEncoded=" + dictionaryEncoded + ")");
      assertEquals(preSortedColMeta.hasDictionary(), dictionaryEncoded,
          "Dictionary encoding must match config (dataType=" + dataType + ", dictionaryEncoded=" + dictionaryEncoded
              + ")");

      ColumnMetadata nonSortedColMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertFalse(nonSortedColMeta.isSorted(), "Non-sorted column must have isSorted=false");
    } finally {
      mutableSegment.destroy();
    }
  }

  /// Returns the i-th ascending value for the given data type, out of numRows total rows.
  private static Object sortedValue(FieldSpec.DataType dataType, int i, int numRows) {
    switch (dataType) {
      case INT:
        return i;
      case LONG:
        return (long) i;
      case FLOAT:
        return (float) i;
      case DOUBLE:
        return (double) i;
      case BIG_DECIMAL:
        return new BigDecimal(i);
      case BOOLEAN:
        // First half false (0), second half true (1) — yields a non-decreasing sequence
        return i >= numRows / 2 ? 1 : 0;
      case TIMESTAMP:
        return (long) i * 1000L;
      case STRING:
        // Zero-padded to preserve lexicographic order
        return String.format("%05d", i);
      case BYTES:
        return new byte[]{(byte) i};
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /// Compares two values of the given stored type.
  private static int compareValues(FieldSpec.DataType storedType, Object v1, Object v2) {
    if (storedType == FieldSpec.DataType.BYTES) {
      return ByteArray.compare((byte[]) v1, (byte[]) v2);
    }
    return storedType.compare(v1, v2);
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
