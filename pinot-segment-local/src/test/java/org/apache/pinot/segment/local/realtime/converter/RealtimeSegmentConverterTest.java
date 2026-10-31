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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshManager;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
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
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("col1", DataType.STRING)
        .build();
    String segmentName = "segment1";
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    Set<FieldSpec> initialVirtualColumns = getVirtualColumns(schema);
    assertNotEquals(initialVirtualColumns, Set.of(), "Initial virtual columns should not be empty");
    Schema newSchema = RealtimeSegmentConverter.getUpdatedSchema(schema);
    Set<FieldSpec> newVirtualColumns = getVirtualColumns(newSchema);
    assertEquals(newVirtualColumns, Set.of(), "Virtual columns should be removed");
    assertEquals(newSchema.getSchemaName(), schema.getSchemaName(), "Schema name should be the same");
    assertEquals(newSchema.isEnableColumnBasedNullHandling(), schema.isEnableColumnBasedNullHandling(),
        "Column based null handling should be the same");
  }

  private Set<FieldSpec> getVirtualColumns(Schema schema) {
    return schema.getAllFieldSpecs().stream().filter(FieldSpec::isVirtualColumn).collect(Collectors.toSet());
  }

  @Test
  public void testNoRecordsIndexedRowMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(List.of(STRING_COLUMN1))
        .setSortedColumn(LONG_COLUMN1)
        .setRangeIndexColumns(List.of(STRING_COLUMN2))
        .setNoDictionaryColumns(List.of(LONG_COLUMN2))
        .setVarLengthDictionaryColumns(List.of(STRING_COLUMN3))
        .setOnHeapDictionaryColumns(List.of(LONG_COLUMN3))
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.forward(),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW))
        .setIndex(Set.of(STRING_COLUMN3), StandardIndexes.dictionary(), new DictionaryIndexConfig(false, true))
        .setIndex(Set.of(STRING_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

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
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(List.of(STRING_COLUMN1, LONG_COLUMN1))
        .setSortedColumn(LONG_COLUMN1)
        .setRangeIndexColumns(List.of(STRING_COLUMN2))
        .setNoDictionaryColumns(List.of(LONG_COLUMN2))
        .setVarLengthDictionaryColumns(List.of(STRING_COLUMN3))
        .setOnHeapDictionaryColumns(List.of(LONG_COLUMN3))
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.forward(),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW))
        .setIndex(Set.of(STRING_COLUMN3), StandardIndexes.dictionary(), new DictionaryIndexConfig(false, true))
        .setIndex(Set.of(STRING_COLUMN1, LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(List.of(STRING_COLUMN1))
        .setSortedColumn(LONG_COLUMN1)
        .setRangeIndexColumns(List.of(STRING_COLUMN2))
        .setNoDictionaryColumns(List.of(LONG_COLUMN2))
        .setVarLengthDictionaryColumns(List.of(STRING_COLUMN3))
        .setOnHeapDictionaryColumns(List.of(LONG_COLUMN3))
        .setColumnMajorSegmentBuilderEnabled(true)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.forward(),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW))
        .setIndex(Set.of(STRING_COLUMN3), StandardIndexes.dictionary(), new DictionaryIndexConfig(false, true))
        .setIndex(Set.of(STRING_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

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
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(List.of(STRING_COLUMN1, LONG_COLUMN1))
        .setSortedColumn(LONG_COLUMN1)
        .setRangeIndexColumns(List.of(STRING_COLUMN2))
        .setNoDictionaryColumns(List.of(LONG_COLUMN2))
        .setVarLengthDictionaryColumns(List.of(STRING_COLUMN3))
        .setOnHeapDictionaryColumns(List.of(LONG_COLUMN3))
        .setColumnMajorSegmentBuilderEnabled(true)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LONG_COLUMN2), StandardIndexes.forward(),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW))
        .setIndex(Set.of(STRING_COLUMN3), StandardIndexes.dictionary(), new DictionaryIndexConfig(false, true))
        .setIndex(Set.of(STRING_COLUMN1, LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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
    // Format: {optimizeDictionaryType, expectedCRC}, crc is used here to check the correct dictionary type was used.
    return new Object[][]{
        {true, "145089250"},
        {false, "2079382792"},
    };
  }

  @Test(dataProvider = "optimizeDictionaryTypeParams")
  public void testOptimizeDictionaryTypeConversion(Object[] params)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setColumnMajorSegmentBuilderEnabled(true)
        .setOptimizeDictionaryType((boolean) params[0])
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setIndex(Set.of(STRING_COLUMN1, STRING_COLUMN2), StandardIndexes.dictionary(), DictionaryIndexConfig.DEFAULT)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(Long.toString(segmentMetadata.getCrc()), params[1]);

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
    FieldConfig textIndexFieldConfig = new FieldConfig.Builder(STRING_COLUMN1)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexTypes(List.of(FieldConfig.IndexType.TEXT))
        .withProperties(fieldConfigColumnProperties)
        .build();
    List<FieldConfig> fieldConfigList = List.of(textIndexFieldConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(List.of(LONG_COLUMN1))
        .setNoDictionaryColumns(List.of(STRING_COLUMN1))
        .setSortedColumn(sortedColumn)
        .setColumnMajorSegmentBuilderEnabled(columnMajorSegmentBuilder)
        .setFieldConfigList(fieldConfigList)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    TextIndexConfig textIndexConfig = new TextIndexConfigBuilder()
        .withUseANDForMultiTermQueries(false)
        .withReuseMutableIndex(reuseMutableIndex)
        .withLuceneNRTCachingDirectoryMaxBufferSizeMB(luceneNRTCachingDirectoryMaxBufferSizeMB)
        .withRawValueForTextIndex(rawValueForTextIndex)
        .build();
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setIndex(Set.of(LONG_COLUMN1), StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setIndex(Set.of(STRING_COLUMN1), StandardIndexes.text(), textIndexConfig)
        .setIndex(Set.of(STRING_COLUMN1), StandardIndexes.dictionary(), dictionaryIndexConfig)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());
    if (!dictionaryIndexConfig.isEnabled()) {
      realtimeSegmentConfigBuilder.setIndex(Set.of(STRING_COLUMN1), StandardIndexes.forward(),
          ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW));
    }
    RealtimeSegmentConfig realtimeSegmentConfig = realtimeSegmentConfigBuilder.build();

    // create mutable segment impl
    RealtimeLuceneTextIndexSearcherPool.init(1);
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    ImmutableSegmentImpl segmentFile = null;
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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

  @DataProvider
  public Object[][] dictionaryTextConversionParams() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "dictionaryTextConversionParams")
  public void testDictionaryBasedTextIndexConversion(boolean compacted)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "dictionaryText_" + compacted);
    Map<String, String> properties = Map.of(FieldConfig.TEXT_INDEX_BUILD_ON_DICTIONARY, "true",
        FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX, "true");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN).setColumnMajorSegmentBuilderEnabled(false)
        .setFieldConfigList(List.of(new FieldConfig.Builder(STRING_COLUMN1)
            .withEncodingType(FieldConfig.EncodingType.DICTIONARY).withIndexTypes(List.of(FieldConfig.IndexType.TEXT))
            .withProperties(properties).build())).build();
    if (compacted) {
      UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
      upsertConfig.setEnableCommitTimeCompaction(true);
      tableConfig.setUpsertConfig(upsertConfig);
    }
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    String segmentName = "testTable__0__0__123456";
    RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableConfig.getTableName()).setSegmentName(segmentName)
        .setStreamName(tableConfig.getTableName()).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(10).setIndex(Set.of(STRING_COLUMN1), StandardIndexes.text(),
            new TextIndexConfigBuilder().withProperties(properties).build())
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath()).build();
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    for (int docId : new int[]{0, 1, 2, 5}) {
      validDocIds.add(docId);
    }
    RealtimeLuceneTextIndexSearcherPool.init(1);
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(config, null) {
      @Override
      public ThreadSafeMutableRoaringBitmap getValidDocIds() {
        // Supply the upsert snapshot to exercise CompactedColumnStatistics without testing metadata management.
        return compacted ? validDocIds : super.getValidDocIds();
      }
    };
    try {
      String[] values = {"beta", "alpha", "alpha", "gamma", "alpha", "beta"};
      for (int i = 0; i < values.length; i++) {
        GenericRow row = new GenericRow();
        row.putValue(STRING_COLUMN1, values[i]);
        row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
        mutableSegment.index(row, null);
      }
      RealtimeLuceneTextIndex consumingIndex =
          (RealtimeLuceneTextIndex) mutableSegment.getDataSource(STRING_COLUMN1).getTextIndex();
      consumingIndex.getSearcherManager().maybeRefreshBlocking();
      assertFalse(consumingIndex.isBuildOnDictionary());
      assertEquals(consumingIndex.getDocIds("alpha").toArray(), new int[]{1, 2, 4});

      File outputDir = new File(tmpDir, "outputDir");
      new RealtimeSegmentConverter(mutableSegment, new SegmentZKPropsConfig(), outputDir.getAbsolutePath(), schema,
          tableConfig.getTableName(), tableConfig, segmentName, false).build(SegmentVersion.v3);
      ImmutableSegment sealedSegment = ImmutableSegmentLoader.load(new File(outputDir, segmentName),
          new IndexLoadingConfig(tableConfig, schema), false);
      try {
        TextIndexReader sealedIndex = sealedSegment.getIndex(STRING_COLUMN1, StandardIndexes.text());
        assertTrue(sealedIndex.isBuildOnDictionary());
        Dictionary dictionary = sealedSegment.getDataSource(STRING_COLUMN1).getDictionary();
        ImmutableRoaringBitmap matchingDictIds = sealedIndex.getDictIds("alpha");
        assertEquals(matchingDictIds.toArray(), new int[]{dictionary.indexOf("alpha")});
        assertEquals(sealedSegment.getSegmentMetadata().getTotalDocs(), compacted ? 4 : 6);
        List<Integer> matchingDocIds = new ArrayList<>();
        for (int docId = 0; docId < sealedSegment.getSegmentMetadata().getTotalDocs(); docId++) {
          if (matchingDictIds.contains(dictionary.indexOf((String) sealedSegment.getValue(docId, STRING_COLUMN1)))) {
            matchingDocIds.add(docId);
          }
        }
        assertEquals(matchingDocIds, compacted ? List.of(1, 2) : List.of(1, 2, 4));
        assertEquals(sealedIndex.getDictIds("gamma").isEmpty(), compacted);
      } finally {
        sealedSegment.destroy();
      }
    } finally {
      mutableSegment.destroy();
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
        Map.of(STRING_COLUMN1,
            new ColumnPartitionConfig("BoundedColumnValue", 4, functionConfig)));

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "BoundedColumnValue", 4, functionConfig);
    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setPartitionColumn(STRING_COLUMN1)
        .setPartitionFunction(partitionFunction)
        .setPartitionId(0)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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
        Map.of(STRING_COLUMN1,
            new ColumnPartitionConfig("Murmur3", 5, functionConfig)));

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "Murmur3", 5, functionConfig);
    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setPartitionColumn(STRING_COLUMN1)
        .setPartitionFunction(partitionFunction)
        .setPartitionId(0)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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
        Map.of(STRING_COLUMN1,
            new ColumnPartitionConfig("FNV", 5, functionConfig)));

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(
        "FNV", 5, functionConfig);
    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(3)
        .setPartitionColumn(STRING_COLUMN1)
        .setPartitionFunction(partitionFunction)
        .setPartitionId(0)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

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
    DataType[] dataTypes = {
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.BIG_DECIMAL,
        DataType.BOOLEAN,
        DataType.TIMESTAMP,
        DataType.STRING,
        DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : dataTypes) {
      params.add(new Object[]{dataType, true});   // dictionary-encoded
      params.add(new Object[]{dataType, false});  // no-dictionary
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that the isSorted metadata flag is correctly set on both the mutable segment (via sorted doc ID order)
  /// and the converted immutable segment (via ColumnMetadata) for all supported single-value data types, both
  /// dictionary-encoded and no-dictionary.
  @Test(dataProvider = "sortedColumnParams")
  public void testSortedColumnMetadata(DataType dataType, boolean enableDictionary)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_sorted_" + dataType + "_" + enableDictionary + "_" + System.nanoTime());
    String sortedColumn = "sorted_col";
    String dupColumn = "sorted_col2";

    List<String> noDictColumns = enableDictionary ? List.of() : List.of(sortedColumn, dupColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSortedColumn(sortedColumn)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(sortedColumn, dataType)
        .addSingleValueDimension(dupColumn, dataType)
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema)
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
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      DataType storedType = dataType.getStoredType();
      Object prevValue = null;
      for (int docId : sortedDocIds) {
        Object currValue = mutableSegment.getValue(docId, sortedColumn);
        if (prevValue != null) {
          assertTrue(compareValues(storedType, currValue, prevValue) >= 0,
              "Mutable segment: " + sortedColumn + " values must be non-decreasing in sorted doc ID order"
                  + " (dataType=" + dataType + ", enableDictionary=" + enableDictionary + ")");
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
      converter.build(SegmentVersion.v3);

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(outputDir, segmentName));

      ColumnMetadata sortedColMeta = segmentMetadata.getColumnMetadataFor(sortedColumn);
      assertTrue(sortedColMeta.isSorted(),
          "Immutable segment: sorted column must have isSorted=true (dataType=" + dataType + ", enableDictionary="
              + enableDictionary + ")");
      assertEquals(sortedColMeta.hasDictionary(), enableDictionary,
          "Immutable segment: dictionary encoding must match config (dataType=" + dataType + ", enableDictionary="
              + enableDictionary + ")");

      // A non-sorted column must not be marked as sorted
      ColumnMetadata nonSortedColMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertFalse(nonSortedColMeta.isSorted(), "Immutable segment: non-sorted column must have isSorted=false");

      // Duplicate column with same values must also be detected as sorted
      ColumnMetadata dupColMeta = segmentMetadata.getColumnMetadataFor(dupColumn);
      assertTrue(dupColMeta.isSorted(),
          "Immutable segment: duplicate column with same values must also have isSorted=true (dataType=" + dataType
              + ", enableDictionary=" + enableDictionary + ")");
      assertEquals(dupColMeta.hasDictionary(), enableDictionary,
          "Immutable segment: dictionary encoding must match config for duplicate column (dataType=" + dataType
              + ", enableDictionary=" + enableDictionary + ")");

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
                    + " (dataType=" + dataType + ", enableDictionary=" + enableDictionary + ")");
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
  public void testPreSortedRecordsWithoutSortedColumn(DataType dataType, boolean enableDictionary)
      throws Exception {
    File tmpDir =
        new File(TMP_DIR, "tmp_presorted_" + dataType + "_" + enableDictionary + "_" + System.nanoTime());
    String preSortedColumn = "sorted_col";

    List<String> noDictColumns = enableDictionary ? List.of() : List.of(preSortedColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(preSortedColumn, dataType)
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema)
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
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(realtimeSegmentConfig, null);
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
      converter.build(SegmentVersion.v3);

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(outputDir, segmentName));

      ColumnMetadata preSortedColMeta = segmentMetadata.getColumnMetadataFor(preSortedColumn);
      assertTrue(preSortedColMeta.isSorted(),
          "Pre-sorted column must have isSorted=true even without sortedColumn config (dataType=" + dataType
              + ", enableDictionary=" + enableDictionary + ")");
      assertEquals(preSortedColMeta.hasDictionary(), enableDictionary,
          "Dictionary encoding must match config (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ")");

      ColumnMetadata nonSortedColMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN1);
      assertFalse(nonSortedColMeta.isSorted(), "Non-sorted column must have isSorted=false");
    } finally {
      mutableSegment.destroy();
    }
  }

  @DataProvider
  public static Object[][] emptySegmentParams() {
    DataType[] dataTypes = {
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.BIG_DECIMAL,
        DataType.BOOLEAN,
        DataType.TIMESTAMP,
        DataType.STRING,
        DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : dataTypes) {
      for (boolean enableDictionary : new boolean[]{true, false}) {
        for (boolean sorted : new boolean[]{true, false}) {
          params.add(new Object[]{dataType, enableDictionary, sorted});
        }
      }
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that converting an empty mutable segment (zero rows) to an immutable segment succeeds for all supported
  /// single-value data types, both dictionary-encoded and no-dictionary, with and without a sorted column configured.
  @Test(dataProvider = "emptySegmentParams")
  public void testEmptySegmentAllDataTypes(DataType dataType, boolean enableDictionary, boolean sorted)
      throws Exception {
    File tmpDir = new File(TMP_DIR,
        "tmp_empty_" + dataType + "_dict" + enableDictionary + "_sorted" + sorted + "_" + System.nanoTime());
    String testColumn = "test_col";

    List<String> noDictColumns = enableDictionary ? List.of() : List.of(testColumn);
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false);
    if (sorted) {
      tableConfigBuilder.setSortedColumn(testColumn);
    }
    TableConfig tableConfig = tableConfigBuilder.build();

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(testColumn, dataType)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema)
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
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      // No rows indexed — empty segment
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegment, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      assertEquals(segmentMetadata.getTotalDocs(), 0,
          "Empty segment must have zero docs (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ", sorted=" + sorted + ")");
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
      assertEquals(segmentMetadata.getStartTime(), segmentMetadata.getEndTime(),
          "Start time and end time must be equal for an empty segment");
      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()),
          "Converted segment must contain all schema columns");
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");

      ColumnMetadata colMeta = segmentMetadata.getColumnMetadataFor(testColumn);
      assertNotNull(colMeta,
          "Column metadata must be present (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ", sorted=" + sorted + ")");
      assertTrue(colMeta.isSingleValue(),
          "Column must be single-valued (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ", sorted=" + sorted + ")");
      // Empty segments always have hasDictionary=false: BaseSegmentCreator.initColSegmentCreationInfo() skips
      // dictionary creator construction when _totalDocs == 0 (every column falls into the else branch that uses a
      // null dictionary creator), so writeMetadata() records hasDictionary=(dictionaryCreator != null) = false
      // for every column regardless of the configured encoding.
      assertFalse(colMeta.hasDictionary(),
          "Empty segment must always have hasDictionary=false (dataType=" + dataType + ", enableDictionary="
              + enableDictionary + ", sorted=" + sorted + ")");
      // An empty segment is vacuously sorted regardless of configuration
      assertTrue(colMeta.isSorted(),
          "Empty segment column must be vacuously sorted (dataType=" + dataType + ", enableDictionary="
              + enableDictionary + ", sorted=" + sorted + ")");
    } finally {
      mutableSegment.destroy();
    }
  }

  @DataProvider
  public static Object[][] emptySegmentMvParams() {
    // Raw (no-dictionary) MV only supports fixed-width stored types in the mutable forward index.
    // Variable-width MV raw (STRING, BYTES) is not yet supported in the mutable segment
    // (see ForwardIndexType: "TODO: Add support for variable width MV RAW column types").
    DataType[] rawMvTypes = {
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.TIMESTAMP,
    };
    // Dictionary-encoded MV supports all common MV types including variable-width ones.
    DataType[] dictMvTypes = {
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.TIMESTAMP,
        DataType.STRING,
        DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : rawMvTypes) {
      params.add(new Object[]{dataType, false});
    }
    for (DataType dataType : dictMvTypes) {
      params.add(new Object[]{dataType, true});
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that converting an empty mutable segment (zero rows) to an immutable segment succeeds for all supported
  /// multi-value data types, both dictionary-encoded and raw (no-dictionary).
  @Test(dataProvider = "emptySegmentMvParams")
  public void testEmptySegmentMvAllDataTypes(DataType dataType, boolean enableDictionary)
      throws Exception {
    File tmpDir = new File(TMP_DIR,
        "tmp_empty_mv_" + dataType + "_dict" + enableDictionary + "_" + System.nanoTime());
    String testColumn = "test_mv_col";

    List<String> noDictColumns = enableDictionary ? List.of() : List.of(testColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setNoDictionaryColumns(noDictColumns)
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addMultiValueDimension(testColumn, dataType)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setAvgNumMultiValues(2)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      // No rows indexed — empty segment
      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegment, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);

      assertEquals(segmentMetadata.getTotalDocs(), 0,
          "Empty MV segment must have zero docs (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ")");
      assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
      assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
      assertEquals(segmentMetadata.getStartTime(), segmentMetadata.getEndTime(),
          "Start time and end time must be equal for an empty MV segment");
      assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()),
          "Converted segment must contain all schema columns");
      assertEquals(segmentMetadata.getStartOffset(), "1");
      assertEquals(segmentMetadata.getEndOffset(), "100");

      ColumnMetadata colMeta = segmentMetadata.getColumnMetadataFor(testColumn);
      assertNotNull(colMeta,
          "Column metadata must be present (dataType=" + dataType + ", enableDictionary=" + enableDictionary + ")");
      assertFalse(colMeta.isSingleValue(),
          "Column must be multi-valued (dataType=" + dataType + ", enableDictionary=" + enableDictionary + ")");
      // Empty segments always have hasDictionary=false (see testEmptySegmentAllDataTypes comment for full explanation).
      assertFalse(colMeta.hasDictionary(),
          "Empty MV segment must always have hasDictionary=false (dataType=" + dataType + ", enableDictionary="
              + enableDictionary + ")");
      // MV columns are never sorted
      assertFalse(colMeta.isSorted(),
          "MV column must always have isSorted=false (dataType=" + dataType + ", enableDictionary=" + enableDictionary
              + ")");
    } finally {
      mutableSegment.destroy();
    }
  }

  /// Returns the i-th ascending value for the given data type, out of numRows total rows.
  private static Object sortedValue(DataType dataType, int i, int numRows) {
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
  private static int compareValues(DataType storedType, Object v1, Object v2) {
    if (storedType == DataType.BYTES) {
      return ByteArray.compare((byte[]) v1, (byte[]) v2);
    }
    return storedType.compare(v1, v2);
  }

  /// Ingestion-aggregated metric columns are forced to be no-dictionary and skip min/max tracking during consumption
  /// (their values mutate in place). A BitSliced (version 2) range index on such a column previously failed to build
  /// because the creator had no value domain. This test verifies the full seal path: after conversion, the aggregated
  /// no-dictionary metric column has recovered min/max in its column metadata and carries a range index.
  @Test
  public void testRangeIndexOnIngestionAggregatedNoDictColumn()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    IngestionConfig ingestionConfig = new IngestionConfig();
    List<AggregationConfig> aggregationConfigs =
        List.of(new AggregationConfig(LONG_COLUMN4, "SUM(" + LONG_COLUMN4 + ")"));
    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setIngestionConfig(ingestionConfig)
        .setNoDictionaryColumns(List.of(LONG_COLUMN4))
        .setRangeIndexColumns(List.of(LONG_COLUMN4))
        .setColumnMajorSegmentBuilderEnabled(false)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(STRING_COLUMN1, DataType.STRING)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setIngestionAggregationConfigs(aggregationConfigs)
        .setIndex(Set.of(LONG_COLUMN4), StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(Set.of(LONG_COLUMN4), StandardIndexes.forward(),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW))
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      // Each row carries a distinct dimension/time value, so no rows collapse and the aggregated metric retains its
      // per-row values 64..73 (SUM over a single-row group). The mutable segment reports null min/max for it.
      List<GenericRow> rows = generateTestData();
      for (GenericRow row : rows) {
        mutableSegmentImpl.index(row, null);
      }

      File outputDir = new File(tmpDir, "outputDir");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegmentImpl, new SegmentZKPropsConfig(), outputDir.getAbsolutePath(),
              schema, tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      ColumnMetadata metricMetadata = segmentMetadata.getColumnMetadataFor(LONG_COLUMN4);
      assertFalse(metricMetadata.hasDictionary(), "Aggregated metric column must stay no-dictionary");
      // Min/max are recovered by scanning the sealed forward index despite the mutable segment not tracking them.
      assertEquals(metricMetadata.getMinValue(), 64L);
      assertEquals(metricMetadata.getMaxValue(), 73L);

      // The BitSliced range index must be present on the aggregated no-dictionary column.
      try (SegmentLocalFSDirectory segmentDir = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
          SegmentDirectory.Reader segmentReader = segmentDir.createReader()) {
        assertTrue(segmentReader.hasIndexFor(LONG_COLUMN4, StandardIndexes.range()),
            "Range index should be built on the aggregated no-dictionary column");
      }
    } finally {
      mutableSegmentImpl.destroy();
    }
  }

  @DataProvider
  public static Object[][] noDictSortedAggregationKeyParams() {
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : new DataType[]{DataType.INT, DataType.LONG, DataType.STRING, DataType.BYTES}) {
      for (boolean legacyAggregateMetrics : new boolean[]{false, true}) {
        params.add(new Object[]{dataType, false, legacyAggregateMetrics});  // row-major segment builder
        params.add(new Object[]{dataType, true, legacyAggregateMetrics});   // column-major segment builder
      }
    }
    return params.toArray(new Object[0][]);
  }

  /// A sorted DIMENSION column configured as no-dictionary gets no inverted index in the consuming segment, but
  /// ingestion aggregation forces a dictionary on it because it is part of the aggregation key. The sort step at seal
  /// time used to take the inverted-index path because the column had a dictionary, and failed with an NPE. This
  /// test verifies that the segment builds, the rows aggregate, and the sorted column stays sorted and raw. It covers
  /// both the ingestion `aggregationConfigs` and the legacy `aggregateMetrics` flag (SUM on every metric).
  @Test(dataProvider = "noDictSortedAggregationKeyParams")
  public void testNoDictSortedColumnAsIngestionAggregationKey(DataType dataType, boolean columnMajorSegmentBuilder,
      boolean legacyAggregateMetrics)
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_sorted_agg_" + dataType + "_" + columnMajorSegmentBuilder + "_"
        + legacyAggregateMetrics + "_" + System.nanoTime());
    String sortedColumn = "sorted_col";
    List<AggregationConfig> aggregationConfigs = legacyAggregateMetrics ? List.of()
        : List.of(new AggregationConfig(LONG_COLUMN4, "SUM(" + LONG_COLUMN4 + ")"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setAggregationConfigs(aggregationConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setSortedColumn(sortedColumn)
        .setNoDictionaryColumns(List.of(sortedColumn, LONG_COLUMN4))
        .setIngestionConfig(ingestionConfig)
        .setAggregateMetrics(legacyAggregateMetrics)
        .setColumnMajorSegmentBuilderEnabled(columnMajorSegmentBuilder)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension(sortedColumn, dataType)
        .addMetric(LONG_COLUMN4, DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";

    RealtimeSegmentConfig realtimeSegmentConfig = new RealtimeSegmentConfig.Builder(tableConfig, schema)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(segmentName)
        .setStreamName(tableNameWithType)
        .setSchema(schema)
        .setTimeColumnName(DATE_TIME_COLUMN)
        .setCapacity(1000)
        .setIngestionAggregationConfigs(aggregationConfigs)
        .setAggregateMetrics(legacyAggregateMetrics)
        .setSegmentZKMetadata(getSegmentZKMetadata(segmentName))
        .setOffHeap(true)
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(tmpDir, "stats")))
        .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath())
        .build();

    MutableSegmentImpl mutableSegment = new MutableSegmentImpl(realtimeSegmentConfig, null);
    try {
      // Key i has (i % 3) + 1 distinct time values, so the sort buckets have unequal sizes (1, 2 or 3 docs). Each
      // (key, time) pair is inserted twice, keys in reverse sorted order, so that rows aggregate and the conversion
      // must reorder them. For one key, the docIds follow the time order, so a stable sort keeps the times ascending.
      // Row (i, t) carries metric value i + 1, so the aggregated value for each (key i, time) pair is 2 * (i + 1).
      int numKeys = 10;
      int maxTimesPerKey = 3;
      long baseTime = 1697814309L;
      int expectedNumDocs = 0;
      for (int i = 0; i < numKeys; i++) {
        expectedNumDocs += numTimes(i, maxTimesPerKey);
      }
      for (int round = 0; round < 2; round++) {
        for (int t = 0; t < maxTimesPerKey; t++) {
          for (int i = numKeys - 1; i >= 0; i--) {
            if (t < numTimes(i, maxTimesPerKey)) {
              GenericRow row = new GenericRow();
              row.putValue(sortedColumn, sortedValue(dataType, i, numKeys));
              row.putValue(LONG_COLUMN4, (long) (i + 1));
              row.putValue(DATE_TIME_COLUMN, baseTime + t);
              mutableSegment.index(row, null);
            }
          }
        }
      }
      assertEquals(mutableSegment.getNumDocsIndexed(), expectedNumDocs);

      File outputDir = new File(tmpDir, "outputDir");
      SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
      segmentZKPropsConfig.setStartOffset("1");
      segmentZKPropsConfig.setEndOffset("100");
      RealtimeSegmentConverter converter =
          new RealtimeSegmentConverter(mutableSegment, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
              tableNameWithType, tableConfig, segmentName, false);
      converter.build(SegmentVersion.v3);

      File indexDir = new File(outputDir, segmentName);
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
      assertEquals(segmentMetadata.getTotalDocs(), expectedNumDocs);
      ColumnMetadata sortedColMeta = segmentMetadata.getColumnMetadataFor(sortedColumn);
      assertTrue(sortedColMeta.isSorted(), "Sorted column must have isSorted=true");
      assertFalse(sortedColMeta.hasDictionary(), "Sorted column must stay no-dictionary in the immutable segment");

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
        DataType storedType = dataType.getStoredType();
        int docId = 0;
        for (int i = 0; i < numKeys; i++) {
          for (int t = 0; t < numTimes(i, maxTimesPerKey); t++, docId++) {
            assertEquals(compareValues(storedType, immutableSegment.getValue(docId, sortedColumn),
                sortedValue(dataType, i, numKeys)), 0, "Unexpected sorted column value at docId=" + docId);
            assertEquals(immutableSegment.getValue(docId, DATE_TIME_COLUMN), baseTime + t,
                "Unexpected time value at docId=" + docId);
            assertEquals(immutableSegment.getValue(docId, LONG_COLUMN4), 2L * (i + 1),
                "Unexpected aggregated metric value at docId=" + docId);
          }
        }
      } finally {
        immutableSegment.destroy();
      }
    } finally {
      mutableSegment.destroy();
    }
  }

  private static int numTimes(int key, int maxTimesPerKey) {
    return key % maxTimesPerKey + 1;
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
