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
package org.apache.pinot.realtime.converter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentReader;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.ColumnIndicesForRealtimeTable;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeSegmentConverterTest {

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

  public void setup() {
    Preconditions.checkState(TMP_DIR.mkdirs());
  }

  @Test
  public void testNoVirtualColumnsInSchema() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "col1"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "col2")).build();
    String segmentName = "segment1";
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    assertEquals(schema.getColumnNames().size(), 5);
    Schema newSchema = RealtimeSegmentConverter.getUpdatedSchema(schema);
    assertEquals(newSchema.getColumnNames().size(), 2);
  }

  @Test
  public void testNoRecordsIndexedRowMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
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
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

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

    File outputDir = new File(tmpDir, "outputDir");
    SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
    segmentZKPropsConfig.setStartOffset("1");
    segmentZKPropsConfig.setEndOffset("100");
    ColumnIndicesForRealtimeTable cdc = new ColumnIndicesForRealtimeTable(indexingConfig.getSortedColumn().get(0),
        indexingConfig.getInvertedIndexColumns(), null, null, indexingConfig.getNoDictionaryColumns(),
        indexingConfig.getVarLengthDictionaryColumns());
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
            tableNameWithType, tableConfig, segmentName, cdc, false, false);
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
  }

  @Test
  public void testNoRecordsIndexedColumnMajorSegmentBuilder()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
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
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT).addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

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

    File outputDir = new File(tmpDir, "outputDir");
    SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
    segmentZKPropsConfig.setStartOffset("1");
    segmentZKPropsConfig.setEndOffset("100");
    ColumnIndicesForRealtimeTable cdc = new ColumnIndicesForRealtimeTable(indexingConfig.getSortedColumn().get(0),
        indexingConfig.getInvertedIndexColumns(), null, null, indexingConfig.getNoDictionaryColumns(),
        indexingConfig.getVarLengthDictionaryColumns());
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
            tableNameWithType, tableConfig, segmentName, cdc, false, true);
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
  }

  @Test
  public void testNoRecordsIndexedColumnMajorSegmentBuilderX()
      throws Exception {
    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
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
        .addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

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
    List<GenericRow> rows = generateTestData();

    for(GenericRow row : rows) {
      mutableSegmentImpl.index(row, null);
    }

    File outputDir = new File(tmpDir, "outputDir");
    SegmentZKPropsConfig segmentZKPropsConfig = new SegmentZKPropsConfig();
    segmentZKPropsConfig.setStartOffset("1");
    segmentZKPropsConfig.setEndOffset("100");
    ColumnIndicesForRealtimeTable cdc = new ColumnIndicesForRealtimeTable(indexingConfig.getSortedColumn().get(0),
        indexingConfig.getInvertedIndexColumns(), null, null, indexingConfig.getNoDictionaryColumns(),
        indexingConfig.getVarLengthDictionaryColumns());
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(mutableSegmentImpl, segmentZKPropsConfig, outputDir.getAbsolutePath(), schema,
            tableNameWithType, tableConfig, segmentName, cdc, false, true);
    converter.build(SegmentVersion.v3, null);

    File indexDir = new File(outputDir, segmentName);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
    assertEquals(segmentMetadata.getTotalDocs(), rows.size());
    assertEquals(segmentMetadata.getTimeColumn(), DATE_TIME_COLUMN);
    assertEquals(segmentMetadata.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(segmentMetadata.getStartTime(), segmentMetadata.getEndTime() - (rows.size() - 1));
    assertTrue(segmentMetadata.getAllColumns().containsAll(schema.getColumnNames()));
    assertEquals(segmentMetadata.getStartOffset(), "1");
    assertEquals(segmentMetadata.getEndOffset(), "100");

    testSegment(rows, indexDir, tableConfig, segmentMetadata);
  }

  private void testSegment(List<GenericRow> rows, File indexDir, TableConfig tableConfig, SegmentMetadataImpl segmentMetadata)
      throws IOException {
    SegmentLocalFSDirectory segmentDir = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
    SegmentDirectory.Reader segmentReader = segmentDir.createReader();

    Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
    Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      // FIXME: text-index only works with local SegmentDirectory
      indexContainerMap.put(entry.getKey(),
          new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig));
    }
    ImmutableSegmentImpl segment_file = new ImmutableSegmentImpl(segmentDir, segmentMetadata, indexContainerMap, null);

    GenericRow readRow = new GenericRow();
    int docId = 0;
    for(GenericRow row : rows) {
      segment_file.getRecord(docId, readRow);
      assertEquals(readRow.getValue(STRING_COLUMN1), row.getValue(STRING_COLUMN1));
      assertEquals(readRow.getValue(STRING_COLUMN2), row.getValue(STRING_COLUMN2));
      assertEquals(readRow.getValue(STRING_COLUMN3), row.getValue(STRING_COLUMN3));
      assertEquals(readRow.getValue(STRING_COLUMN4), row.getValue(STRING_COLUMN4));
      assertEquals(readRow.getValue(LONG_COLUMN1), row.getValue(LONG_COLUMN1));
      assertEquals(readRow.getValue(LONG_COLUMN2), row.getValue(LONG_COLUMN2));
      assertEquals(readRow.getValue(LONG_COLUMN3), row.getValue(LONG_COLUMN3));
      assertEquals(readRow.getValue(LONG_COLUMN4), row.getValue(LONG_COLUMN4));
      assertEquals(readRow.getValue(DATE_TIME_COLUMN), row.getValue(DATE_TIME_COLUMN));

      docId += 1;
    }
  }

  private List<GenericRow> generateTestData() {
    LinkedList<GenericRow> rows = new LinkedList<>();

    for(int i = 0; i < 10; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN1, "Hello" + i);
      row.putValue(STRING_COLUMN2, "World" + i);
      row.putValue(STRING_COLUMN3, "F1" + i);
      row.putValue(STRING_COLUMN4, "F2" + i);
      row.putValue(LONG_COLUMN1, 67L + i);
      row.putValue(LONG_COLUMN2, 66L + i);
      row.putValue(LONG_COLUMN3, 65L + i);
      row.putValue(LONG_COLUMN4, 64L + i);
      row.putValue(DATE_TIME_COLUMN, 1697814309L + i);
      rows.add(row);
    }

    return rows;
  }

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    return segmentZKMetadata;
  }

  public void destroy() {
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
