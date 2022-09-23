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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ForwardIndexHandlerTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ForwardIndexHandlerTest");
  private static final String TABLE_NAME = "myTable";
  private static final String SEGMENT_NAME = "testSegment";

  // TODOs:
  // 1. Add other datatypes (double, float, bigdecimal, bytes). Also add MV columns.
  // 2. Add text index and other index types for raw columns.
  private static final String SNAPPY_STRING = "SNAPPY_STRING";
  private static final String PASS_THROUGH_STRING = "PASS_THROUGH_STRING";
  private static final String ZSTANDARD_STRING = "ZSTANDARD_STRING";
  private static final String LZ4_STRING = "LZ4_STRING";

  private static final String SNAPPY_LONG = "SNAPPY_LONG";
  private static final String PASS_THROUGH_LONG = "PASS_THROUGH_LONG";
  private static final String ZSTANDARD_LONG = "ZSTANDARD_LONG";
  private static final String LZ4_LONG = "LZ4_LONG";

  private static final String SNAPPY_INTEGER = "SNAPPY_INTEGER";
  private static final String PASS_THROUGH_INTEGER = "PASS_THROUGH_INTEGER";
  private static final String ZSTANDARD_INTEGER = "ZSTANDARD_INTEGER";
  private static final String LZ4_INTEGER = "LZ4_INTEGER";

  private static final String DICT_INTEGER = "DICT_INTEGER";
  private static final String DICT_STRING = "DICT_STRING";
  private static final String DICT_LONG = "DICT_LONG";

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(SNAPPY_STRING, SNAPPY_LONG, SNAPPY_INTEGER);

  private static final List<String> RAW_ZSTANDARD_INDEX_COLUMNS =
      Arrays.asList(ZSTANDARD_STRING, ZSTANDARD_LONG, ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_INDEX_COLUMNS =
      Arrays.asList(PASS_THROUGH_STRING, PASS_THROUGH_LONG, PASS_THROUGH_INTEGER);

  private static final List<String> RAW_LZ4_INDEX_COLUMNS = Arrays.asList(LZ4_STRING, LZ4_LONG, LZ4_INTEGER);

  private List<String> _noDictionaryColumns = new ArrayList<>();
  TableConfig _tableConfig;
  Schema _schema;
  private SegmentMetadataImpl _existingSegmentMetadata;
  private SegmentDirectory.Writer _writer;
  private List<GenericRow> _rows;

  @BeforeClass
  public void setUp()
      throws Exception {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
  }

  private void buildSegment()
      throws Exception {
    _rows = createTestData();

    List<FieldConfig> fieldConfigs = new ArrayList<>(
        RAW_SNAPPY_INDEX_COLUMNS.size() + RAW_ZSTANDARD_INDEX_COLUMNS.size() + RAW_PASS_THROUGH_INDEX_COLUMNS.size()
            + RAW_LZ4_INDEX_COLUMNS.size());

    for (String indexColumn : RAW_SNAPPY_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.SNAPPY, null));
    }

    for (String indexColumn : RAW_ZSTANDARD_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.ZSTANDARD, null));
    }

    for (String indexColumn : RAW_PASS_THROUGH_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.PASS_THROUGH, null));
    }

    for (String indexColumn : RAW_LZ4_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.LZ4, null));
    }

    _noDictionaryColumns.addAll(RAW_SNAPPY_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_ZSTANDARD_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_PASS_THROUGH_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_LZ4_INDEX_COLUMNS);

    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SNAPPY_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ZSTANDARD_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LZ4_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SNAPPY_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(ZSTANDARD_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(LZ4_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(SNAPPY_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(ZSTANDARD_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(PASS_THROUGH_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LZ4_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DICT_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DICT_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DICT_STRING, FieldSpec.DataType.STRING).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(_rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    File segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    _existingSegmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    _writer = new SegmentLocalFSDirectory(segmentDirectory, _existingSegmentMetadata, ReadMode.mmap).createWriter();
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();

    //Generate random data
    int rowLength = 1000;
    Random random = new Random();
    String[] tempStringRows = new String[rowLength];
    Integer[] tempIntRows = new Integer[rowLength];
    Long[] tempLongRows = new Long[rowLength];

    for (int i = 0; i < rowLength; i++) {
      //Adding a fixed value to check for filter queries
      if (i % 10 == 0) {
        tempStringRows[i] = "testRow";
        tempIntRows[i] = 1001;
        tempLongRows[i] = 1001L;
      } else {
        tempStringRows[i] = "n" + i;
        tempIntRows[i] = i;
        tempLongRows[i] = (long) i;
      }
    }

    for (int i = 0; i < rowLength; i++) {
      GenericRow row = new GenericRow();

      // Raw String columns
      row.putValue(SNAPPY_STRING, tempStringRows[i]);
      row.putValue(ZSTANDARD_STRING, tempStringRows[i]);
      row.putValue(PASS_THROUGH_STRING, tempStringRows[i]);
      row.putValue(LZ4_STRING, tempStringRows[i]);

      // Raw integer columns
      row.putValue(SNAPPY_INTEGER, tempIntRows[i]);
      row.putValue(ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(LZ4_INTEGER, tempIntRows[i]);

      // Raw long columns
      row.putValue(SNAPPY_LONG, tempLongRows[i]);
      row.putValue(ZSTANDARD_LONG, tempLongRows[i]);
      row.putValue(PASS_THROUGH_LONG, tempLongRows[i]);
      row.putValue(LZ4_LONG, tempLongRows[i]);

      // Dictionary columns
      row.putValue(DICT_INTEGER, tempIntRows[i]);
      row.putValue(DICT_LONG, tempLongRows[i]);
      row.putValue(DICT_STRING, tempStringRows[i]);

      rows.add(row);
    }
    return rows;
  }

  @Test
  public void testComputeOperation() {
    // TEST1 : Validate with zero changes.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    ForwardIndexHandler fwdIndexHandler =
        new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    Map<String, ForwardIndexHandler.Operation> operationMap = new HashMap<>();
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST2: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().remove(ZSTANDARD_STRING);
    fwdIndexHandler = new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST3: Disable dictionary
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().add(DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST4: Add random index
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getTextIndexColumns().add(DICT_INTEGER);
    indexLoadingConfig.getTextIndexColumns().add(LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST5: Change compression

    // Create new tableConfig with the modified fieldConfigs.
    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    FieldConfig config = fieldConfigs.remove(0);
    FieldConfig newConfig = new FieldConfig(config.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        FieldConfig.CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    fwdIndexHandler = new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_COMPRESSION_TYPE);

    // TEST6: Change compression and add index
    fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    config = fieldConfigs.remove(0);
    newConfig = new FieldConfig(config.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        FieldConfig.CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig);
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    indexLoadingConfig.getTextIndexColumns().add(config.getName());
    indexLoadingConfig.getInvertedIndexColumns().add(config.getName());
    fwdIndexHandler = new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(_writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_COMPRESSION_TYPE);
  }

  @Test
  public void testRewriteRawForwardIndex()
      throws Exception {
    List<String> allCompressionTypes = Arrays.asList("PASS_THROUGH", "ZSTANDARD", "LZ4", "SNAPPY");

    for (int i = 0; i < _noDictionaryColumns.size(); i++) {

      // For every noDictionaryColumn, change the compressionType to all available types, one by one.
      for (String compressionType : allCompressionTypes) {
        List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
        FieldConfig config = fieldConfigs.remove(i);
        String columnName = config.getName();
        FieldConfig.CompressionCodec newCompressionType = FieldConfig.CompressionCodec.valueOf(compressionType);
        if (config.getCompressionCodec().equals(newCompressionType)) {
          // Compression types match. No need to invoke forward index handler.
          continue;
        }

        FieldConfig newConfig =
            new FieldConfig(columnName, FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType,
                null);
        fieldConfigs.add(newConfig);

        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .setNoDictionaryColumns(_noDictionaryColumns).setFieldConfigList(fieldConfigs).build();
        tableConfig.setFieldConfigList(fieldConfigs);

        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
        IndexCreatorProvider indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
        ForwardIndexHandler fwdIndexHandler =
            new ForwardIndexHandler(_existingSegmentMetadata, indexLoadingConfig);
        fwdIndexHandler.updateIndices(_writer, indexCreatorProvider);

        ColumnMetadata columnMetadata = _existingSegmentMetadata.getColumnMetadataFor(columnName);

        validateForwardIndex(columnMetadata, newCompressionType);
      }
    }
  }

  private void validateForwardIndex(ColumnMetadata columnMetadata, FieldConfig.CompressionCodec expectedCompressionType)
      throws IOException {
    String columnName = columnMetadata.getColumnName();

    // Check Compression type in header
    ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(_writer, columnMetadata);
    ChunkCompressionType fwdIndexCompressionType = fwdIndexReader.getCompressionType();
    assertEquals(fwdIndexCompressionType.name(), expectedCompressionType.name());

    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(_writer, columnMetadata)) {
      PinotSegmentColumnReader columnReader =
          new PinotSegmentColumnReader(forwardIndexReader, null, null, columnMetadata.getMaxNumberOfMultiValues());

      for (int rowIdx = 0; rowIdx < columnMetadata.getTotalDocs(); rowIdx++) {
        if (rowIdx % 10 == 0) {
          Object val = columnReader.getValue(rowIdx);

          FieldSpec.DataType dataType = forwardIndexReader.getStoredType();
          if (dataType == FieldSpec.DataType.STRING) {
            assertEquals((String) val, "testRow");
          } else if (dataType == FieldSpec.DataType.INT) {
            assertEquals((int) val, 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
          } else if (dataType == FieldSpec.DataType.LONG) {
            assertEquals((long) val, 1001L, columnName + " " + rowIdx + " " + expectedCompressionType);
          }
        }
      }
    }
  }
}
