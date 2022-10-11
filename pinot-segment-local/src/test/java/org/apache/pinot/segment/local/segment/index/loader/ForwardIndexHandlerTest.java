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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
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

  // TODO:
  // 1. Add other datatypes (double, float, bigdecimal, bytes). Also add MV columns.
  // 2. Add text index and other index types for raw columns.
  private static final String DIM_SNAPPY_STRING = "DIM_SNAPPY_STRING";
  private static final String DIM_PASS_THROUGH_STRING = "DIM_PASS_THROUGH_STRING";
  private static final String DIM_ZSTANDARD_STRING = "DIM_ZSTANDARD_STRING";
  private static final String DIM_LZ4_STRING = "DIM_LZ4_STRING";

  private static final String DIM_SNAPPY_LONG = "DIM_SNAPPY_LONG";
  private static final String DIM_PASS_THROUGH_LONG = "DIM_PASS_THROUGH_LONG";
  private static final String DIM_ZSTANDARD_LONG = "DIM_ZSTANDARD_LONG";
  private static final String DIM_LZ4_LONG = "DIM_LZ4_LONG";

  private static final String DIM_SNAPPY_INTEGER = "DIM_SNAPPY_INTEGER";
  private static final String DIM_PASS_THROUGH_INTEGER = "DIM_PASS_THROUGH_INTEGER";
  private static final String DIM_ZSTANDARD_INTEGER = "DIM_ZSTANDARD_INTEGER";
  private static final String DIM_LZ4_INTEGER = "DIM_LZ4_INTEGER";

  private static final String DIM_DICT_INTEGER = "DIM_DICT_INTEGER";
  private static final String DIM_DICT_STRING = "DIM_DICT_STRING";
  private static final String DIM_DICT_LONG = "DIM_DICT_LONG";

  private static final String METRIC_PASSTHROUGH_INTEGER = "METRIC_PASSTHROUGH_INTEGER";
  private static final String METRIC_SNAPPY_INTEGER = "METRIC_SNAPPY_INTEGER";
  private static final String METRIC_ZSTANDARD_INTEGER = "METRIC_ZSTANDARD_INTEGER";
  private static final String METRIC_LZ4_INTEGER = "METRIC_LZ4_INTEGER";

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(DIM_SNAPPY_STRING, DIM_SNAPPY_LONG, DIM_SNAPPY_INTEGER, METRIC_SNAPPY_INTEGER);

  private static final List<String> RAW_ZSTANDARD_INDEX_COLUMNS =
      Arrays.asList(DIM_ZSTANDARD_STRING, DIM_ZSTANDARD_LONG, DIM_ZSTANDARD_INTEGER, METRIC_ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_INDEX_COLUMNS =
      Arrays.asList(DIM_PASS_THROUGH_STRING, DIM_PASS_THROUGH_LONG, DIM_PASS_THROUGH_INTEGER,
          METRIC_PASSTHROUGH_INTEGER);

  private static final List<String> RAW_LZ4_INDEX_COLUMNS =
      Arrays.asList(DIM_LZ4_STRING, DIM_LZ4_LONG, DIM_LZ4_INTEGER, METRIC_LZ4_INTEGER);

  private final List<String> _noDictionaryColumns = new ArrayList<>();
  TableConfig _tableConfig;
  Schema _schema;
  File _segmentDirectory;
  private List<FieldConfig.CompressionCodec> _allCompressionTypes =
      Arrays.asList(FieldConfig.CompressionCodec.values());

  @BeforeClass
  public void setUp()
      throws Exception {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData();

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
        .addSingleValueDimension(DIM_SNAPPY_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_ZSTANDARD_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_LZ4_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_SNAPPY_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_ZSTANDARD_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_LZ4_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_SNAPPY_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_ZSTANDARD_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_PASS_THROUGH_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_LZ4_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_DICT_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_DICT_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_DICT_STRING, FieldSpec.DataType.STRING)
        .addMetric(METRIC_PASSTHROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_SNAPPY_INTEGER, FieldSpec.DataType.INT).addMetric(METRIC_LZ4_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_ZSTANDARD_INTEGER, FieldSpec.DataType.INT)

        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
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
      row.putValue(DIM_SNAPPY_STRING, tempStringRows[i]);
      row.putValue(DIM_ZSTANDARD_STRING, tempStringRows[i]);
      row.putValue(DIM_PASS_THROUGH_STRING, tempStringRows[i]);
      row.putValue(DIM_LZ4_STRING, tempStringRows[i]);

      // Raw integer columns
      row.putValue(DIM_SNAPPY_INTEGER, tempIntRows[i]);
      row.putValue(DIM_ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(DIM_PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(DIM_LZ4_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_LZ4_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_PASSTHROUGH_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_SNAPPY_INTEGER, tempIntRows[i]);

      // Raw long columns
      row.putValue(DIM_SNAPPY_LONG, tempLongRows[i]);
      row.putValue(DIM_ZSTANDARD_LONG, tempLongRows[i]);
      row.putValue(DIM_PASS_THROUGH_LONG, tempLongRows[i]);
      row.putValue(DIM_LZ4_LONG, tempLongRows[i]);

      // Dictionary columns
      row.putValue(DIM_DICT_INTEGER, tempIntRows[i]);
      row.putValue(DIM_DICT_LONG, tempLongRows[i]);
      row.putValue(DIM_DICT_STRING, tempStringRows[i]);

      rows.add(row);
    }
    return rows;
  }

  @Test
  public void testComputeOperation()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1 : Validate with zero changes. ForwardIndexHandler should be a No-Op.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    Map<String, ForwardIndexHandler.Operation> operationMap = new HashMap<>();
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST2: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN. ForwardIndexHandler should be a No-Op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().remove(DIM_ZSTANDARD_STRING);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST3: Disable dictionary. ForwardIndexHandler should be a No-Op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().add(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST4: Add random index. ForwardIndexHandler should be a No-Op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getTextIndexColumns().add(DIM_DICT_INTEGER);
    indexLoadingConfig.getTextIndexColumns().add(DIM_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(writer);
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
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);

    // TEST6: Change compression and add index. Change compressionType for more than 1 column.
    fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    FieldConfig config1 = fieldConfigs.remove(0);
    FieldConfig config2 = fieldConfigs.remove(1);

    FieldConfig newConfig1 = new FieldConfig(config1.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        FieldConfig.CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig1);
    FieldConfig newConfig2 = new FieldConfig(config2.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        FieldConfig.CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig2);

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    indexLoadingConfig.getTextIndexColumns().add(config1.getName());
    indexLoadingConfig.getInvertedIndexColumns().add(config1.getName());
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(config1.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);
    assertEquals(operationMap.get(config2.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testRewriteRawForwardIndexForSingleColumn()
      throws Exception {
    for (int i = 0; i < _noDictionaryColumns.size(); i++) {

      // For every noDictionaryColumn, change the compressionType to all available types, one by one.
      for (FieldConfig.CompressionCodec compressionType : _allCompressionTypes) {
        // Setup
        SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
        SegmentDirectory segmentLocalFSDirectory =
            new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

        List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
        FieldConfig config = fieldConfigs.remove(i);
        String columnName = config.getName();
        FieldConfig.CompressionCodec newCompressionType = compressionType;

        FieldConfig newConfig =
            new FieldConfig(columnName, FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType,
                null);
        fieldConfigs.add(newConfig);

        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .setNoDictionaryColumns(_noDictionaryColumns).setFieldConfigList(fieldConfigs).build();
        tableConfig.setFieldConfigList(fieldConfigs);

        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
        IndexCreatorProvider indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
        ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);

        fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
        segmentLocalFSDirectory.close();

        // Validation
        validateIndexMap();
        validateForwardIndex(columnName, newCompressionType);
      }
    }
  }

  @Test
  public void testRewriteRawForwardIndexForMultipleColumns()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    Random rand = new Random();
    int randomIdx = rand.nextInt(_allCompressionTypes.size());
    FieldConfig.CompressionCodec newCompressionType = _allCompressionTypes.get(randomIdx);

    // Column 1
    randomIdx = rand.nextInt(fieldConfigs.size());
    FieldConfig config1 = fieldConfigs.remove(randomIdx);
    String column1 = config1.getName();
    FieldConfig newConfig1 =
        new FieldConfig(column1, FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType, null);
    fieldConfigs.add(newConfig1);

    // Column 2
    randomIdx = rand.nextInt(fieldConfigs.size());
    FieldConfig config2 = fieldConfigs.remove(randomIdx);
    String column2 = config2.getName();
    FieldConfig newConfig2 =
        new FieldConfig(column2, FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType, null);
    fieldConfigs.add(newConfig2);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    IndexCreatorProvider indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig);
    fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    validateIndexMap();
    validateForwardIndex(column1, newCompressionType);
    validateForwardIndex(column2, newCompressionType);
  }

  private void validateForwardIndex(String columnName, FieldConfig.CompressionCodec expectedCompressionType)
      throws IOException {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    ColumnMetadata columnMetadata = existingSegmentMetadata.getColumnMetadataFor(columnName);

    // Check Compression type in header
    ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(writer, columnMetadata);
    ChunkCompressionType fwdIndexCompressionType = fwdIndexReader.getCompressionType();
    assertEquals(fwdIndexCompressionType.name(), expectedCompressionType.name());

    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(writer, columnMetadata)) {
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

  private void validateIndexMap()
      throws IOException {
    // Panic validation to make sure all columns have only one forward index entry in index map.
    for (String columnName : _noDictionaryColumns) {
      String segmentDir = INDEX_DIR + "/" + SEGMENT_NAME + "/v3";
      File idxMapFile = new File(segmentDir, V1Constants.INDEX_MAP_FILE_NAME);
      String indexMapStr = FileUtils.readFileToString(idxMapFile, StandardCharsets.UTF_8);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".startOffset"), 1);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".size"), 1);
    }
  }
}
