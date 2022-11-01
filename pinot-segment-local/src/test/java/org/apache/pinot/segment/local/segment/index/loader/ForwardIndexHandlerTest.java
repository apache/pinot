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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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
import static org.testng.Assert.assertTrue;


public class ForwardIndexHandlerTest {
  private static final BigDecimal BASE_BIG_DECIMAL = BigDecimal.valueOf(new Random().nextDouble());
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ForwardIndexHandlerTest");
  private static final String TABLE_NAME = "myTable";
  private static final String SEGMENT_NAME = "testSegment";

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

  private static final String DIM_SNAPPY_BYTES = "DIM_SNAPPY_BYTES";
  private static final String DIM_PASS_THROUGH_BYTES = "DIM_PASS_THROUGH_BYTES";
  private static final String DIM_ZSTANDARD_BYTES = "DIM_ZSTANDARD_BYTES";
  private static final String DIM_LZ4_BYTES = "DIM_LZ4_BYTES";

  // Sorted column
  private static final String DIM_PASS_THROUGH_SORTED_LONG = "DIM_PASS_THROUGH_SORTED_LONG";

  // Dictionary columns
  private static final String DIM_DICT_INTEGER = "DIM_DICT_INTEGER";
  private static final String DIM_DICT_STRING = "DIM_DICT_STRING";
  private static final String DIM_DICT_LONG = "DIM_DICT_LONG";

  // Metric columns
  private static final String METRIC_PASS_THROUGH_INTEGER = "METRIC_PASS_THROUGH_INTEGER";
  private static final String METRIC_SNAPPY_INTEGER = "METRIC_SNAPPY_INTEGER";
  private static final String METRIC_ZSTANDARD_INTEGER = "METRIC_ZSTANDARD_INTEGER";
  private static final String METRIC_LZ4_INTEGER = "METRIC_LZ4_INTEGER";

  private static final String METRIC_SNAPPY_BIG_DECIMAL = "METRIC_SNAPPY_BIG_DECIMAL";
  private static final String METRIC_PASS_THROUGH_BIG_DECIMAL = "METRIC_PASS_THROUGH_BIG_DECIMAL";
  private static final String METRIC_ZSTANDARD_BIG_DECIMAL = "METRIC_ZSTANDARD_BIG_DECIMAL";
  private static final String METRIC_LZ4_BIG_DECIMAL = "METRIC_LZ4_BIG_DECIMAL";

  // Multivalue columns
  private static final String DIM_MV_PASS_THROUGH_INTEGER = "DIM_MV_PASS_THROUGH_INTEGER";
  private static final String DIM_MV_PASS_THROUGH_LONG = "DIM_MV_PASS_THROUGH_LONG";
  private static final String DIM_MV_PASS_THROUGH_STRING = "DIM_MV_PASS_THROUGH_STRING";
  private static final String DIM_MV_PASS_THROUGH_BYTES = "DIM_MV_PASS_THROUGH_BYTES";

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(DIM_SNAPPY_STRING, DIM_SNAPPY_LONG, DIM_SNAPPY_INTEGER, DIM_SNAPPY_BYTES, METRIC_SNAPPY_BIG_DECIMAL,
          METRIC_SNAPPY_INTEGER);

  private static final List<String> RAW_ZSTANDARD_INDEX_COLUMNS =
      Arrays.asList(DIM_ZSTANDARD_STRING, DIM_ZSTANDARD_LONG, DIM_ZSTANDARD_INTEGER, DIM_ZSTANDARD_BYTES,
          METRIC_ZSTANDARD_BIG_DECIMAL, METRIC_ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_INDEX_COLUMNS =
      Arrays.asList(DIM_PASS_THROUGH_STRING, DIM_PASS_THROUGH_LONG, DIM_PASS_THROUGH_INTEGER, DIM_PASS_THROUGH_BYTES,
          METRIC_PASS_THROUGH_BIG_DECIMAL, METRIC_PASS_THROUGH_INTEGER, DIM_MV_PASS_THROUGH_INTEGER,
          DIM_MV_PASS_THROUGH_LONG, DIM_MV_PASS_THROUGH_STRING, DIM_MV_PASS_THROUGH_BYTES,
          DIM_PASS_THROUGH_SORTED_LONG);

  private static final List<String> RAW_LZ4_INDEX_COLUMNS =
      Arrays.asList(DIM_LZ4_STRING, DIM_LZ4_LONG, DIM_LZ4_INTEGER, DIM_LZ4_BYTES, METRIC_LZ4_BIG_DECIMAL,
          METRIC_LZ4_INTEGER);

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
        .addSingleValueDimension(DIM_SNAPPY_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(DIM_PASS_THROUGH_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(DIM_ZSTANDARD_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(DIM_LZ4_BYTES, FieldSpec.DataType.BYTES)
        .addMetric(METRIC_SNAPPY_BIG_DECIMAL, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(METRIC_PASS_THROUGH_BIG_DECIMAL, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(METRIC_ZSTANDARD_BIG_DECIMAL, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(METRIC_LZ4_BIG_DECIMAL, FieldSpec.DataType.BIG_DECIMAL)
        .addSingleValueDimension(DIM_DICT_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_DICT_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_DICT_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_PASS_THROUGH_SORTED_LONG, FieldSpec.DataType.LONG)
        .addMetric(METRIC_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_SNAPPY_INTEGER, FieldSpec.DataType.INT).addMetric(METRIC_LZ4_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_ZSTANDARD_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_LONG, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_BYTES, FieldSpec.DataType.BYTES).build();

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
    byte[][] tempBytesRows = new byte[rowLength][];
    BigDecimal[] tempBigDecimalRows = new BigDecimal[rowLength];

    int maxNumberOfMVEntries = random.nextInt(500);
    String[][] tempMVStringRows = new String[rowLength][maxNumberOfMVEntries];
    Integer[][] tempMVIntRows = new Integer[rowLength][maxNumberOfMVEntries];
    Long[][] tempMVLongRows = new Long[rowLength][maxNumberOfMVEntries];
    byte[][][] tempMVByteRows = new byte[rowLength][maxNumberOfMVEntries][];

    for (int i = 0; i < rowLength; i++) {
      //Adding a fixed value to check for filter queries
      if (i % 10 == 0) {
        String str = "testRow";
        tempStringRows[i] = str;
        tempIntRows[i] = 1001;
        tempLongRows[i] = 1001L;
        tempBytesRows[i] = str.getBytes();
        tempBigDecimalRows[i] = BASE_BIG_DECIMAL.add(BigDecimal.valueOf(1001));

        // Avoid creating empty arrays.
        int numMVElements = random.nextInt(maxNumberOfMVEntries - 1) + 1;
        for (int j = 0; j < numMVElements; j++) {
          tempMVIntRows[i][j] = 1001;
          tempMVLongRows[i][j] = 1001L;
          tempMVStringRows[i][j] = str;
          tempMVByteRows[i][j] = (byte[]) str.getBytes();
        }
      } else {
        String str = "n" + i;
        tempStringRows[i] = str;
        tempIntRows[i] = i;
        tempLongRows[i] = (long) i;
        tempBytesRows[i] = (byte[]) str.getBytes();
        tempBigDecimalRows[i] = BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i));

        // Avoid creating empty arrays.
        int numMVElements = random.nextInt(maxNumberOfMVEntries - 1) + 1;
        for (int j = 0; j < numMVElements; j++) {
          tempMVIntRows[i][j] = j;
          tempMVLongRows[i][j] = (long) j;
          tempMVStringRows[i][j] = str;
          tempMVByteRows[i][j] = (byte[]) str.getBytes();
        }
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
      row.putValue(METRIC_PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_SNAPPY_INTEGER, tempIntRows[i]);

      // Raw long columns
      row.putValue(DIM_SNAPPY_LONG, tempLongRows[i]);
      row.putValue(DIM_ZSTANDARD_LONG, tempLongRows[i]);
      row.putValue(DIM_PASS_THROUGH_LONG, tempLongRows[i]);
      row.putValue(DIM_LZ4_LONG, tempLongRows[i]);

      // Raw Byte columns
      row.putValue(DIM_SNAPPY_BYTES, tempBytesRows[i]);
      row.putValue(DIM_ZSTANDARD_BYTES, tempBytesRows[i]);
      row.putValue(DIM_PASS_THROUGH_BYTES, tempBytesRows[i]);
      row.putValue(DIM_LZ4_BYTES, tempBytesRows[i]);

      // Raw BigDecimal column
      row.putValue(METRIC_SNAPPY_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_ZSTANDARD_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_PASS_THROUGH_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_LZ4_BIG_DECIMAL, tempBigDecimalRows[i]);

      // Dictionary columns
      row.putValue(DIM_DICT_INTEGER, tempIntRows[i]);
      row.putValue(DIM_DICT_LONG, tempLongRows[i]);
      row.putValue(DIM_DICT_STRING, tempStringRows[i]);

      // MV columns
      row.putValue(DIM_MV_PASS_THROUGH_INTEGER, tempMVIntRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_LONG, tempMVLongRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_STRING, tempMVStringRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_BYTES, tempMVByteRows[i]);

      // Sorted columns
      row.putValue(DIM_PASS_THROUGH_SORTED_LONG, (long) i);

      rows.add(row);
    }
    return rows;
  }

  @Test(priority = 0)
  public void testComputeOperation()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1 : Validate with zero changes. ForwardIndexHandler should be a No-Op.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, null);
    Map<String, ForwardIndexHandler.Operation> operationMap = new HashMap<>();
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST2: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().remove(DIM_ZSTANDARD_STRING);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.get(DIM_ZSTANDARD_STRING), ForwardIndexHandler.Operation.ENABLE_DICTIONARY);

    // TEST3: Enable dictionary for an MV column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().remove(DIM_MV_PASS_THROUGH_STRING);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.get(DIM_MV_PASS_THROUGH_STRING), ForwardIndexHandler.Operation.ENABLE_DICTIONARY);

    // TEST4: Enable dictionary for a sorted column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().remove(DIM_PASS_THROUGH_SORTED_LONG);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.get(DIM_PASS_THROUGH_SORTED_LONG), ForwardIndexHandler.Operation.ENABLE_DICTIONARY);

    // TEST5: Enable dictionary for a dict column. Should be a No-op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST6: Disable dictionary. Should be a No-op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getNoDictionaryColumns().add(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST7: Add an additional text index. ForwardIndexHandler should be a No-Op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getTextIndexColumns().add(DIM_DICT_INTEGER);
    indexLoadingConfig.getTextIndexColumns().add(DIM_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST8: Add text index and disable forward index.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.getRangeIndexColumns().add(METRIC_LZ4_INTEGER);
    indexLoadingConfig.getNoDictionaryColumns().remove(METRIC_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.get(METRIC_LZ4_INTEGER), ForwardIndexHandler.Operation.ENABLE_DICTIONARY);

    // TEST8: Change compression
    Random rand = new Random();

    // Create new tableConfig with the modified fieldConfigs.
    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    int randIdx = rand.nextInt(fieldConfigs.size());
    FieldConfig config = fieldConfigs.remove(randIdx);
    FieldConfig.CompressionCodec newCompressionType = null;
    for (FieldConfig.CompressionCodec type : _allCompressionTypes) {
      if (config.getCompressionCodec() != type) {
        newCompressionType = type;
      }
    }
    FieldConfig newConfig =
        new FieldConfig(config.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType,
            null);
    fieldConfigs.add(newConfig);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);

    // TEST9: Change compression and add index. Change compressionType for more than 1 column.
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
    fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(config1.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);
    assertEquals(operationMap.get(config2.getName()), ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test(priority = 1)
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
        ForwardIndexHandler fwdIndexHandler =
            new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, null);
        boolean val = fwdIndexHandler.needUpdateIndices(writer);
        fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
        segmentLocalFSDirectory.close();

        // Validation
        validateIndexMap(columnName, false);
        validateForwardIndex(columnName, newCompressionType);

        // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
        // change.
        ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(columnName);
        validateMetadataProperties(columnName, metadata.hasDictionary(), metadata.getColumnMaxLength(),
            metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
            metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
            metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
            metadata.getMaxValue());
      }
    }
  }

  @Test(priority = 2)
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
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, null);
    fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    validateIndexMap(column1, false);
    validateForwardIndex(column1, newCompressionType);
    // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
    // change.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    validateMetadataProperties(column1, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue());

    validateIndexMap(column2, false);
    validateForwardIndex(column2, newCompressionType);
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    validateMetadataProperties(column2, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue());
  }

  @Test(priority = 3)
  public void testEnableDictionaryForMultipleColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    String col1 = _noDictionaryColumns.get(rand.nextInt(_noDictionaryColumns.size()));
    indexLoadingConfig.getNoDictionaryColumns().remove(col1);
    String col2 = _noDictionaryColumns.get(rand.nextInt(_noDictionaryColumns.size()));
    indexLoadingConfig.getNoDictionaryColumns().remove(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
    IndexCreatorProvider indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
    fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    validateIndexMap(col1, true);
    validateForwardIndex(col1, null);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    int dictionaryElementSize = 0;
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    FieldSpec.DataType dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 11;
    }
    validateMetadataProperties(col1, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue());

    // Col2 validation.
    validateIndexMap(col2, true);
    validateForwardIndex(col2, null);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    dictionaryElementSize = 0;
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 11;
    }
    validateMetadataProperties(col2, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue());
  }

  @Test(priority = 4)
  public void testEnableDictionaryForSingleColumn()
      throws Exception {
    for (int i = 0; i < _noDictionaryColumns.size(); i++) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      String column = _noDictionaryColumns.get(i);
      indexLoadingConfig.getNoDictionaryColumns().remove(column);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(existingSegmentMetadata, indexLoadingConfig, _schema);
      IndexCreatorProvider indexCreatorProvider = IndexingOverrides.getIndexCreatorProvider();
      fwdIndexHandler.updateIndices(writer, indexCreatorProvider);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      validateIndexMap(column, true);
      validateForwardIndex(column, null);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      FieldSpec.DataType dataType = metadata.getDataType();
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
        dictionaryElementSize = 11;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue());
    }
  }

  private void validateForwardIndex(String columnName, @Nullable FieldConfig.CompressionCodec expectedCompressionType)
      throws IOException {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    ColumnMetadata columnMetadata = existingSegmentMetadata.getColumnMetadataFor(columnName);
    boolean isSingleValue = columnMetadata.isSingleValue();
    boolean isSorted = columnMetadata.isSorted();

    // Check Compression type in header
    ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(writer, columnMetadata);
    ChunkCompressionType fwdIndexCompressionType = fwdIndexReader.getCompressionType();
    if (expectedCompressionType != null) {
      assertEquals(fwdIndexCompressionType.name(), expectedCompressionType.name());
    }

    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(writer, columnMetadata)) {
      Dictionary dictionary = null;
      if (columnMetadata.hasDictionary()) {
        dictionary = LoaderUtils.getDictionary(writer, columnMetadata);
      }
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(forwardIndexReader, dictionary, null,
          columnMetadata.getMaxNumberOfMultiValues());

      for (int rowIdx = 0; rowIdx < columnMetadata.getTotalDocs(); rowIdx++) {
        if (rowIdx % 10 == 0) {
          Object val = columnReader.getValue(rowIdx);
          FieldSpec.DataType dataType = columnMetadata.getDataType();

          switch (dataType) {
            case STRING: {
              if (isSingleValue) {
                assertEquals((String) val, "testRow");
              } else {
                Object[] values = (Object[]) val;
                int length = values.length;
                for (int i = 0; i < length; i++) {
                  assertEquals((String) values[i], "testRow");
                }
              }
              break;
            }
            case INT: {
              if (isSingleValue) {
                assertEquals((int) val, 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
              } else {
                Object[] values = (Object[]) val;
                int length = values.length;
                for (int i = 0; i < length; i++) {
                  assertEquals((int) values[i], 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
                }
              }
              break;
            }
            case LONG: {
              if (isSingleValue) {
                if (isSorted) {
                  assertEquals((long) val, rowIdx, columnName + " " + rowIdx + " " + expectedCompressionType);
                } else {
                  assertEquals((long) val, 1001L, columnName + " " + rowIdx + " " + expectedCompressionType);
                }
              } else {
                Object[] values = (Object[]) val;
                int length = values.length;
                for (int i = 0; i < length; i++) {
                  assertEquals((long) values[i], 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
                }
              }
              break;
            }
            case BYTES: {
              byte[] expectedVal = "testRow".getBytes();
              if (isSingleValue) {
                assertEquals((byte[]) val, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
              } else {
                Object[] values = (Object[]) val;
                int length = values.length;
                for (int i = 0; i < length; i++) {
                  assertEquals((byte[]) values[i], expectedVal,
                      columnName + " " + rowIdx + " " + expectedCompressionType);
                }
              }
              break;
            }
            case BIG_DECIMAL: {
              assertTrue(isSingleValue);
              assertEquals((BigDecimal) val, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(1001)));
              break;
            }
            default:
              // Unreachable code.
              throw new IllegalStateException("Invalid datatype for column=" + columnName);
          }
        }
      }
    }
  }

  private void validateIndexMap(String columnName, boolean dictionaryEnabled)
      throws IOException {
    // Panic validation to make sure all columns have only one forward index entry in index map.
    String segmentDir = INDEX_DIR + "/" + SEGMENT_NAME + "/v3";
    File idxMapFile = new File(segmentDir, V1Constants.INDEX_MAP_FILE_NAME);
    String indexMapStr = FileUtils.readFileToString(idxMapFile, StandardCharsets.UTF_8);
    assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".startOffset"), 1, columnName);
    assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".size"), 1, columnName);

    if (dictionaryEnabled) {
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".startOffset"), 1, columnName);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".size"), 1, columnName);
    }
  }

  private void validateMetadataProperties(String column, boolean hasDictionary, int dictionaryElementSize,
      int cardinality, int totalDocs, FieldSpec.DataType dataType, FieldSpec.FieldType fieldType, boolean isSorted,
      boolean isSingleValue, int maxNumberOfMVEntries, int totalNumberOfEntries, boolean isAutoGenerated,
      Comparable minValue, Comparable maxValue)
      throws IOException {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

    assertEquals(columnMetadata.hasDictionary(), hasDictionary, column);
    assertEquals(columnMetadata.getColumnMaxLength(), dictionaryElementSize, column);
    assertEquals(columnMetadata.getCardinality(), cardinality, column);
    assertEquals(columnMetadata.getTotalDocs(), totalDocs, column);
    assertTrue(columnMetadata.getDataType().equals(dataType), column);
    assertTrue(columnMetadata.getFieldType().equals(fieldType));
    assertEquals(columnMetadata.isSorted(), isSorted);
    assertEquals(columnMetadata.isSingleValue(), isSingleValue);
    assertEquals(columnMetadata.getMaxNumberOfMultiValues(), maxNumberOfMVEntries);
    assertEquals(columnMetadata.getTotalNumberOfEntries(), totalNumberOfEntries);
    assertEquals(columnMetadata.isAutoGenerated(), isAutoGenerated);
    assertEquals(columnMetadata.getMinValue(), minValue);
    assertEquals(columnMetadata.getMaxValue(), maxValue);
  }
}
