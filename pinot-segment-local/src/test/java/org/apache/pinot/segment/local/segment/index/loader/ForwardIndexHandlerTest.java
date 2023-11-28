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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ForwardIndexHandlerTest {
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

  // Sorted columns
  private static final String DIM_RAW_SORTED_INTEGER = "DIM_RAW_SORTED_INTEGER";

  // Metric columns
  private static final String METRIC_PASS_THROUGH_INTEGER = "METRIC_PASS_THROUGH_INTEGER";
  private static final String METRIC_SNAPPY_INTEGER = "METRIC_SNAPPY_INTEGER";
  private static final String METRIC_ZSTANDARD_INTEGER = "METRIC_ZSTANDARD_INTEGER";
  private static final String METRIC_LZ4_INTEGER = "METRIC_LZ4_INTEGER";

  private static final String METRIC_SNAPPY_BIG_DECIMAL = "METRIC_SNAPPY_BIG_DECIMAL";
  private static final String METRIC_PASS_THROUGH_BIG_DECIMAL = "METRIC_PASS_THROUGH_BIG_DECIMAL";
  private static final String METRIC_ZSTANDARD_BIG_DECIMAL = "METRIC_ZSTANDARD_BIG_DECIMAL";
  private static final String METRIC_LZ4_BIG_DECIMAL = "METRIC_LZ4_BIG_DECIMAL";

  // Multi-value columns
  private static final String DIM_MV_PASS_THROUGH_INTEGER = "DIM_MV_PASS_THROUGH_INTEGER";
  private static final String DIM_MV_PASS_THROUGH_LONG = "DIM_MV_PASS_THROUGH_LONG";
  private static final String DIM_MV_PASS_THROUGH_STRING = "DIM_MV_PASS_THROUGH_STRING";
  private static final String DIM_MV_PASS_THROUGH_BYTES = "DIM_MV_PASS_THROUGH_BYTES";

  // Dictionary columns
  private static final String DIM_DICT_INTEGER = "DIM_DICT_INTEGER";
  private static final String DIM_DICT_STRING = "DIM_DICT_STRING";
  private static final String DIM_DICT_LONG = "DIM_DICT_LONG";
  private static final String DIM_DICT_BYES = "DIM_DICT_BYTES";
  private static final String DIM_DICT_MV_INTEGER = "DIM_DICT_MV_INTEGER";
  private static final String DIM_DICT_MV_LONG = "DIM_DICT_MV_LONG";
  private static final String DIM_DICT_MV_STRING = "DIM_DICT_MV_STRING";
  private static final String DIM_DICT_MV_BYTES = "DIM_DICT_MV_BYTES";

  // Forward index disabled single-value columns
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_INTEGER = "DIM_SV_FORWARD_INDEX_DISABLED_INTEGER";
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_LONG = "DIM_SV_FORWARD_INDEX_DISABLED_LONG";
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_STRING = "DIM_SV_FORWARD_INDEX_DISABLED_STRING";
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_BYTES = "DIM_SV_FORWARD_INDEX_DISABLED_BYTES";

  // Forward index disabled multi-value columns
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_INTEGER = "DIM_MV_FORWARD_INDEX_DISABLED_INTEGER";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_LONG = "DIM_MV_FORWARD_INDEX_DISABLED_LONG";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_STRING = "DIM_MV_FORWARD_INDEX_DISABLED_STRING";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_BYTES = "DIM_MV_FORWARD_INDEX_DISABLED_BYTES";

  // Forward index disabled multi-value columns with duplicates
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER =
      "DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG =
      "DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING =
      "DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING";
  private static final String DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES =
      "DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES";

  // Forward index disabled raw single-value column
  private static final String DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER = "DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER";

  // Forward index disabled raw multi-value column
  private static final String DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER = "DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER";

  // Forward index disabled dictionary enabled but inverted index disabled single-value column
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX =
      "DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX";

  // Dictionary based forward index disabled column with a range index
  private static final String DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX =
      "DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX";

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(DIM_SNAPPY_STRING, DIM_SNAPPY_LONG, DIM_SNAPPY_INTEGER, DIM_SNAPPY_BYTES, METRIC_SNAPPY_BIG_DECIMAL,
          METRIC_SNAPPY_INTEGER);

  private static final List<String> RAW_SORTED_INDEX_COLUMNS = Arrays.asList(DIM_RAW_SORTED_INTEGER);

  private static final List<String> RAW_ZSTANDARD_INDEX_COLUMNS =
      Arrays.asList(DIM_ZSTANDARD_STRING, DIM_ZSTANDARD_LONG, DIM_ZSTANDARD_INTEGER, DIM_ZSTANDARD_BYTES,
          METRIC_ZSTANDARD_BIG_DECIMAL, METRIC_ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_INDEX_COLUMNS =
      Arrays.asList(DIM_PASS_THROUGH_STRING, DIM_PASS_THROUGH_LONG, DIM_PASS_THROUGH_INTEGER, DIM_PASS_THROUGH_BYTES,
          METRIC_PASS_THROUGH_BIG_DECIMAL, METRIC_PASS_THROUGH_INTEGER, DIM_MV_PASS_THROUGH_INTEGER,
          DIM_MV_PASS_THROUGH_LONG, DIM_MV_PASS_THROUGH_STRING, DIM_MV_PASS_THROUGH_BYTES);

  private static final List<String> RAW_LZ4_INDEX_COLUMNS =
      Arrays.asList(DIM_LZ4_STRING, DIM_LZ4_LONG, DIM_LZ4_INTEGER, DIM_LZ4_BYTES, METRIC_LZ4_BIG_DECIMAL,
          METRIC_LZ4_INTEGER);

  private static final List<String> DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX = Arrays.asList(DIM_DICT_INTEGER,
      DIM_DICT_LONG, DIM_DICT_STRING, DIM_DICT_BYES, DIM_DICT_MV_BYTES, DIM_DICT_MV_STRING,
      DIM_DICT_MV_INTEGER, DIM_DICT_MV_LONG);

  private static final List<String> DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX =
      Arrays.asList(DIM_DICT_MV_INTEGER, DIM_DICT_MV_LONG, DIM_DICT_MV_STRING, DIM_DICT_MV_BYTES);

  private static final List<String> SV_FORWARD_INDEX_DISABLED_COLUMNS = Arrays.asList(
      DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_SV_FORWARD_INDEX_DISABLED_LONG, DIM_SV_FORWARD_INDEX_DISABLED_STRING,
      DIM_SV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_COLUMNS =
      Arrays.asList(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_STRING, DIM_MV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS =
      Arrays.asList(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES);

  private static final List<String> FORWARD_INDEX_DISABLED_RAW_COLUMNS =
      Arrays.asList(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);

  private static final List<CompressionCodec> RAW_COMPRESSION_TYPES =
      Arrays.stream(CompressionCodec.values()).filter(CompressionCodec::isApplicableToRawIndex)
          .collect(Collectors.toList());

  private final List<String> _noDictionaryColumns = new ArrayList<>();
  private final List<String> _forwardIndexDisabledColumns = new ArrayList<>();
  private final List<String> _invertedIndexColumns = new ArrayList<>();
  TableConfig _tableConfig;
  Schema _schema;
  File _segmentDirectory;

  @BeforeMethod
  public void setUp()
      throws Exception {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
  }

  @AfterMethod
  public void tearDown() {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = createTestData();

    List<FieldConfig> fieldConfigs = new ArrayList<>(
        RAW_SNAPPY_INDEX_COLUMNS.size() + RAW_SORTED_INDEX_COLUMNS.size() + RAW_ZSTANDARD_INDEX_COLUMNS.size()
            + RAW_PASS_THROUGH_INDEX_COLUMNS.size() + RAW_LZ4_INDEX_COLUMNS.size()
            + SV_FORWARD_INDEX_DISABLED_COLUMNS.size() + MV_FORWARD_INDEX_DISABLED_COLUMNS.size()
            + MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size() + FORWARD_INDEX_DISABLED_RAW_COLUMNS.size() + 2);

    for (String indexColumn : RAW_SNAPPY_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          CompressionCodec.SNAPPY, null));
    }

    for (String indexColumn : RAW_SORTED_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW,
          Collections.singletonList(FieldConfig.IndexType.SORTED), CompressionCodec.SNAPPY, null));
    }

    for (String indexColumn : RAW_ZSTANDARD_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          CompressionCodec.ZSTANDARD, null));
    }

    for (String indexColumn : RAW_PASS_THROUGH_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          CompressionCodec.PASS_THROUGH, null));
    }

    for (String indexColumn : RAW_LZ4_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          CompressionCodec.LZ4, null));
    }

    for (String indexColumn : SV_FORWARD_INDEX_DISABLED_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.DICTIONARY, Collections.singletonList(
          FieldConfig.IndexType.INVERTED), null,
          Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));
    }

    for (String indexColumn : MV_FORWARD_INDEX_DISABLED_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.DICTIONARY, Collections.singletonList(
          FieldConfig.IndexType.INVERTED), null,
          Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));
    }

    for (String indexColumn : MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.DICTIONARY, Collections.singletonList(
          FieldConfig.IndexType.INVERTED), null,
          Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));
    }

    for (String indexColumn : FORWARD_INDEX_DISABLED_RAW_COLUMNS) {
      fieldConfigs.add(
          new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(), CompressionCodec.LZ4,
          Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));
    }

    fieldConfigs.add(new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX,
        FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(), null,
        Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));

    fieldConfigs.add(new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX,
        FieldConfig.EncodingType.DICTIONARY, Collections.singletonList(FieldConfig.IndexType.RANGE), null,
        Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));

    _noDictionaryColumns.addAll(RAW_SNAPPY_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_ZSTANDARD_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_PASS_THROUGH_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(RAW_LZ4_INDEX_COLUMNS);
    _noDictionaryColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    _noDictionaryColumns.addAll(RAW_SORTED_INDEX_COLUMNS);

    _forwardIndexDisabledColumns.addAll(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    _forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    _forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    _forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    _forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    _forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);

    _invertedIndexColumns.addAll(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    _invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    _invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);

    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setInvertedIndexColumns(_invertedIndexColumns)
            .setRangeIndexColumns(Collections.singletonList(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX))
            .setFieldConfigList(fieldConfigs).build();
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DIM_SNAPPY_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_ZSTANDARD_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_LZ4_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_SNAPPY_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_RAW_SORTED_INTEGER, FieldSpec.DataType.INT)
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
        .addSingleValueDimension(DIM_DICT_BYES, FieldSpec.DataType.BYTES)
        .addMetric(METRIC_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_SNAPPY_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_LZ4_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_ZSTANDARD_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_LONG, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DIM_MV_PASS_THROUGH_BYTES, FieldSpec.DataType.BYTES)
        .addMultiValueDimension(DIM_DICT_MV_BYTES, FieldSpec.DataType.BYTES)
        .addMultiValueDimension(DIM_DICT_MV_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_DICT_MV_LONG, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DIM_DICT_MV_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_BYTES, FieldSpec.DataType.BYTES)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_LONG, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_STRING, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_BYTES, FieldSpec.DataType.BYTES)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG, FieldSpec.DataType.LONG)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, FieldSpec.DataType.STRING)
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldSpec.DataType.INT)
        .addMultiValueDimension(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX, FieldSpec.DataType.INT)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, _invertedIndexColumns);
    config.setForwardIndexDisabledColumns(_forwardIndexDisabledColumns);
    config.setRangeIndexCreationColumns(
        Collections.singletonList(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX));
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();

    // Generate random data
    int rowLength = 1000;
    Random random = new Random();
    String[] tempStringRows = new String[rowLength];
    Integer[] tempIntRows = new Integer[rowLength];
    Long[] tempLongRows = new Long[rowLength];
    byte[][] tempBytesRows = new byte[rowLength][];
    BigDecimal[] tempBigDecimalRows = new BigDecimal[rowLength];

    int maxNumberOfMVEntries = random.nextInt(500) + 1;
    String[][] tempMVStringRows = new String[rowLength][maxNumberOfMVEntries];
    Integer[][] tempMVIntRows = new Integer[rowLength][maxNumberOfMVEntries];
    Long[][] tempMVLongRows = new Long[rowLength][maxNumberOfMVEntries];
    byte[][][] tempMVByteRows = new byte[rowLength][maxNumberOfMVEntries][];

    // For MV columns today adding duplicate entries within the same row will result in the total number of MV entries
    // reducing for that row since we cannot support rebuilding the forward index without losing duplicates within a
    // row today.
    String[][] tempMVStringRowsForwardIndexDisabled = new String[rowLength][maxNumberOfMVEntries];
    Integer[][] tempMVIntRowsForwardIndexDisabled = new Integer[rowLength][maxNumberOfMVEntries];
    Long[][] tempMVLongRowsForwardIndexDisabled = new Long[rowLength][maxNumberOfMVEntries];
    byte[][][] tempMVByteRowsForwardIndexDisabled = new byte[rowLength][maxNumberOfMVEntries][];

    for (int i = 0; i < rowLength; i++) {
      // Adding a fixed value to check for filter queries
      if (i % 10 == 0) {
        String str = "testRow";
        tempStringRows[i] = str;
        tempIntRows[i] = 1001;
        tempLongRows[i] = 1001L;
        tempBytesRows[i] = str.getBytes();
        tempBigDecimalRows[i] = BigDecimal.valueOf(1001);

        // Avoid creating empty arrays.
        int numMVElements = random.nextInt(maxNumberOfMVEntries) + 1;
        for (int j = 0; j < numMVElements; j++) {
          tempMVIntRows[i][j] = 1001;
          tempMVLongRows[i][j] = 1001L;
          tempMVStringRows[i][j] = str;
          tempMVByteRows[i][j] = str.getBytes();
        }
      } else {
        String str = "n" + i;
        tempStringRows[i] = str;
        tempIntRows[i] = i;
        tempLongRows[i] = (long) i;
        tempBytesRows[i] = str.getBytes();
        tempBigDecimalRows[i] = BigDecimal.valueOf(i);

        // Avoid creating empty arrays.
        int numMVElements = random.nextInt(maxNumberOfMVEntries) + 1;
        for (int j = 0; j < numMVElements; j++) {
          tempMVIntRows[i][j] = j;
          tempMVLongRows[i][j] = (long) j;
          tempMVStringRows[i][j] = str;
          tempMVByteRows[i][j] = str.getBytes();
        }
      }

      // Populate data for the MV columns with forward index disabled to have unique entries per row.
      // Avoid creating empty arrays.
      int numMVElements = random.nextInt(maxNumberOfMVEntries) + 1;
      for (int j = 0; j < numMVElements; j++) {
        String str = "n" + i + j;
        tempMVIntRowsForwardIndexDisabled[i][j] = j;
        tempMVLongRowsForwardIndexDisabled[i][j] = (long) j;
        tempMVStringRowsForwardIndexDisabled[i][j] = str;
        tempMVByteRowsForwardIndexDisabled[i][j] = str.getBytes();
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
      row.putValue(DIM_RAW_SORTED_INTEGER, i);

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

      // Dictionary SV columns
      row.putValue(DIM_DICT_INTEGER, tempIntRows[i]);
      row.putValue(DIM_DICT_LONG, tempLongRows[i]);
      row.putValue(DIM_DICT_STRING, tempStringRows[i]);
      row.putValue(DIM_DICT_BYES, tempBytesRows[i]);

      // Dictionary MV columns
      row.putValue(DIM_DICT_MV_BYTES, tempMVByteRows[i]);
      row.putValue(DIM_DICT_MV_INTEGER, tempMVIntRows[i]);
      row.putValue(DIM_DICT_MV_STRING, tempMVStringRows[i]);
      row.putValue(DIM_DICT_MV_LONG, tempMVLongRows[i]);

      // MV Raw columns
      row.putValue(DIM_MV_PASS_THROUGH_INTEGER, tempMVIntRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_LONG, tempMVLongRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_STRING, tempMVStringRows[i]);
      row.putValue(DIM_MV_PASS_THROUGH_BYTES, tempMVByteRows[i]);

      // Forward index disabled columns
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, tempIntRows[i]);
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_LONG, tempLongRows[i]);
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_STRING, tempStringRows[i]);
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_BYTES, tempBytesRows[i]);
      row.putValue(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, tempIntRows[i]);
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, tempIntRows[i]);
      row.putValue(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX, tempIntRows[i]);

      // Forward index disabled MV columns
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, tempMVIntRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_LONG, tempMVLongRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_STRING, tempMVStringRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_BYTES, tempMVByteRowsForwardIndexDisabled[i]);
      row.putValue(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, tempMVIntRowsForwardIndexDisabled[i]);

      // Forward index disabled MV columns with duplicates
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, tempMVIntRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG, tempMVLongRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, tempMVStringRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES, tempMVByteRows[i]);

      rows.add(row);
    }
    return rows;
  }

  @Test
  public void testComputeOperationNoOp() throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Validate with zero changes. ForwardIndexHandler should be a No-Op.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testComputeOperationEnableDictionary() throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_ZSTANDARD_STRING);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(DIM_ZSTANDARD_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_DICTIONARY));

    // TEST2: Enable dictionary for an MV column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_MV_PASS_THROUGH_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(DIM_MV_PASS_THROUGH_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_DICTIONARY));

    // TEST3: Enable dictionary for a dict column. Should be a No-op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST4: Add an additional text index. ForwardIndexHandler should be a No-Op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addTextIndexColumns(DIM_DICT_INTEGER);
    indexLoadingConfig.addTextIndexColumns(DIM_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST5: Add text index and enable dictionary.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addRangeIndexColumns(METRIC_LZ4_INTEGER);
    indexLoadingConfig.removeNoDictionaryColumns(METRIC_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(METRIC_LZ4_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_DICTIONARY));

    // TEST6: Enable Dictionary for sorted column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_RAW_SORTED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(DIM_RAW_SORTED_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_DICTIONARY));


    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testComputeOperationDisableDictionary() throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Disable dictionary for a raw column. Should be a no-op.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SNAPPY_INTEGER);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST2: Disable dictionary for a dictionary SV column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(DIM_DICT_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_DICTIONARY));

    // TEST3: Disable dictionary for a dictionary MV column.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_DICT_MV_BYTES);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.get(DIM_DICT_MV_BYTES),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_DICTIONARY));

    // TEST4: Disable dictionary and enable inverted index. Should be a no-op.
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_DICT_STRING);
    indexLoadingConfig.addInvertedIndexColumns(DIM_DICT_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testComputeOperationChangeCompression() throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Change compression
    Random rand = new Random();

    // Create new tableConfig with the modified fieldConfigs.
    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    int randIdx;
    String name;
    do {
      // Only try to change compression type for forward index enabled columns
      randIdx = rand.nextInt(fieldConfigs.size());
      name = fieldConfigs.get(randIdx).getName();
    } while (SV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name) || MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name)
        || MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.contains(name)
        || FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX.equals(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX.equals(name));
    FieldConfig config = fieldConfigs.remove(randIdx);
    CompressionCodec newCompressionType = null;
    for (CompressionCodec type : CompressionCodec.values()) {

      if (config.getCompressionCodec() != type) {
        newCompressionType = type;
        break;
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

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()),
        Collections.singletonList(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE));

    // TEST2: Change compression and add index. Change compressionType for more than 1 column.
    fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    FieldConfig config1 = fieldConfigs.remove(0);
    FieldConfig config2 = fieldConfigs.remove(1);

    FieldConfig newConfig1 = new FieldConfig(config1.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig1);
    FieldConfig newConfig2 = new FieldConfig(config2.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(),
        CompressionCodec.ZSTANDARD, null);
    fieldConfigs.add(newConfig2);

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(_noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    indexLoadingConfig.addTextIndexColumns(config1.getName());
    indexLoadingConfig.addInvertedIndexColumns(config1.getName());
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(config1.getName()),
        Collections.singletonList(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE));
    assertEquals(operationMap.get(config2.getName()),
        Collections.singletonList(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE));

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testComputeOperationDisableForwardIndex()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Disable forward index for a column which already has forward index disabled
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST2: Disable forward index for a dictionary column with forward index enabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_DICT_INTEGER);
    indexLoadingConfig.addInvertedIndexColumns(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_DICT_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX));

    // TEST3: Disable forward index for a raw column with forward index enabled and enable inverted index and
    // dictionary
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_LZ4_INTEGER);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_LZ4_INTEGER);
    indexLoadingConfig.addInvertedIndexColumns(DIM_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    Set<ForwardIndexHandler.Operation> operations = new HashSet<>(operationMap.get(DIM_LZ4_INTEGER));
    assertEquals(operations.size(), 2);
    Set<ForwardIndexHandler.Operation> expectedOperations =
        new HashSet<>(Arrays.asList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
            ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    assertEquals(expectedOperations, operations);

    // TEST4: Disable forward index for two dictionary columns with forward index enabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_DICT_LONG);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_DICT_STRING);
    indexLoadingConfig.addInvertedIndexColumns(DIM_DICT_LONG);
    indexLoadingConfig.addInvertedIndexColumns(DIM_DICT_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(DIM_DICT_LONG),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX));
    assertEquals(operationMap.get(DIM_DICT_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX));

    // TEST5: Disable forward index for two raw columns with forward index enabled and enable dictionary
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_LZ4_LONG);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_LZ4_LONG);
    indexLoadingConfig.addInvertedIndexColumns(DIM_LZ4_LONG);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_SNAPPY_STRING);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_SNAPPY_STRING);
    indexLoadingConfig.addInvertedIndexColumns(DIM_SNAPPY_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    operations = new HashSet<>(operationMap.get(DIM_LZ4_LONG));
    assertEquals(operations.size(), 2);
    expectedOperations = new HashSet<>(Arrays.asList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
            ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    assertEquals(expectedOperations, operations);
    operations = new HashSet<>(operationMap.get(DIM_SNAPPY_STRING));
    assertEquals(operations.size(), 2);
    assertEquals(expectedOperations, operations);

    // TEST6: Disable forward index for a dictionary and a raw column with forward index enabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_ZSTANDARD_INTEGER);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_ZSTANDARD_INTEGER);
    indexLoadingConfig.addInvertedIndexColumns(DIM_ZSTANDARD_INTEGER);
    indexLoadingConfig.addInvertedIndexColumns(DIM_DICT_STRING);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_DICT_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    operations = new HashSet<>(operationMap.get(DIM_ZSTANDARD_INTEGER));
    assertEquals(operations.size(), 2);
    expectedOperations = new HashSet<>(Arrays.asList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
        ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    assertEquals(expectedOperations, operations);
    assertEquals(operationMap.get(DIM_DICT_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX));

    // TEST7: Disable forward index for a raw column without enabling dictionary or inverted index
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_LZ4_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_LZ4_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX));

    // TEST8: Disable forward index for a dictionary column and also disable dictionary and inverted index
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_DICT_INTEGER);
    indexLoadingConfig.addNoDictionaryColumns(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    operations = new HashSet<>(operationMap.get(DIM_DICT_INTEGER));
    assertEquals(operations.size(), 2);
    expectedOperations = new HashSet<>(Arrays.asList(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
        ForwardIndexHandler.Operation.DISABLE_DICTIONARY));
    assertEquals(expectedOperations, operations);

    // TEST9: Disable dictionary on a column that already has forward index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_DICTIONARY));

    // TEST10: Disable inverted index on a column that already has forward index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST11: Disable dictionary on a column that already has forward index disabled and inverted index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX),
        Collections.singletonList(ForwardIndexHandler.Operation.DISABLE_DICTIONARY));

    // TEST12: Enable dictionary on a column that already has forward index disabled and dictionary disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.removeNoDictionaryColumns(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    try {
      operationMap = fwdIndexHandler.computeOperations(writer);
      Assert.fail("Enabling dictionary on forward index disabled column is not possible");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Cannot regenerate the dictionary for column "
          + "DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER with forward index disabled. Please refresh or back-fill "
          + "the data to add back the forward index");
    }

    // TEST13: Disable dictionary on a column that already has forward index disabled without an inverted index but
    // with a range index
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    try {
      operationMap = fwdIndexHandler.computeOperations(writer);
      Assert.fail("Disabling dictionary on forward index disabled column without inverted index but which has a "
          + "range index is not possible");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Must disable range (enabled) index to disable the dictionary for a "
          + "forwardIndexDisabled column: DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX or refresh / "
          + "back-fill the forward index");
    }

    // TEST13: Disable dictionary on a column that already has forward index disabled and inverted index enabled with
    // a range index
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.addRangeIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    try {
      operationMap = fwdIndexHandler.computeOperations(writer);
      Assert.fail("Disabling dictionary on forward index disabled column with inverted index and a range index "
          + "is not possible");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Must disable range (enabled) index to disable the dictionary for a "
          + "forwardIndexDisabled column: DIM_SV_FORWARD_INDEX_DISABLED_INTEGER or refresh / back-fill the "
          + "forward index");
    }
  }

  @Test
  public void testComputeOperationEnableForwardIndex()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // TEST1: Try to change compression type for a forward index disabled column and enable forward index for it
    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    int randIdx;
    Random rand = new Random();
    String name;

    do {
      // Only try to change compression type for forward index disabled columns
      randIdx = rand.nextInt(fieldConfigs.size());
      name = fieldConfigs.get(randIdx).getName();
    } while (!SV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name) && !MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name));
    FieldConfig config = fieldConfigs.remove(randIdx);
    CompressionCodec newCompressionType = null;
    for (CompressionCodec type : RAW_COMPRESSION_TYPES) {
      if (config.getCompressionCodec() != type) {
        newCompressionType = type;
        break;
      }
    }
    FieldConfig newConfig =
        new FieldConfig(config.getName(), FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType,
            null);
    fieldConfigs.add(newConfig);
    List<String> noDictionaryColumns = new ArrayList<>(_noDictionaryColumns);
    noDictionaryColumns.add(config.getName());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(noDictionaryColumns)
            .setInvertedIndexColumns(_invertedIndexColumns).setFieldConfigList(fieldConfigs).build();
    tableConfig.setFieldConfigList(fieldConfigs);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    indexLoadingConfig.addNoDictionaryColumns(config.getName());
    indexLoadingConfig.removeInvertedIndexColumns(config.getName());
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    Map<String, List<ForwardIndexHandler.Operation>> operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(config.getName()),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST2: Enable forward index in dictionary format for a column with forward index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_BYTES);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_BYTES),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST3: Enable forward index in raw format for a column with forward index disabled. Remove column from inverted
    // index as well (inverted index needs dictionary)
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.addNoDictionaryColumns(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 1);
    assertEquals(operationMap.get(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST4: Enable forward index in dictionary format for two columns with forward index disabled. Disable inverted
    // index for one of them
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_MV_FORWARD_INDEX_DISABLED_STRING);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_LONG),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));
    assertEquals(operationMap.get(DIM_MV_FORWARD_INDEX_DISABLED_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST5: Enable forward index in raw format for two columns with forward index disabled. Remove column from
    // inverted index as well (inverted index needs dictionary)
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_STRING);
    indexLoadingConfig.addNoDictionaryColumns(DIM_SV_FORWARD_INDEX_DISABLED_STRING);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_STRING);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.addNoDictionaryColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_STRING),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));
    assertEquals(operationMap.get(DIM_MV_FORWARD_INDEX_DISABLED_LONG),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST6: Enable forward index in dictionary format and one in raw format for columns with forward index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.addNoDictionaryColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.removeInvertedIndexColumns(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_BYTES);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap.size(), 2);
    assertEquals(operationMap.get(DIM_MV_FORWARD_INDEX_DISABLED_LONG),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));
    assertEquals(operationMap.get(DIM_SV_FORWARD_INDEX_DISABLED_BYTES),
        Collections.singletonList(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX));

    // TEST7: Enable forward index for a raw column with forward index disabled and keep it as raw
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // TEST8: Enable forward index for a dictionary based column with forward index and inverted index disabled
    indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    indexLoadingConfig.removeForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    operationMap = fwdIndexHandler.computeOperations(writer);
    assertEquals(operationMap, Collections.EMPTY_MAP);

    // Tear down
    segmentLocalFSDirectory.close();
  }

  @Test
  public void testChangeCompressionForSingleColumn()
      throws Exception {
    for (int i = 0; i < _noDictionaryColumns.size(); i++) {
      if (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(_noDictionaryColumns.get(i))) {
        // Don't run this test for forward index disabled columns
        continue;
      }
      // For every noDictionaryColumn, change the compressionType to all available types, one by one.
      for (CompressionCodec compressionType : RAW_COMPRESSION_TYPES) {
        // Setup
        SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
        SegmentDirectory segmentLocalFSDirectory =
            new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

        List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
        int index = -1;
        for (int j = 0; j < fieldConfigs.size(); j++) {
          if (fieldConfigs.get(j).getName().equals(_noDictionaryColumns.get(i))) {
            index = j;
            break;
          }
        }
        FieldConfig config = fieldConfigs.remove(index);
        String columnName = config.getName();

        FieldConfig newConfig =
            new FieldConfig(columnName, FieldConfig.EncodingType.RAW, Collections.emptyList(), compressionType,
                null);
        fieldConfigs.add(newConfig);

        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .setNoDictionaryColumns(_noDictionaryColumns).setFieldConfigList(fieldConfigs).build();
        tableConfig.setFieldConfigList(fieldConfigs);

        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
        ForwardIndexHandler fwdIndexHandler =
            new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
        boolean val = fwdIndexHandler.needUpdateIndices(writer);
        fwdIndexHandler.updateIndices(writer);
        fwdIndexHandler.postUpdateIndicesCleanup(writer);

        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
        segmentLocalFSDirectory.close();

        // Validation
        ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(columnName);
        testIndexExists(columnName, StandardIndexes.forward());
        validateIndexMap(columnName, false, false);
        validateForwardIndex(columnName, compressionType, metadata.isSorted());

        // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
        // change.
        validateMetadataProperties(columnName, metadata.hasDictionary(), metadata.getColumnMaxLength(),
            metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
            metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
            metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
            metadata.getMaxValue(), false);
      }
    }
  }

  @Test
  public void testChangeDictCompression()
      throws Exception {
    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());

    // Change to MV_ENTRY_DICT compression
    for (String column : DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX) {
      FieldConfig newFieldConfig = new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
          CompressionCodec.MV_ENTRY_DICT, null);
      fieldConfigs.add(newFieldConfig);
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
          .setNoDictionaryColumns(_noDictionaryColumns).setFieldConfigList(fieldConfigs).build();

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      try (SegmentDirectory segmentLocalFSDirectory = new SegmentLocalFSDirectory(_segmentDirectory, segmentMetadata,
          ReadMode.mmap); SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter()) {
        ForwardIndexHandler forwardIndexHandler =
            new ForwardIndexHandler(segmentLocalFSDirectory, new IndexLoadingConfig(tableConfig, null), null);

        Map<String, List<ForwardIndexHandler.Operation>> operations = forwardIndexHandler.computeOperations(writer);
        assertEquals(operations, Collections.singletonMap(column,
            Collections.singletonList(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
        assertTrue(forwardIndexHandler.needUpdateIndices(writer));

        forwardIndexHandler.updateIndices(writer);
        forwardIndexHandler.postUpdateIndicesCleanup(writer);
      }

      segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      try (SegmentDirectory segmentLocalFSDirectory = new SegmentLocalFSDirectory(_segmentDirectory, segmentMetadata,
          ReadMode.mmap); SegmentDirectory.Reader reader = segmentLocalFSDirectory.createReader()) {
        ForwardIndexReader<?> forwardIndexReader =
            ForwardIndexType.read(reader, segmentMetadata.getColumnMetadataFor(column));
        assertTrue(forwardIndexReader.isDictionaryEncoded());
        assertFalse(forwardIndexReader.isSingleValue());
        assertEquals(forwardIndexReader.getDictIdCompressionType(), DictIdCompressionType.MV_ENTRY_DICT);
      }
    }

    // Change back to regular forward index
    for (int i = 0; i < DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX.size(); i++) {
      FieldConfig fieldConfig = fieldConfigs.remove(fieldConfigs.size() - 1);
      String column = fieldConfig.getName();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
          .setNoDictionaryColumns(_noDictionaryColumns).setFieldConfigList(fieldConfigs).build();

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      try (SegmentDirectory segmentLocalFSDirectory = new SegmentLocalFSDirectory(_segmentDirectory, segmentMetadata,
          ReadMode.mmap); SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter()) {
        ForwardIndexHandler forwardIndexHandler =
            new ForwardIndexHandler(segmentLocalFSDirectory, new IndexLoadingConfig(tableConfig, null), null);

        Map<String, List<ForwardIndexHandler.Operation>> operations = forwardIndexHandler.computeOperations(writer);
        assertEquals(operations, Collections.singletonMap(column,
            Collections.singletonList(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
        assertTrue(forwardIndexHandler.needUpdateIndices(writer));

        forwardIndexHandler.updateIndices(writer);
        forwardIndexHandler.postUpdateIndicesCleanup(writer);
      }

      segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      try (SegmentDirectory segmentLocalFSDirectory = new SegmentLocalFSDirectory(_segmentDirectory, segmentMetadata,
          ReadMode.mmap); SegmentDirectory.Reader reader = segmentLocalFSDirectory.createReader()) {
        ForwardIndexReader<?> forwardIndexReader =
            ForwardIndexType.read(reader, segmentMetadata.getColumnMetadataFor(column));
        assertTrue(forwardIndexReader.isDictionaryEncoded());
        assertFalse(forwardIndexReader.isSingleValue());
        assertNull(forwardIndexReader.getDictIdCompressionType());
      }
    }
  }

  @Test
  public void testChangeCompressionForMultipleColumns()
      throws Exception {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    List<FieldConfig> fieldConfigs = new ArrayList<>(_tableConfig.getFieldConfigList());
    Random rand = new Random();
    int randomIdx = rand.nextInt(RAW_COMPRESSION_TYPES.size());
    CompressionCodec newCompressionType = RAW_COMPRESSION_TYPES.get(randomIdx);

    // Column 1
    String name;
    do {
      // Only try to change compression type for forward index enabled columns
      randomIdx = rand.nextInt(fieldConfigs.size());
      name = fieldConfigs.get(randomIdx).getName();
    } while (SV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name) || MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name)
        || MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.contains(name)
        || FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX.equals(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX.equals(name));
    FieldConfig config1 = fieldConfigs.remove(randomIdx);
    String column1 = config1.getName();
    FieldConfig newConfig1 =
        new FieldConfig(column1, FieldConfig.EncodingType.RAW, Collections.emptyList(), newCompressionType, null);
    fieldConfigs.add(newConfig1);

    // Column 2
    do {
      // Only try to change compression type for forward index enabled columns
      randomIdx = rand.nextInt(fieldConfigs.size());
      name = fieldConfigs.get(randomIdx).getName();
    } while (SV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name) || MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(name)
        || MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.contains(name)
        || FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX.equals(name)
        || DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX.equals(name));
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
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, null);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    testIndexExists(column1, StandardIndexes.forward());
    validateIndexMap(column1, false, false);
    validateForwardIndex(column1, newCompressionType, metadata.isSorted());
    // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
    // change.
    validateMetadataProperties(column1, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);

    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    testIndexExists(column2, StandardIndexes.forward());
    validateIndexMap(column2, false, false);
    validateForwardIndex(column2, newCompressionType, metadata.isSorted());
    validateMetadataProperties(column2, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
  }

  @Test
  public void testEnableDictionaryForMultipleColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    String col1;
    do {
      col1 = _noDictionaryColumns.get(rand.nextInt(_noDictionaryColumns.size()));
    } while (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(col1));
    indexLoadingConfig.removeNoDictionaryColumns(col1);
    String col2;
    do {
      col2 = _noDictionaryColumns.get(rand.nextInt(_noDictionaryColumns.size()));
    } while (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(col2) || col2.equals(col1));
    indexLoadingConfig.removeNoDictionaryColumns(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    testIndexExists(col1, StandardIndexes.forward());
    testIndexExists(col1, StandardIndexes.dictionary());
    validateIndexMap(col1, true, false);
    validateForwardIndex(col1, null, metadata.isSorted());
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    int dictionaryElementSize = 0;
    FieldSpec.DataType dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(col1, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    testIndexExists(col2, StandardIndexes.forward());
    testIndexExists(col2, StandardIndexes.dictionary());
    validateIndexMap(col2, true, false);
    validateForwardIndex(col2, null, metadata.isSorted());
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    dictionaryElementSize = 0;
    dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(col2, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testEnableDictionaryForSortedColumn() throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    for (int i = 0; i < RAW_SORTED_INDEX_COLUMNS.size(); i++) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      String column = RAW_SORTED_INDEX_COLUMNS.get(i);
      indexLoadingConfig.removeNoDictionaryColumns(column);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      testIndexExists(column, StandardIndexes.dictionary());
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null, metadata.isSorted());

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      FieldSpec.DataType dataType = metadata.getDataType();
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableDictionaryForSingleColumn()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    for (int i = 0; i < _noDictionaryColumns.size(); i++) {
      if (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(_noDictionaryColumns.get(i))) {
        // Skip the RAW forward index disabled columns
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      String column = _noDictionaryColumns.get(i);
      indexLoadingConfig.removeNoDictionaryColumns(column);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      testIndexExists(column, StandardIndexes.dictionary());
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null, metadata.isSorted());

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      FieldSpec.DataType dataType = metadata.getDataType();
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexForMultipleDictColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    String col1 = DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(
        rand.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    indexLoadingConfig.addForwardIndexDisabledColumns(col1);
    indexLoadingConfig.addInvertedIndexColumns(col1);
    String col2;
    do {
      col2 = DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(rand.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    } while (col2.equals(col1));
    indexLoadingConfig.addForwardIndexDisabledColumns(col2);
    indexLoadingConfig.addInvertedIndexColumns(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    validateIndexMap(col1, true, true);
    validateIndexesForForwardIndexDisabledColumns(col1);
    // In column metadata, nothing should change.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    validateMetadataProperties(col1, metadata.hasDictionary(), metadata.getColumnMaxLength(), metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    validateIndexMap(col2, true, true);
    validateIndexesForForwardIndexDisabledColumns(col2);
    // In column metadata, nothing should change.
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    validateMetadataProperties(col2, metadata.hasDictionary(), metadata.getColumnMaxLength(), metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testDisableForwardIndexForSingleDictColumn()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    for (String column : DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.add(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
      invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing should change.
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableDictionaryForSingleColumn()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    for (int i = 0; i < DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size(); i++) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      String column = DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(i);
      indexLoadingConfig.addNoDictionaryColumns(column);

      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      segmentLocalFSDirectory.close();

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      validateIndexMap(column, false, false);
      // All the columns are dimensions. So default compression type is LZ4.
      validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
          metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableDictionaryForMultipleColumns()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    // Column 1
    Random rand = new Random();
    int randomIdx = rand.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size());
    String column1 = DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(randomIdx);
    indexLoadingConfig.addNoDictionaryColumns(column1);

    // Column 2
    randomIdx = rand.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size());
    String column2 = DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(randomIdx);
    indexLoadingConfig.addNoDictionaryColumns(column2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    segmentLocalFSDirectory.close();

    // Column1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    testIndexExists(column1, StandardIndexes.forward());
    validateIndexMap(column1, false, false);
    // All the columns are dimensions. So default compression type is LZ4.
    validateForwardIndex(column1, CompressionCodec.LZ4, metadata.isSorted());

    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    validateMetadataProperties(column1, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);

    // Column2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    testIndexExists(column2, StandardIndexes.forward());
    validateIndexMap(column2, false, false);
    // All the columns are dimensions. So default compression type is LZ4.
    validateForwardIndex(column2, CompressionCodec.LZ4, metadata.isSorted());

    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    validateMetadataProperties(column2, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testDisableForwardIndexForMultipleRawColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    String col1 = RAW_LZ4_INDEX_COLUMNS.get(
        rand.nextInt(RAW_LZ4_INDEX_COLUMNS.size()));
    indexLoadingConfig.addForwardIndexDisabledColumns(col1);
    indexLoadingConfig.removeNoDictionaryColumns(col1);
    indexLoadingConfig.addInvertedIndexColumns(col1);
    String col2 = RAW_SNAPPY_INDEX_COLUMNS.get(rand.nextInt(RAW_SNAPPY_INDEX_COLUMNS.size()));
    indexLoadingConfig.addForwardIndexDisabledColumns(col2);
    indexLoadingConfig.removeNoDictionaryColumns(col2);
    indexLoadingConfig.addInvertedIndexColumns(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    validateIndexMap(col1, true, true);
    validateIndexesForForwardIndexDisabledColumns(col1);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    int dictionaryElementSize = 0;
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    FieldSpec.DataType dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(col1, true, dictionaryElementSize, metadata.getCardinality(),
        metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    validateIndexMap(col2, true, true);
    validateIndexesForForwardIndexDisabledColumns(col2);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    dictionaryElementSize = 0;
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    dataType = metadata.getDataType();
    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(col2, true, dictionaryElementSize, metadata.getCardinality(),
        metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testDisableForwardIndexForSingleRawColumn()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    for (String column : _noDictionaryColumns) {
      if (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(column) || RAW_SORTED_INDEX_COLUMNS.contains(column)) {
        // Forward index already disabled for these columns, skip them
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.add(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      indexLoadingConfig.removeNoDictionaryColumns(forwardIndexDisabledColumns);
      indexLoadingConfig.addNoDictionaryColumns(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
      invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      FieldSpec.DataType dataType = metadata.getDataType();
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexForRawAndInvertedIndexDisabledColumns()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    for (String column : _noDictionaryColumns) {
      if (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(column) || RAW_SORTED_INDEX_COLUMNS.contains(column)) {
        // Forward index already disabled for these columns, skip them
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.add(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
      invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      invertedIndexColumns.remove(column);
      indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      validateIndexMap(column, false, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      FieldSpec.DataType dataType = metadata.getDataType();
      validateMetadataProperties(column, false, 0, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexForInvertedIndexDisabledColumns()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    Set<String> noDictColumnsToRemove = new HashSet<>();
    noDictColumnsToRemove.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    for (String column : _noDictionaryColumns) {
      if (FORWARD_INDEX_DISABLED_RAW_COLUMNS.contains(column) || RAW_SORTED_INDEX_COLUMNS.contains(column)) {
        // Forward index already disabled for these columns, skip them
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.add(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      noDictColumnsToRemove.add(column);
      indexLoadingConfig.removeNoDictionaryColumns(noDictColumnsToRemove);
      Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
      invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      invertedIndexColumns.remove(column);
      indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      FieldSpec.DataType dataType = metadata.getDataType();
      int dictionaryElementSize = 0;
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == FieldSpec.DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexInDictModeForMultipleForwardIndexDisabledColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    // Remove from forward index list but keep the inverted index enabled
    String col1 = SV_FORWARD_INDEX_DISABLED_COLUMNS.get(rand.nextInt(SV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(col1);
    String col2 = MV_FORWARD_INDEX_DISABLED_COLUMNS.get(rand.nextInt(MV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    validateIndexMap(col1, true, false);
    validateForwardIndex(col1, null, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(col1, metadata.hasDictionary(), metadata.getColumnMaxLength(), metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    validateIndexMap(col2, true, false);
    validateForwardIndex(col2, null, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(col2, metadata.hasDictionary(), metadata.getColumnMaxLength(), metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testEnableForwardIndexInDictModeForMVForwardIndexDisabledColumnWithDuplicates()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    // Remove from forward index list but keep the inverted index enabled
    String column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS
        .get(rand.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(column);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Column validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
    validateIndexMap(column, true, false);
    validateForwardIndex(column, null, metadata.isSorted());
    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        true);
  }

  @Test
  public void testEnableForwardIndexInDictModeForSingleForwardIndexDisabledColumn()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    List<String> allForwardIndexDisabledColumns = new ArrayList<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    allForwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    for (String column : allForwardIndexDisabledColumns) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      // Leave the inverted index as is, should ideally work even if inverted index is disabled
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.remove(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null, metadata.isSorted());

      // In column metadata, nothing should change.
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexInRawModeForMultipleForwardIndexDisabledColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    String col1 = SV_FORWARD_INDEX_DISABLED_COLUMNS.get(rand.nextInt(SV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(col1);
    indexLoadingConfig.removeInvertedIndexColumns(col1);
    indexLoadingConfig.addNoDictionaryColumns(col1);
    String col2 = MV_FORWARD_INDEX_DISABLED_COLUMNS.get(rand.nextInt(MV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(col2);
    indexLoadingConfig.removeInvertedIndexColumns(col2);
    indexLoadingConfig.addNoDictionaryColumns(col2);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(col1);
    validateIndexMap(col1, false, false);
    validateForwardIndex(col1, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(col1, false, 0, metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(col2);
    validateIndexMap(col2, false, false);
    validateForwardIndex(col2, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(col2, false, 0, metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testEnableForwardIndexInRawModeForMVForwardIndexDisabledColumnWithDuplicates()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    // Remove from forward index list but keep the inverted index enabled
    String column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS
        .get(rand.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    indexLoadingConfig.removeForwardIndexDisabledColumns(column);
    indexLoadingConfig.removeInvertedIndexColumns(column);
    indexLoadingConfig.addNoDictionaryColumns(column);

    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Column validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
    validateIndexMap(column, false, false);
    validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    validateMetadataProperties(column, false, 0, metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), true);
  }

  @Test
  public void testEnableForwardIndexInRawModeForSingleForwardIndexDisabledColumn()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    List<String> allForwardIndexDisabledColumns = new ArrayList<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    allForwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    for (String column : allForwardIndexDisabledColumns) {
      SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
      SegmentDirectory segmentLocalFSDirectory =
          new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
      SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
      forwardIndexDisabledColumns.remove(column);
      indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
      Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
      invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);
      indexLoadingConfig.addNoDictionaryColumns(column);
      ForwardIndexHandler fwdIndexHandler =
          new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
      fwdIndexHandler.updateIndices(writer);
      fwdIndexHandler.postUpdateIndicesCleanup(writer);

      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      segmentLocalFSDirectory.close();

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      validateIndexMap(column, false, false);
      validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());

      // In column metadata, nothing should change.
      validateMetadataProperties(column, false, 0,
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexForInvertedIndexDisabledColumn()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);

    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    forwardIndexDisabledColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
    Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
    invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);

    validateIndexMap(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, true, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);

    ForwardIndexHandler fwdIndexHandler =
        new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Validate nothing has changed
    validateIndexMap(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, true, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);

    // In column metadata, nothing should change.
    ColumnMetadata metadata =
        existingSegmentMetadata.getColumnMetadataFor(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    validateMetadataProperties(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
        metadata.getMaxValue(), false);
  }

  @Test
  public void testEnableForwardIndexForDictionaryDisabledColumns()
      throws Exception {
    Set<String> forwardIndexDisabledColumns = new HashSet<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    forwardIndexDisabledColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    forwardIndexDisabledColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);

    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);
    forwardIndexDisabledColumns.remove(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    forwardIndexDisabledColumns.remove(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);
    indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
    Set<String> invertedIndexColumns = new HashSet<>(forwardIndexDisabledColumns);
    invertedIndexColumns.removeAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);
    invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexColumns);

    validateIndexMap(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);
    validateIndexMap(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);

    ForwardIndexHandler fwdIndexHandler =
        new ForwardIndexHandler(segmentLocalFSDirectory, indexLoadingConfig, _schema);
    fwdIndexHandler.updateIndices(writer);
    fwdIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Validate nothing has changed
    validateIndexMap(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    validateIndexMap(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);

    // In column metadata, nothing should change.
    ColumnMetadata metadata =
        existingSegmentMetadata.getColumnMetadataFor(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    validateMetadataProperties(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
        metadata.getMaxValue(), false);
    metadata =
        existingSegmentMetadata.getColumnMetadataFor(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);
    validateMetadataProperties(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
        metadata.getMaxValue(), false);
  }

  @Test
  public void testAddOtherIndexForForwardIndexDisabledColumn()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    Random rand = new Random();
    // Add column to range index list. Must be a numerical type.
    String column;
    do {
      column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS
          .get(rand.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    } while (!column.equals(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING)
        && !column.equals(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES));
    indexLoadingConfig.addRangeIndexColumns(column);

    RangeIndexHandler rangeIndexHandler = new RangeIndexHandler(segmentLocalFSDirectory, indexLoadingConfig);
    rangeIndexHandler.updateIndices(writer);

    // Validate forward index exists before calling post cleanup
    validateIndexMap(column, true, false);

    rangeIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentLocalFSDirectory.close();

    // Validate index map including range index. Forward index should not exist, range index and dictionary should
    validateIndexMap(column, true, true);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    String segmentDir = INDEX_DIR + "/" + SEGMENT_NAME + "/v3";
    File idxMapFile = new File(segmentDir, V1Constants.INDEX_MAP_FILE_NAME);
    String indexMapStr = FileUtils.readFileToString(idxMapFile, StandardCharsets.UTF_8);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index" + ".startOffset"), 1, column);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index" + ".size"), 1, column);

    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
    validateMetadataProperties(column, true, 7, metadata.getCardinality(),
        metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(),
        metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(),
        metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(), true);

    // Validate that expected metadata properties don't match. totalNumberOfEntries will definitely not match since
    // duplicates will be removed, but maxNumberOfMultiValues may still match if the row with max multi-values didn't
    // have any duplicates.
    segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
    assertNotEquals(metadata.getTotalNumberOfEntries(), columnMetadata.getTotalNumberOfEntries());
  }

  @Test
  public void testAddOtherIndexWhenForwardIndexDisabledAndInvertedIndexOrDictionaryDisabled()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, _tableConfig);

    // Add a forward index and inverted index disabled column to the range index list
    indexLoadingConfig.addRangeIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    RangeIndexHandler rangeIndexHandler = new RangeIndexHandler(segmentLocalFSDirectory, indexLoadingConfig);
    try {
      rangeIndexHandler.updateIndices(writer);
      Assert.fail("Creating the range index on forward index and inverted index disabled column should fail");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Forward index disabled column "
          + "DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX must have an inverted index to regenerate the "
          + "forward index. Regeneration of the forward index is required to create new indexes as well. Please "
          + "refresh or back-fill the forward index");
    }

    // Remove inverted index disabled column from range index list and add a raw column instead
    indexLoadingConfig.removeRangeIndexColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    indexLoadingConfig.addRangeIndexColumns(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    rangeIndexHandler = new RangeIndexHandler(segmentLocalFSDirectory, indexLoadingConfig);
    try {
      rangeIndexHandler.updateIndices(writer);
      Assert.fail("Creating the range index on forward index and inverted index disabled column should fail");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Forward index disabled column "
          + "DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER must have a dictionary to regenerate the forward index. "
          + "Regeneration of the forward index is required to create new indexes as well. Please refresh or back-fill "
          + "the forward index");
    }
  }

  /**
   * Tests to verify various combinations of inputs to test dictionary override optimization.
   */
  @Test
  public void testDictionaryOverride() {
    FieldSpec fieldSpec = new MetricFieldSpec();
    fieldSpec.setName("test");
    fieldSpec.setDataType(FieldSpec.DataType.DOUBLE);
    IndexType index1 = Mockito.mock(IndexType.class);
    Mockito.when(index1.getId()).thenReturn("index1");
    IndexConfig indexConf = new IndexConfig(true);
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
        .add(index1, indexConf)
        .build();
    // No need to disable dictionary
    boolean result = DictionaryIndexType.ignoreDictionaryOverride(false, true,
        2, fieldSpec,
        fieldIndexConfigs, 5, 20);
    Assert.assertEquals(result, true);

    // Set a higher noDictionarySizeRatioThreshold
    result = DictionaryIndexType.ignoreDictionaryOverride(false, true,
        5, fieldSpec,
        fieldIndexConfigs, 5, 20);
    Assert.assertEquals(result, false);

    // optimizeDictionary and optimizeDictionaryForMetrics both turned on
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true,
        5, fieldSpec,
        fieldIndexConfigs, 5, 20);
    Assert.assertEquals(result, false);

    // Don't ignore for Json. We want to disable dictionary for json.
    fieldSpec = new DimensionFieldSpec();
    fieldSpec.setName("test");
    fieldSpec.setDataType(FieldSpec.DataType.JSON);
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true,
        5, fieldSpec,
        fieldIndexConfigs, 5, 20);
    Assert.assertEquals(result, true);
  }

  private void validateIndexesForForwardIndexDisabledColumns(String columnName)
      throws IOException {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    ColumnMetadata columnMetadata = existingSegmentMetadata.getColumnMetadataFor(columnName);

    if (columnMetadata.hasDictionary()) {
      assertTrue(writer.hasIndexFor(columnName, StandardIndexes.dictionary()));
      Dictionary dictionary = DictionaryIndexType.read(writer, columnMetadata);
      assertEquals(columnMetadata.getCardinality(), dictionary.length());
    } else {
      assertFalse(writer.hasIndexFor(columnName, StandardIndexes.dictionary()));
    }
    if (columnMetadata.isSorted()) {
      assertTrue(writer.hasIndexFor(columnName, StandardIndexes.forward()));
    } else {
      assertFalse(writer.hasIndexFor(columnName, StandardIndexes.forward()));
    }
  }

  private void validateForwardIndex(String columnName, @Nullable CompressionCodec expectedCompressionType,
      boolean isSorted)
      throws IOException {
    // Setup
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentLocalFSDirectory.createWriter();
    ColumnMetadata columnMetadata = existingSegmentMetadata.getColumnMetadataFor(columnName);
    boolean isSingleValue = columnMetadata.isSingleValue();

    if (expectedCompressionType == null) {
      assertTrue(writer.hasIndexFor(columnName, StandardIndexes.dictionary()));
    } else {
      assertFalse(writer.hasIndexFor(columnName, StandardIndexes.dictionary()));
    }
    assertTrue(writer.hasIndexFor(columnName, StandardIndexes.forward()));

    // Check Compression type in header
    ForwardIndexReader fwdIndexReader = ForwardIndexType.read(writer, columnMetadata);
    ChunkCompressionType fwdIndexCompressionType = fwdIndexReader.getCompressionType();
    if (expectedCompressionType != null) {
      assertEquals(fwdIndexCompressionType.name(), expectedCompressionType.name());
    }

    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(writer, columnMetadata)) {
      Dictionary dictionary = null;
      if (columnMetadata.hasDictionary()) {
        dictionary = DictionaryIndexType.read(writer, columnMetadata);
      }
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(forwardIndexReader, dictionary, null,
          columnMetadata.getMaxNumberOfMultiValues());

      for (int rowIdx = 0; rowIdx < columnMetadata.getTotalDocs(); rowIdx++) {
        // For MV forward index disabled columns cannot do this validation as we had to create a unique set of elements
        if (!MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(columnName) && (rowIdx % 10 == 0)) {
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
                int expectedVal = isSorted ? rowIdx : 1001;
                assertEquals((int) val, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
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
                long expectedVal = isSorted ? rowIdx : 1001L;
                assertEquals((long) val, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
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
              assertEquals((BigDecimal) val, BigDecimal.valueOf(1001));
              break;
            }
            default:
              // Unreachable code.
              throw new IllegalStateException("Invalid datatype for column=" + columnName);
          }
        } else if (MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(columnName)) {
          Object val = columnReader.getValue(rowIdx);
          FieldSpec.DataType dataType = columnMetadata.getDataType();

          // Regenerating forward index from inverted index and dictionary does not guarantee ordering within a given MV
          // row. To validate the data in the row is correct, first generate a set of all possible entries stored at
          // each row and then ensure that every entry is found in the pre-constructed set.
          switch (dataType) {
            case STRING: {
              Object[] values = (Object[]) val;
              int length = values.length;
              Set<String> stringSet = new HashSet<>();
              for (int i = 0; i < length; i++) {
                stringSet.add("n" + rowIdx + i);
              }
              for (Object value : values) {
                assertTrue(stringSet.contains((String) value));
                stringSet.remove((String) value);
              }
              break;
            }
            case INT: {
              Object[] values = (Object[]) val;
              int length = values.length;
              Set<Integer> integerSet = new HashSet<>();
              for (int i = 0; i < length; i++) {
                integerSet.add(i);
              }
              for (Object value : values) {
                assertTrue(integerSet.contains((Integer) value));
                integerSet.remove((Integer) value);
              }
              break;
            }
            case LONG: {
              Object[] values = (Object[]) val;
              int length = values.length;
              Set<Long> longSet = new HashSet<>();
              for (int i = 0; i < length; i++) {
                longSet.add((long) i);
              }
              for (Object value : values) {
                assertTrue(longSet.contains((Long) value));
                longSet.remove((Long) value);
              }
              break;
            }
            case BYTES: {
              Object[] values = (Object[]) val;
              int length = values.length;
              Set<ByteBuffer> bytesSet = new HashSet<>();
              for (int i = 0; i < length; i++) {
                String expectedString = "n" + rowIdx + i;
                ByteBuffer expectedVal = ByteBuffer.wrap(expectedString.getBytes());
                bytesSet.add(expectedVal);
              }
              for (Object value : values) {
                assertTrue(bytesSet.contains(ByteBuffer.wrap((byte[]) value)));
                bytesSet.remove(ByteBuffer.wrap((byte[]) value));
              }
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

  private void testIndexExists(String columnName, IndexType<?, ?, ?> indexType) throws Exception {
    SegmentMetadataImpl existingSegmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    SegmentDirectory segmentLocalFSDirectory =
        new SegmentLocalFSDirectory(_segmentDirectory, existingSegmentMetadata, ReadMode.mmap);
    SegmentDirectory.Reader reader = segmentLocalFSDirectory.createReader();

    assertTrue(reader.hasIndexFor(columnName, indexType));
  }

  private void validateIndexMap(String columnName, boolean dictionaryEnabled, boolean forwardIndexDisabled)
      throws IOException {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);

    // Panic validation to make sure all columns have only one forward index entry in index map.
    String segmentDir = INDEX_DIR + "/" + SEGMENT_NAME + "/v3";
    File idxMapFile = new File(segmentDir, V1Constants.INDEX_MAP_FILE_NAME);
    String indexMapStr = FileUtils.readFileToString(idxMapFile, StandardCharsets.UTF_8);
    if (forwardIndexDisabled) {
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".startOffset"), 0,
          columnName);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".size"), 0, columnName);
    } else {
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".startOffset"), 1,
          columnName);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".forward_index" + ".size"), 1, columnName);
    }

    if (dictionaryEnabled) {
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".startOffset"), 1, columnName);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".size"), 1, columnName);
    } else {
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".startOffset"), 0, columnName);
      assertEquals(StringUtils.countMatches(indexMapStr, columnName + ".dictionary" + ".size"), 0, columnName);
    }
  }

  private void validateMetadataProperties(String column, boolean hasDictionary, int dictionaryElementSize,
      int cardinality, int totalDocs, FieldSpec.DataType dataType, FieldSpec.FieldType fieldType, boolean isSorted,
      boolean isSingleValue, int maxNumberOfMVEntries, int totalNumberOfEntries, boolean isAutoGenerated,
      Comparable minValue, Comparable maxValue, boolean isRegeneratedMVColumnWithDuplicates)
      throws IOException {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectory);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

    assertEquals(columnMetadata.hasDictionary(), hasDictionary, column);
    assertEquals(columnMetadata.getColumnMaxLength(), dictionaryElementSize, column);
    assertEquals(columnMetadata.getCardinality(), cardinality, column);
    assertEquals(columnMetadata.getTotalDocs(), totalDocs, column);
    assertEquals(columnMetadata.getDataType(), dataType, column);
    assertEquals(columnMetadata.getFieldType(), fieldType);
    assertEquals(columnMetadata.isSorted(), isSorted);
    assertEquals(columnMetadata.isSingleValue(), isSingleValue);
    if (isRegeneratedMVColumnWithDuplicates) {
      // For MV columns with duplicates within a row, the duplicates are removed when regenerating the forward index.
      // Thus the metadata might not match depending on how many duplicates were found. Relax these metadata checks
      assertTrue(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.contains(column));
      if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.BYTES) {
        // Every entry is duplicated within the row so total number of entries matches number of docs and max number of
        // MVs per row is 1
        assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 1);
        assertEquals(columnMetadata.getTotalNumberOfEntries(), totalDocs);
      } else {
        // Cannot check for exact numbers as it will vary depending on number of entries generated per row
        assertTrue(columnMetadata.getMaxNumberOfMultiValues() <= maxNumberOfMVEntries);
        assertTrue(columnMetadata.getTotalNumberOfEntries() <= totalNumberOfEntries);
      }
    } else {
      assertEquals(columnMetadata.getMaxNumberOfMultiValues(), maxNumberOfMVEntries);
      assertEquals(columnMetadata.getTotalNumberOfEntries(), totalNumberOfEntries);
    }
    assertEquals(columnMetadata.isAutoGenerated(), isAutoGenerated);
    assertEquals(columnMetadata.getMinValue(), minValue);
    assertEquals(columnMetadata.getMaxValue(), maxValue);
  }
}
