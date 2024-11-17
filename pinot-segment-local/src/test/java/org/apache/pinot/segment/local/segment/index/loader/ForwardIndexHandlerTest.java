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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ForwardIndexHandlerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexHandlerTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);

  private static final String DIM_SNAPPY_STRING = "DIM_SNAPPY_STRING";
  private static final String DIM_PASS_THROUGH_STRING = "DIM_PASS_THROUGH_STRING";
  private static final String DIM_ZSTANDARD_STRING = "DIM_ZSTANDARD_STRING";
  private static final String DIM_LZ4_STRING = "DIM_LZ4_STRING";
  private static final String DIM_GZIP_STRING = "DIM_GZIP_STRING";

  private static final String DIM_SNAPPY_LONG = "DIM_SNAPPY_LONG";
  private static final String DIM_PASS_THROUGH_LONG = "DIM_PASS_THROUGH_LONG";
  private static final String DIM_ZSTANDARD_LONG = "DIM_ZSTANDARD_LONG";
  private static final String DIM_LZ4_LONG = "DIM_LZ4_LONG";
  private static final String DIM_GZIP_LONG = "DIM_GZIP_LONG";
  private static final String DIM_SNAPPY_INTEGER = "DIM_SNAPPY_INTEGER";
  private static final String DIM_PASS_THROUGH_INTEGER = "DIM_PASS_THROUGH_INTEGER";
  private static final String DIM_ZSTANDARD_INTEGER = "DIM_ZSTANDARD_INTEGER";
  private static final String DIM_LZ4_INTEGER = "DIM_LZ4_INTEGER";
  private static final String DIM_GZIP_INTEGER = "DIM_GZIP_INTEGER";
  private static final String DIM_SNAPPY_BYTES = "DIM_SNAPPY_BYTES";
  private static final String DIM_PASS_THROUGH_BYTES = "DIM_PASS_THROUGH_BYTES";
  private static final String DIM_ZSTANDARD_BYTES = "DIM_ZSTANDARD_BYTES";
  private static final String DIM_LZ4_BYTES = "DIM_LZ4_BYTES";
  private static final String DIM_GZIP_BYTES = "DIM_GZIP_BYTES";

  // Sorted columns
  private static final String DIM_RAW_SORTED_INTEGER = "DIM_RAW_SORTED_INTEGER";

  // Metric columns
  private static final String METRIC_PASS_THROUGH_INTEGER = "METRIC_PASS_THROUGH_INTEGER";
  private static final String METRIC_SNAPPY_INTEGER = "METRIC_SNAPPY_INTEGER";
  private static final String METRIC_ZSTANDARD_INTEGER = "METRIC_ZSTANDARD_INTEGER";
  private static final String METRIC_LZ4_INTEGER = "METRIC_LZ4_INTEGER";
  private static final String METRIC_GZIP_INTEGER = "METRIC_GZIP_INTEGER";

  private static final String METRIC_SNAPPY_BIG_DECIMAL = "METRIC_SNAPPY_BIG_DECIMAL";
  private static final String METRIC_PASS_THROUGH_BIG_DECIMAL = "METRIC_PASS_THROUGH_BIG_DECIMAL";
  private static final String METRIC_ZSTANDARD_BIG_DECIMAL = "METRIC_ZSTANDARD_BIG_DECIMAL";
  private static final String METRIC_LZ4_BIG_DECIMAL = "METRIC_LZ4_BIG_DECIMAL";
  private static final String METRIC_GZIP_BIG_DECIMAL = "METRIC_GZIP_BIG_DECIMAL";

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

  private static final List<String> RAW_SNAPPY_COLUMNS =
      List.of(DIM_SNAPPY_STRING, DIM_SNAPPY_LONG, DIM_SNAPPY_INTEGER, DIM_SNAPPY_BYTES, METRIC_SNAPPY_BIG_DECIMAL,
          METRIC_SNAPPY_INTEGER);

  private static final List<String> RAW_SORTED_COLUMNS = List.of(DIM_RAW_SORTED_INTEGER);

  private static final List<String> RAW_ZSTANDARD_COLUMNS =
      List.of(DIM_ZSTANDARD_STRING, DIM_ZSTANDARD_LONG, DIM_ZSTANDARD_INTEGER, DIM_ZSTANDARD_BYTES,
          METRIC_ZSTANDARD_BIG_DECIMAL, METRIC_ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_COLUMNS =
      List.of(DIM_PASS_THROUGH_STRING, DIM_PASS_THROUGH_LONG, DIM_PASS_THROUGH_INTEGER, DIM_PASS_THROUGH_BYTES,
          METRIC_PASS_THROUGH_BIG_DECIMAL, METRIC_PASS_THROUGH_INTEGER, DIM_MV_PASS_THROUGH_INTEGER,
          DIM_MV_PASS_THROUGH_LONG, DIM_MV_PASS_THROUGH_STRING, DIM_MV_PASS_THROUGH_BYTES);

  private static final List<String> RAW_LZ4_COLUMNS =
      List.of(DIM_LZ4_STRING, DIM_LZ4_LONG, DIM_LZ4_INTEGER, DIM_LZ4_BYTES, METRIC_LZ4_BIG_DECIMAL, METRIC_LZ4_INTEGER);

  private static final List<String> RAW_GZIP_COLUMNS =
      List.of(DIM_GZIP_STRING, DIM_GZIP_LONG, DIM_GZIP_INTEGER, DIM_GZIP_BYTES, METRIC_GZIP_BIG_DECIMAL,
          METRIC_GZIP_INTEGER);

  private static final List<String> RAW_COLUMNS_WITH_FORWARD_INDEX;

  static {
    RAW_COLUMNS_WITH_FORWARD_INDEX = new ArrayList<>();
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_SNAPPY_COLUMNS);
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_SORTED_COLUMNS);
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_ZSTANDARD_COLUMNS);
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_PASS_THROUGH_COLUMNS);
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_LZ4_COLUMNS);
    RAW_COLUMNS_WITH_FORWARD_INDEX.addAll(RAW_GZIP_COLUMNS);
  }

  private static final List<String> DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX =
      List.of(DIM_DICT_INTEGER, DIM_DICT_LONG, DIM_DICT_STRING, DIM_DICT_BYES, DIM_DICT_MV_BYTES, DIM_DICT_MV_STRING,
          DIM_DICT_MV_INTEGER, DIM_DICT_MV_LONG);

  private static final List<String> DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX =
      List.of(DIM_DICT_MV_INTEGER, DIM_DICT_MV_LONG, DIM_DICT_MV_STRING, DIM_DICT_MV_BYTES);

  private static final List<String> SV_FORWARD_INDEX_DISABLED_COLUMNS =
      List.of(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_SV_FORWARD_INDEX_DISABLED_LONG,
          DIM_SV_FORWARD_INDEX_DISABLED_STRING, DIM_SV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_COLUMNS =
      List.of(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_STRING, DIM_MV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS =
      List.of(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES);

  private static final List<String> FORWARD_INDEX_DISABLED_RAW_COLUMNS =
      List.of(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);

  private static final List<CompressionCodec> RAW_COMPRESSION_TYPES =
      Arrays.stream(CompressionCodec.values()).filter(CompressionCodec::isApplicableToRawIndex)
          .collect(Collectors.toList());

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(DIM_SNAPPY_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_PASS_THROUGH_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_ZSTANDARD_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_LZ4_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_GZIP_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_SNAPPY_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_RAW_SORTED_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_ZSTANDARD_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_PASS_THROUGH_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_LZ4_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_GZIP_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_SNAPPY_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_ZSTANDARD_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_PASS_THROUGH_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_LZ4_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_GZIP_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_SNAPPY_BYTES, DataType.BYTES)
      .addSingleValueDimension(DIM_PASS_THROUGH_BYTES, DataType.BYTES)
      .addSingleValueDimension(DIM_ZSTANDARD_BYTES, DataType.BYTES)
      .addSingleValueDimension(DIM_LZ4_BYTES, DataType.BYTES)
      .addSingleValueDimension(DIM_GZIP_BYTES, DataType.BYTES)
      .addMetric(METRIC_SNAPPY_BIG_DECIMAL, DataType.BIG_DECIMAL)
      .addMetric(METRIC_PASS_THROUGH_BIG_DECIMAL, DataType.BIG_DECIMAL)
      .addMetric(METRIC_ZSTANDARD_BIG_DECIMAL, DataType.BIG_DECIMAL)
      .addMetric(METRIC_LZ4_BIG_DECIMAL, DataType.BIG_DECIMAL)
      .addMetric(METRIC_GZIP_BIG_DECIMAL, DataType.BIG_DECIMAL)
      .addSingleValueDimension(DIM_DICT_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_DICT_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_DICT_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_DICT_BYES, DataType.BYTES)
      .addMetric(METRIC_PASS_THROUGH_INTEGER, DataType.INT)
      .addMetric(METRIC_SNAPPY_INTEGER, DataType.INT).addMetric(METRIC_LZ4_INTEGER, DataType.INT)
      .addMetric(METRIC_GZIP_INTEGER, DataType.INT)
      .addMetric(METRIC_ZSTANDARD_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_MV_PASS_THROUGH_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_MV_PASS_THROUGH_LONG, DataType.LONG)
      .addMultiValueDimension(DIM_MV_PASS_THROUGH_STRING, DataType.STRING)
      .addMultiValueDimension(DIM_MV_PASS_THROUGH_BYTES, DataType.BYTES)
      .addMultiValueDimension(DIM_DICT_MV_BYTES, DataType.BYTES)
      .addMultiValueDimension(DIM_DICT_MV_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_DICT_MV_LONG, DataType.LONG)
      .addMultiValueDimension(DIM_DICT_MV_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_LONG, DataType.LONG)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_STRING, DataType.STRING)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_BYTES, DataType.BYTES)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_LONG, DataType.LONG)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_STRING, DataType.STRING)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_BYTES, DataType.BYTES)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG, DataType.LONG)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, DataType.STRING)
      .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES, DataType.BYTES)
      .addSingleValueDimension(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, DataType.INT)
      .addMultiValueDimension(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, DataType.INT)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, DataType.INT)
      .addSingleValueDimension(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX, DataType.INT)
      .build();
  //@formatter:on

  private static final Random RANDOM = new Random();

  private static final List<GenericRow> TEST_DATA;

  static {
    // Generate RANDOM data
    int numRows = 1000;
    TEST_DATA = new ArrayList<>(numRows);

    String[] tempStringRows = new String[numRows];
    Integer[] tempIntRows = new Integer[numRows];
    Long[] tempLongRows = new Long[numRows];
    byte[][] tempBytesRows = new byte[numRows][];
    BigDecimal[] tempBigDecimalRows = new BigDecimal[numRows];

    int maxNumberOfMVEntries = RANDOM.nextInt(500) + 1;
    String[][] tempMVStringRows = new String[numRows][maxNumberOfMVEntries];
    Integer[][] tempMVIntRows = new Integer[numRows][maxNumberOfMVEntries];
    Long[][] tempMVLongRows = new Long[numRows][maxNumberOfMVEntries];
    byte[][][] tempMVByteRows = new byte[numRows][maxNumberOfMVEntries][];

    // For MV columns today adding duplicate entries within the same row will result in the total number of MV entries
    // reducing for that row since we cannot support rebuilding the forward index without losing duplicates within a
    // row today.
    String[][] tempMVStringRowsForwardIndexDisabled = new String[numRows][maxNumberOfMVEntries];
    Integer[][] tempMVIntRowsForwardIndexDisabled = new Integer[numRows][maxNumberOfMVEntries];
    Long[][] tempMVLongRowsForwardIndexDisabled = new Long[numRows][maxNumberOfMVEntries];
    byte[][][] tempMVByteRowsForwardIndexDisabled = new byte[numRows][maxNumberOfMVEntries][];

    for (int i = 0; i < numRows; i++) {
      // Adding a fixed value to check for filter queries
      if (i % 10 == 0) {
        String str = "testRow";
        tempStringRows[i] = str;
        tempIntRows[i] = 1001;
        tempLongRows[i] = 1001L;
        tempBytesRows[i] = str.getBytes();
        tempBigDecimalRows[i] = BigDecimal.valueOf(1001);

        // Avoid creating empty arrays.
        int numMVElements = RANDOM.nextInt(maxNumberOfMVEntries) + 1;
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
        int numMVElements = RANDOM.nextInt(maxNumberOfMVEntries) + 1;
        for (int j = 0; j < numMVElements; j++) {
          tempMVIntRows[i][j] = j;
          tempMVLongRows[i][j] = (long) j;
          tempMVStringRows[i][j] = str;
          tempMVByteRows[i][j] = str.getBytes();
        }
      }

      // Populate data for the MV columns with forward index disabled to have unique entries per row.
      // Avoid creating empty arrays.
      int numMVElements = RANDOM.nextInt(maxNumberOfMVEntries) + 1;
      for (int j = 0; j < numMVElements; j++) {
        String str = "n" + i + j;
        tempMVIntRowsForwardIndexDisabled[i][j] = j;
        tempMVLongRowsForwardIndexDisabled[i][j] = (long) j;
        tempMVStringRowsForwardIndexDisabled[i][j] = str;
        tempMVByteRowsForwardIndexDisabled[i][j] = str.getBytes();
      }
    }

    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();

      // Raw String columns
      row.putValue(DIM_SNAPPY_STRING, tempStringRows[i]);
      row.putValue(DIM_ZSTANDARD_STRING, tempStringRows[i]);
      row.putValue(DIM_PASS_THROUGH_STRING, tempStringRows[i]);
      row.putValue(DIM_LZ4_STRING, tempStringRows[i]);
      row.putValue(DIM_GZIP_STRING, tempStringRows[i]);

      // Raw integer columns
      row.putValue(DIM_SNAPPY_INTEGER, tempIntRows[i]);
      row.putValue(DIM_ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(DIM_PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(DIM_LZ4_INTEGER, tempIntRows[i]);
      row.putValue(DIM_GZIP_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_LZ4_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_GZIP_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(METRIC_SNAPPY_INTEGER, tempIntRows[i]);
      row.putValue(DIM_RAW_SORTED_INTEGER, i);

      // Raw long columns
      row.putValue(DIM_SNAPPY_LONG, tempLongRows[i]);
      row.putValue(DIM_ZSTANDARD_LONG, tempLongRows[i]);
      row.putValue(DIM_PASS_THROUGH_LONG, tempLongRows[i]);
      row.putValue(DIM_LZ4_LONG, tempLongRows[i]);
      row.putValue(DIM_GZIP_LONG, tempLongRows[i]);

      // Raw Byte columns
      row.putValue(DIM_SNAPPY_BYTES, tempBytesRows[i]);
      row.putValue(DIM_ZSTANDARD_BYTES, tempBytesRows[i]);
      row.putValue(DIM_PASS_THROUGH_BYTES, tempBytesRows[i]);
      row.putValue(DIM_LZ4_BYTES, tempBytesRows[i]);
      row.putValue(DIM_GZIP_BYTES, tempBytesRows[i]);

      // Raw BigDecimal column
      row.putValue(METRIC_SNAPPY_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_ZSTANDARD_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_PASS_THROUGH_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_LZ4_BIG_DECIMAL, tempBigDecimalRows[i]);
      row.putValue(METRIC_GZIP_BIG_DECIMAL, tempBigDecimalRows[i]);

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

      TEST_DATA.add(row);
    }
  }

  private Set<String> _noDictionaryColumns;
  private Set<String> _invertedIndexColumns;
  private Set<String> _rangeIndexColumns;
  private Map<String, FieldConfig> _fieldConfigMap;

  private SegmentDirectory _segmentDirectory;
  private SegmentDirectory.Writer _writer;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    buildSegment();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private void buildSegment()
      throws Exception {
    resetIndexConfigs();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(createTableConfig(), SCHEMA);
    config.setOutDir(TEMP_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(TEST_DATA));
    driver.build();
  }

  private void resetIndexConfigs() {
    _noDictionaryColumns = new HashSet<>();
    _noDictionaryColumns.addAll(RAW_COLUMNS_WITH_FORWARD_INDEX);
    _noDictionaryColumns.addAll(FORWARD_INDEX_DISABLED_RAW_COLUMNS);

    _invertedIndexColumns = new HashSet<>();
    _invertedIndexColumns.addAll(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    _invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    _invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);

    _rangeIndexColumns = new HashSet<>();
    _rangeIndexColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);

    _fieldConfigMap = new HashMap<>();
    for (String column : RAW_SNAPPY_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
    }
    for (String column : RAW_SORTED_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.SORTED),
              CompressionCodec.SNAPPY, null));
    }
    for (String column : RAW_ZSTANDARD_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));
    }
    for (String column : RAW_PASS_THROUGH_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.PASS_THROUGH, null));
    }
    for (String column : RAW_LZ4_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));
    }
    for (String column : RAW_GZIP_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.GZIP, null));
    }
    for (String column : SV_FORWARD_INDEX_DISABLED_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    }
    for (String column : MV_FORWARD_INDEX_DISABLED_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    }
    for (String column : MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    }
    for (String column : FORWARD_INDEX_DISABLED_RAW_COLUMNS) {
      _fieldConfigMap.put(column, new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4,
          Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    }
    _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX,
        new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, FieldConfig.EncodingType.DICTIONARY,
            List.of(), null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX,
        new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.RANGE), null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
  }

  private TableConfig createTableConfig() {
    return createTableConfig(_noDictionaryColumns, _invertedIndexColumns, _rangeIndexColumns, _fieldConfigMap);
  }

  private TableConfig createTableConfig(Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      Set<String> rangeIndexColumns, Map<String, FieldConfig> fieldConfigMap) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(new ArrayList<>(noDictionaryColumns))
        .setInvertedIndexColumns(new ArrayList<>(invertedIndexColumns))
        .setCreateInvertedIndexDuringSegmentGeneration(true).setRangeIndexColumns(new ArrayList<>(rangeIndexColumns))
        .setFieldConfigList(new ArrayList<>(fieldConfigMap.values())).build();
  }

  @Test
  public void testComputeOperationNoOp()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Validate with zero changes. ForwardIndexHandler should be a No-Op.
      assertTrue(computeOperations().isEmpty());
    }
  }

  private IndexLoadingConfig createIndexLoadingConfig() {
    return new IndexLoadingConfig(createTableConfig(), SCHEMA);
  }

  private ForwardIndexHandler createForwardIndexHandler() {
    return new ForwardIndexHandler(_segmentDirectory, createIndexLoadingConfig(), SCHEMA);
  }

  private Map<String, List<ForwardIndexHandler.Operation>> computeOperations()
      throws Exception {
    return createForwardIndexHandler().computeOperations(_writer);
  }

  private void updateIndices()
      throws Exception {
    ForwardIndexHandler handler = createForwardIndexHandler();
    handler.updateIndices(_writer);
    handler.postUpdateIndicesCleanup(_writer);
  }

  @Test
  public void testComputeOperationEnableDictionary()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN.
      _noDictionaryColumns.remove(DIM_ZSTANDARD_STRING);
      _fieldConfigMap.remove(DIM_ZSTANDARD_STRING);
      assertEquals(computeOperations(),
          Map.of(DIM_ZSTANDARD_STRING, List.of(ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));

      // TEST2: Enable dictionary for an MV column.
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_MV_PASS_THROUGH_STRING);
      _fieldConfigMap.remove(DIM_MV_PASS_THROUGH_STRING);
      assertEquals(computeOperations(),
          Map.of(DIM_MV_PASS_THROUGH_STRING, List.of(ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));

      // TEST3: Add text index. ForwardIndexHandler should be a No-Op.
      resetIndexConfigs();
      _fieldConfigMap.put(DIM_DICT_STRING,
          new FieldConfig(DIM_DICT_STRING, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.TEXT),
              null, null));
      _fieldConfigMap.put(DIM_LZ4_STRING,
          new FieldConfig(DIM_LZ4_STRING, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
              CompressionCodec.LZ4, null));
      assertTrue(computeOperations().isEmpty());

      // TEST4: Add range index and enable dictionary.
      resetIndexConfigs();
      _noDictionaryColumns.remove(METRIC_LZ4_INTEGER);
      _rangeIndexColumns.add(METRIC_LZ4_INTEGER);
      _fieldConfigMap.remove(METRIC_LZ4_INTEGER);
      assertEquals(computeOperations(),
          Map.of(METRIC_LZ4_INTEGER, List.of(ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));

      // TEST5: Enable Dictionary for sorted column.
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_RAW_SORTED_INTEGER);
      _fieldConfigMap.remove(DIM_RAW_SORTED_INTEGER);
      assertEquals(computeOperations(),
          Map.of(DIM_RAW_SORTED_INTEGER, List.of(ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));
    }
  }

  @Test
  public void testComputeOperationDisableDictionary()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Disable dictionary for a dictionary SV column.
      _noDictionaryColumns.add(DIM_DICT_INTEGER);
      assertEquals(computeOperations(),
          Map.of(DIM_DICT_INTEGER, List.of(ForwardIndexHandler.Operation.DISABLE_DICTIONARY)));

      // TEST2: Disable dictionary for a dictionary MV column.
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_DICT_MV_BYTES);
      assertEquals(computeOperations(),
          Map.of(DIM_DICT_MV_BYTES, List.of(ForwardIndexHandler.Operation.DISABLE_DICTIONARY)));

      // TEST3: Disable dictionary and enable inverted index. Should be a no-op.
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_DICT_STRING);
      _invertedIndexColumns.add(DIM_DICT_STRING);
      assertTrue(computeOperations().isEmpty());
    }
  }

  @Test
  public void testComputeOperationChangeCompression()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Change compression
      // Only try to change compression type for forward index enabled columns
      String column = RAW_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(RAW_COLUMNS_WITH_FORWARD_INDEX.size()));
      CompressionCodec compressionCodec = _fieldConfigMap.get(column).getCompressionCodec();
      CompressionCodec newCompressionCodec = null;
      for (CompressionCodec codec : CompressionCodec.values()) {
        if (compressionCodec != codec) {
          newCompressionCodec = codec;
          break;
        }
      }
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), newCompressionCodec, null));
      assertEquals(computeOperations(),
          Map.of(column, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));

      // TEST2: Change compression and add index. Change compressionType for more than 1 column.
      resetIndexConfigs();
      _rangeIndexColumns.add(DIM_SNAPPY_INTEGER);
      _fieldConfigMap.put(DIM_SNAPPY_INTEGER,
          new FieldConfig(DIM_SNAPPY_INTEGER, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.RANGE),
              CompressionCodec.ZSTANDARD, null));
      _fieldConfigMap.put(DIM_SNAPPY_STRING,
          new FieldConfig(DIM_SNAPPY_STRING, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
              CompressionCodec.ZSTANDARD, null));
      assertEquals(computeOperations(),
          Map.of(DIM_SNAPPY_INTEGER, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE),
              DIM_SNAPPY_STRING, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
    }
  }

  @Test
  public void testComputeOperationDisableForwardIndex()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Disable forward index for a dictionary column with forward index enabled
      _invertedIndexColumns.add(DIM_DICT_INTEGER);
      _fieldConfigMap.put(DIM_DICT_INTEGER, new FieldConfig(DIM_DICT_INTEGER, FieldConfig.EncodingType.DICTIONARY,
          List.of(FieldConfig.IndexType.INVERTED), null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(),
          Map.of(DIM_DICT_INTEGER, List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX)));

      // TEST2: Disable forward index for a raw column with forward index enabled and enable inverted index and
      // dictionary
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_LZ4_INTEGER);
      _invertedIndexColumns.add(DIM_LZ4_INTEGER);
      _fieldConfigMap.put(DIM_LZ4_INTEGER,
          new FieldConfig(DIM_LZ4_INTEGER, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(), Map.of(DIM_LZ4_INTEGER,
          List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
              ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));

      // TEST3: Disable forward index for two dictionary columns with forward index enabled
      resetIndexConfigs();
      _invertedIndexColumns.add(DIM_DICT_LONG);
      _invertedIndexColumns.add(DIM_DICT_STRING);
      _fieldConfigMap.put(DIM_DICT_LONG,
          new FieldConfig(DIM_DICT_LONG, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      _fieldConfigMap.put(DIM_DICT_STRING,
          new FieldConfig(DIM_DICT_STRING, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(),
          Map.of(DIM_DICT_LONG, List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX), DIM_DICT_STRING,
              List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX)));

      // TEST4: Disable forward index for two raw columns with forward index enabled and enable dictionary
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_LZ4_LONG);
      _noDictionaryColumns.remove(DIM_SNAPPY_STRING);
      _invertedIndexColumns.add(DIM_LZ4_LONG);
      _invertedIndexColumns.add(DIM_SNAPPY_STRING);
      _fieldConfigMap.put(DIM_LZ4_LONG,
          new FieldConfig(DIM_LZ4_LONG, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      _fieldConfigMap.put(DIM_SNAPPY_STRING, new FieldConfig(DIM_SNAPPY_STRING, FieldConfig.EncodingType.DICTIONARY,
          List.of(FieldConfig.IndexType.INVERTED), null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(), Map.of(DIM_LZ4_LONG,
          List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX, ForwardIndexHandler.Operation.ENABLE_DICTIONARY),
          DIM_SNAPPY_STRING, List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
              ForwardIndexHandler.Operation.ENABLE_DICTIONARY)));

      // TEST5: Disable forward index for a dictionary and a raw column with forward index enabled
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_ZSTANDARD_INTEGER);
      _invertedIndexColumns.add(DIM_ZSTANDARD_INTEGER);
      _invertedIndexColumns.add(DIM_DICT_STRING);
      _fieldConfigMap.put(DIM_ZSTANDARD_INTEGER,
          new FieldConfig(DIM_ZSTANDARD_INTEGER, FieldConfig.EncodingType.DICTIONARY,
              List.of(FieldConfig.IndexType.INVERTED), null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      _fieldConfigMap.put(DIM_DICT_STRING,
          new FieldConfig(DIM_DICT_STRING, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(), Map.of(DIM_ZSTANDARD_INTEGER,
          List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX, ForwardIndexHandler.Operation.ENABLE_DICTIONARY),
          DIM_DICT_STRING, List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX)));

      // TEST6: Disable forward index for a raw column without enabling dictionary or inverted index
      resetIndexConfigs();
      _fieldConfigMap.put(DIM_LZ4_INTEGER,
          new FieldConfig(DIM_LZ4_INTEGER, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(),
          Map.of(DIM_LZ4_INTEGER, List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX)));

      // TEST7: Disable forward index for a dictionary column and also disable dictionary and inverted index
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_DICT_LONG);
      _fieldConfigMap.put(DIM_DICT_LONG,
          new FieldConfig(DIM_DICT_LONG, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(), Map.of(DIM_DICT_LONG,
          List.of(ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX,
              ForwardIndexHandler.Operation.DISABLE_DICTIONARY)));

      // TEST8: Disable dictionary on a column that already has forward index disabled
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(),
          Map.of(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, List.of(ForwardIndexHandler.Operation.DISABLE_DICTIONARY)));

      // TEST9: Disable inverted index on a column that already has forward index disabled
      resetIndexConfigs();
      _invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertTrue(computeOperations().isEmpty());

      // TEST10: Disable dictionary on a column that already has forward index disabled and inverted index disabled
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, FieldConfig.EncodingType.RAW,
              List.of(), CompressionCodec.LZ4, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      assertEquals(computeOperations(), Map.of(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX,
          List.of(ForwardIndexHandler.Operation.DISABLE_DICTIONARY)));

      // TEST11: Enable dictionary on a column that already has forward index disabled and dictionary disabled
      resetIndexConfigs();
      _noDictionaryColumns.remove(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _fieldConfigMap.put(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.DICTIONARY, List.of(),
              null, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      try {
        computeOperations();
        fail("Enabling dictionary on forward index disabled column is not possible");
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Cannot regenerate the dictionary for column: "
            + "DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER of segment: testSegment with forward index disabled. Please "
            + "refresh or back-fill the data to add back the forward index");
      }

      // TEST12: Disable dictionary on a column that already has forward index disabled without an inverted index but
      // with a range index
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX, FieldConfig.EncodingType.RAW,
              List.of(FieldConfig.IndexType.RANGE), CompressionCodec.LZ4,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      try {
        computeOperations();
        fail("Disabling dictionary on forward index disabled column without inverted index but which has a "
            + "range index is not possible");
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Must disable range index (enabled) to disable the dictionary for a "
            + "forwardIndexDisabled column: DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX of segment: "
            + "testSegment or refresh / back-fill the forward index");
      }

      // TEST13: Disable dictionary on a column that already has forward index disabled and inverted index enabled with
      // a range index
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _rangeIndexColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.RAW,
              List.of(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE), CompressionCodec.LZ4,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      try {
        computeOperations();
        fail("Disabling dictionary on forward index disabled column with inverted index and a range index "
            + "is not possible");
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Must disable range index (enabled) to disable the dictionary for a "
            + "forwardIndexDisabled column: DIM_SV_FORWARD_INDEX_DISABLED_INTEGER of segment: testSegment or refresh "
            + "/ back-fill the forward index");
      }
    }
  }

  @Test
  public void testComputeOperationEnableForwardIndex()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // TEST1: Try to change compression type for a forward index disabled column and enable forward index for it
      List<String> forwardIndexDisabledColumns = new ArrayList<>();
      forwardIndexDisabledColumns.addAll(SV_FORWARD_INDEX_DISABLED_COLUMNS);
      forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
      String column = forwardIndexDisabledColumns.get(RANDOM.nextInt(forwardIndexDisabledColumns.size()));
      _noDictionaryColumns.add(column);
      _invertedIndexColumns.remove(column);
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));
      assertEquals(computeOperations(), Map.of(column, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST2: Enable forward index in dictionary format for a column with forward index disabled
      resetIndexConfigs();
      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_BYTES);
      assertEquals(computeOperations(),
          Map.of(DIM_SV_FORWARD_INDEX_DISABLED_BYTES, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST3: Enable forward index in raw format for a column with forward index disabled. Remove column from inverted
      // index as well (inverted index needs dictionary)
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
      _invertedIndexColumns.remove(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
      _fieldConfigMap.put(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, null));
      assertEquals(computeOperations(),
          Map.of(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST4: Enable forward index in dictionary format for two columns with forward index disabled. Disable inverted
      // index for one of them
      resetIndexConfigs();
      _invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
      _fieldConfigMap.remove(DIM_MV_FORWARD_INDEX_DISABLED_STRING);
      assertEquals(computeOperations(),
          Map.of(DIM_SV_FORWARD_INDEX_DISABLED_LONG, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX),
              DIM_MV_FORWARD_INDEX_DISABLED_STRING, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST5: Enable forward index in raw format for two columns with forward index disabled. Remove column from
      // inverted index as well (inverted index needs dictionary)
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_STRING);
      _noDictionaryColumns.add(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
      _invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_STRING);
      _invertedIndexColumns.remove(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
      _fieldConfigMap.put(DIM_SV_FORWARD_INDEX_DISABLED_STRING,
          new FieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_STRING, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, null));
      _fieldConfigMap.put(DIM_MV_FORWARD_INDEX_DISABLED_LONG,
          new FieldConfig(DIM_MV_FORWARD_INDEX_DISABLED_LONG, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, null));
      assertEquals(computeOperations(),
          Map.of(DIM_SV_FORWARD_INDEX_DISABLED_STRING, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX),
              DIM_MV_FORWARD_INDEX_DISABLED_LONG, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST6: Enable forward index in dictionary format and one in raw format for columns with forward index disabled
      resetIndexConfigs();
      _noDictionaryColumns.add(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
      _invertedIndexColumns.remove(DIM_MV_FORWARD_INDEX_DISABLED_LONG);
      _fieldConfigMap.put(DIM_MV_FORWARD_INDEX_DISABLED_LONG,
          new FieldConfig(DIM_MV_FORWARD_INDEX_DISABLED_LONG, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, null));
      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_BYTES);
      assertEquals(computeOperations(),
          Map.of(DIM_MV_FORWARD_INDEX_DISABLED_LONG, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX),
              DIM_SV_FORWARD_INDEX_DISABLED_BYTES, List.of(ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX)));

      // TEST7: Enable forward index for a raw column with forward index disabled and keep it as raw
      resetIndexConfigs();
      _fieldConfigMap.put(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER,
          new FieldConfig(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, FieldConfig.EncodingType.RAW, List.of(),
              CompressionCodec.LZ4, null));
      assertTrue(computeOperations().isEmpty());

      // TEST8: Enable forward index for a dictionary based column with forward index and inverted index disabled
      resetIndexConfigs();
      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      assertTrue(computeOperations().isEmpty());
    }
  }

  @Test
  public void testChangeCompressionForSingleColumn()
      throws Exception {
    for (String column : RAW_COLUMNS_WITH_FORWARD_INDEX) {
      // For every noDictionaryColumn, change the compressionType to all available types, one by one.
      for (CompressionCodec compressionType : RAW_COMPRESSION_TYPES) {
        SegmentMetadataImpl existingSegmentMetadata;
        try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
            SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
          _segmentDirectory = segmentDirectory;
          _writer = writer;

          existingSegmentMetadata = segmentDirectory.getSegmentMetadata();
          FieldConfig existingFieldConfig = _fieldConfigMap.put(column,
              new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), compressionType, null));
          assertNotNull(existingFieldConfig);
          ForwardIndexHandler handler = createForwardIndexHandler();
          if (existingFieldConfig.getCompressionCodec() == compressionType) {
            assertFalse(handler.needUpdateIndices(writer));
            continue;
          }
          assertTrue(handler.needUpdateIndices(writer));
          handler.updateIndices(writer);
          handler.postUpdateIndicesCleanup(writer);
          // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
        }

        // Validation
        ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
        testIndexExists(column, StandardIndexes.forward());
        validateIndexMap(column, false, false);
        validateForwardIndex(column, compressionType, metadata.isSorted());

        // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
        // change.
        validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
            metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
            metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
            metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
            metadata.getMaxValue(), false);
      }
    }
  }

  @Test
  public void testChangeCompressionAndIndexVersion()
      throws Exception {
    List<String> columns = new ArrayList<>(RAW_SNAPPY_COLUMNS.size() + RAW_SORTED_COLUMNS.size());
    columns.addAll(RAW_SNAPPY_COLUMNS);
    columns.addAll(RAW_SORTED_COLUMNS);
    for (String column : columns) {
      // Convert from SNAPPY v2 to LZ4 v4
      SegmentMetadataImpl existingSegmentMetadata;
      FieldConfig existingFieldConfig;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;

        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();
        ForwardIndexConfig forwardIndexConfig =
            new ForwardIndexConfig.Builder().withCompressionCodec(CompressionCodec.LZ4).withRawIndexWriterVersion(4)
                .build();
        ObjectNode indexes = JsonUtils.newObjectNode();
        indexes.set("forward", forwardIndexConfig.toJsonNode());
        FieldConfig fieldConfig =
            new FieldConfig.Builder(column).withEncodingType(FieldConfig.EncodingType.RAW).withIndexes(indexes).build();
        existingFieldConfig = _fieldConfigMap.put(column, fieldConfig);
        assertNotNull(existingFieldConfig);
        ForwardIndexHandler handler = createForwardIndexHandler();
        assertTrue(handler.needUpdateIndices(writer));
        handler.updateIndices(writer);
        handler.postUpdateIndicesCleanup(writer);
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      // Validation
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      validateIndexMap(column, false, false);
      validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());

      // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
      // change.
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);

      // Convert it back
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        _fieldConfigMap.put(column, existingFieldConfig);
        ForwardIndexHandler handler = createForwardIndexHandler();
        assertTrue(handler.needUpdateIndices(writer));
        handler.updateIndices(writer);
        handler.postUpdateIndicesCleanup(writer);
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      // Validation
      metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      validateIndexMap(column, false, false);
      validateForwardIndex(column, CompressionCodec.SNAPPY, metadata.isSorted());

      // Validate metadata properties. Nothing should change when a forwardIndex is rewritten for compressionType
      // change.
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testChangeDictCompression()
      throws Exception {
    // Change to MV_ENTRY_DICT compression
    for (String column : DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX) {
      _fieldConfigMap.put(column,
          new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(), CompressionCodec.MV_ENTRY_DICT,
              null));

      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;

        ForwardIndexHandler handler = createForwardIndexHandler();
        assertEquals(handler.computeOperations(writer),
            Map.of(column, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
        assertTrue(handler.needUpdateIndices(writer));
        handler.updateIndices(writer);
        handler.postUpdateIndicesCleanup(writer);
      }

      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
        ForwardIndexReader<?> forwardIndexReader =
            ForwardIndexType.read(reader, segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column));
        assertTrue(forwardIndexReader.isDictionaryEncoded());
        assertFalse(forwardIndexReader.isSingleValue());
        assertEquals(forwardIndexReader.getDictIdCompressionType(), DictIdCompressionType.MV_ENTRY_DICT);
      }
    }

    // Change back to regular forward index
    for (String column : DICT_ENABLED_MV_COLUMNS_WITH_FORWARD_INDEX) {
      _fieldConfigMap.remove(column);

      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;

        ForwardIndexHandler handler = createForwardIndexHandler();
        assertEquals(handler.computeOperations(writer),
            Map.of(column, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
        assertTrue(handler.needUpdateIndices(writer));
        handler.updateIndices(writer);
        handler.postUpdateIndicesCleanup(writer);
      }

      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
        ForwardIndexReader<?> forwardIndexReader =
            ForwardIndexType.read(reader, segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column));
        assertTrue(forwardIndexReader.isDictionaryEncoded());
        assertFalse(forwardIndexReader.isSingleValue());
        assertNull(forwardIndexReader.getDictIdCompressionType());
      }
    }
  }

  @Test
  public void testChangeCompressionForMultipleColumns()
      throws Exception {
    String column1 = RAW_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(RAW_COLUMNS_WITH_FORWARD_INDEX.size()));
    String column2 = RAW_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(RAW_COLUMNS_WITH_FORWARD_INDEX.size()));
    CompressionCodec newCompressionType = RAW_COMPRESSION_TYPES.get(RANDOM.nextInt(RAW_COMPRESSION_TYPES.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.put(column1,
          new FieldConfig(column1, FieldConfig.EncodingType.RAW, List.of(), newCompressionType, null));
      _fieldConfigMap.put(column2,
          new FieldConfig(column2, FieldConfig.EncodingType.RAW, List.of(), newCompressionType, null));
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

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
  public void testEnableDictionaryForSingleColumn()
      throws Exception {
    for (String column : RAW_COLUMNS_WITH_FORWARD_INDEX) {
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _noDictionaryColumns.remove(column);
        _fieldConfigMap.remove(column);
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      testIndexExists(column, StandardIndexes.forward());
      testIndexExists(column, StandardIndexes.dictionary());
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null, metadata.isSorted());

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      DataType dataType = metadata.getDataType();
      if (dataType == DataType.STRING || dataType == DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableDictionaryForMultipleColumns()
      throws Exception {
    String column1 = RAW_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(RAW_COLUMNS_WITH_FORWARD_INDEX.size()));
    String column2 = RAW_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(RAW_COLUMNS_WITH_FORWARD_INDEX.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _noDictionaryColumns.remove(column1);
      _noDictionaryColumns.remove(column2);
      _fieldConfigMap.remove(column1);
      _fieldConfigMap.remove(column2);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    testIndexExists(column1, StandardIndexes.forward());
    testIndexExists(column1, StandardIndexes.dictionary());
    validateIndexMap(column1, true, false);
    validateForwardIndex(column1, null, metadata.isSorted());
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    int dictionaryElementSize = 0;
    DataType dataType = metadata.getDataType();
    if (dataType == DataType.STRING || dataType == DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(column1, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    testIndexExists(column2, StandardIndexes.forward());
    testIndexExists(column2, StandardIndexes.dictionary());
    validateIndexMap(column2, true, false);
    validateForwardIndex(column2, null, metadata.isSorted());
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    dictionaryElementSize = 0;
    dataType = metadata.getDataType();
    if (dataType == DataType.STRING || dataType == DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(column2, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testDisableForwardIndexForSingleDictColumn()
      throws Exception {
    for (String column : DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX) {
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _fieldConfigMap.put(column, new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

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
  public void testDisableForwardIndexForMultipleDictColumns()
      throws Exception {
    String column1 =
        DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    String column2 =
        DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.put(column1, new FieldConfig(column1, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
          Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      _fieldConfigMap.put(column2, new FieldConfig(column2, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
          Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Col1 validation.
    validateIndexMap(column1, true, true);
    validateIndexesForForwardIndexDisabledColumns(column1);
    // In column metadata, nothing should change.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    validateMetadataProperties(column1, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);

    // Col2 validation.
    validateIndexMap(column2, true, true);
    validateIndexesForForwardIndexDisabledColumns(column2);
    // In column metadata, nothing should change.
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    validateMetadataProperties(column2, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
  }

  @Test
  public void testDisableDictionaryForSingleColumn()
      throws Exception {
    for (String column : DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX) {
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _noDictionaryColumns.add(column);
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

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
    String column1 =
        DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    String column2 =
        DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.get(RANDOM.nextInt(DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _noDictionaryColumns.add(column1);
      _noDictionaryColumns.add(column2);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

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
  public void testDisableForwardIndexForSingleRawColumn()
      throws Exception {
    for (String column : RAW_COLUMNS_WITH_FORWARD_INDEX) {
      if (RAW_SORTED_COLUMNS.contains(column)) {
        // Skip sorted columns because forward index is also used as inverted index.
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _noDictionaryColumns.remove(column);
        _invertedIndexColumns.add(column);
        _fieldConfigMap.put(column,
            new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
                Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      int dictionaryElementSize = 0;
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      DataType dataType = metadata.getDataType();
      if (dataType == DataType.STRING || dataType == DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexForMultipleRawColumns()
      throws Exception {
    String column1 = RAW_LZ4_COLUMNS.get(RANDOM.nextInt(RAW_LZ4_COLUMNS.size()));
    String column2 = RAW_SNAPPY_COLUMNS.get(RANDOM.nextInt(RAW_SNAPPY_COLUMNS.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _noDictionaryColumns.remove(column1);
      _noDictionaryColumns.remove(column2);
      _invertedIndexColumns.add(column1);
      _invertedIndexColumns.add(column2);
      _fieldConfigMap.put(column1,
          new FieldConfig(column1, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      _fieldConfigMap.put(column2,
          new FieldConfig(column2, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.INVERTED), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Col1 validation.
    validateIndexMap(column1, true, true);
    validateIndexesForForwardIndexDisabledColumns(column1);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    int dictionaryElementSize = 0;
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    DataType dataType = metadata.getDataType();
    if (dataType == DataType.STRING || dataType == DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(column1, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    validateIndexMap(column2, true, true);
    validateIndexesForForwardIndexDisabledColumns(column2);
    // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
    dictionaryElementSize = 0;
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    dataType = metadata.getDataType();
    if (dataType == DataType.STRING || dataType == DataType.BYTES) {
      // This value is based on the rows in createTestData().
      dictionaryElementSize = 7;
    } else if (dataType == DataType.BIG_DECIMAL) {
      dictionaryElementSize = 4;
    }
    validateMetadataProperties(column2, true, dictionaryElementSize, metadata.getCardinality(), metadata.getTotalDocs(),
        dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testDisableForwardIndexForRawAndInvertedIndexDisabledColumns()
      throws Exception {
    for (String column : RAW_COLUMNS_WITH_FORWARD_INDEX) {
      if (RAW_SORTED_COLUMNS.contains(column)) {
        // Skip sorted columns because forward index is also used as inverted index.
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _fieldConfigMap.put(column, new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      validateIndexMap(column, false, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      DataType dataType = metadata.getDataType();
      validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(), dataType,
          metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexForInvertedIndexDisabledColumns()
      throws Exception {
    for (String column : RAW_COLUMNS_WITH_FORWARD_INDEX) {
      if (RAW_SORTED_COLUMNS.contains(column)) {
        // Skip sorted columns because forward index is also used as inverted index.
        continue;
      }
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _noDictionaryColumns.remove(column);
        _fieldConfigMap.put(column, new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);

      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      DataType dataType = metadata.getDataType();
      int dictionaryElementSize = 0;
      if (dataType == DataType.STRING || dataType == DataType.BYTES) {
        // This value is based on the rows in createTestData().
        dictionaryElementSize = 7;
      } else if (dataType == DataType.BIG_DECIMAL) {
        dictionaryElementSize = 4;
      }
      validateMetadataProperties(column, true, dictionaryElementSize, metadata.getCardinality(),
          metadata.getTotalDocs(), dataType, metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexInDictModeForSingleForwardIndexDisabledColumn()
      throws Exception {
    List<String> forwardIndexDisabledColumns = new ArrayList<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    for (String column : forwardIndexDisabledColumns) {
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _fieldConfigMap.remove(column);
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

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
  public void testEnableForwardIndexInDictModeForMultipleForwardIndexDisabledColumns()
      throws Exception {
    String column1 = SV_FORWARD_INDEX_DISABLED_COLUMNS.get(RANDOM.nextInt(SV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    String column2 = MV_FORWARD_INDEX_DISABLED_COLUMNS.get(RANDOM.nextInt(MV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.remove(column1);
      _fieldConfigMap.remove(column2);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    validateIndexMap(column1, true, false);
    validateForwardIndex(column1, null, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(column1, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    validateIndexMap(column2, true, false);
    validateForwardIndex(column2, null, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(column2, metadata.hasDictionary(), metadata.getColumnMaxLength(),
        metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
        metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
  }

  @Test
  public void testEnableForwardIndexInDictModeForMVForwardIndexDisabledColumnWithDuplicates()
      throws Exception {
    String column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.get(
        RANDOM.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.remove(column);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

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
  public void testEnableForwardIndexInRawModeForMultipleForwardIndexDisabledColumns()
      throws Exception {
    String column1 = SV_FORWARD_INDEX_DISABLED_COLUMNS.get(RANDOM.nextInt(SV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    String column2 = MV_FORWARD_INDEX_DISABLED_COLUMNS.get(RANDOM.nextInt(MV_FORWARD_INDEX_DISABLED_COLUMNS.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _noDictionaryColumns.add(column1);
      _noDictionaryColumns.add(column2);
      _invertedIndexColumns.remove(column1);
      _invertedIndexColumns.remove(column2);
      _fieldConfigMap.remove(column1);
      _fieldConfigMap.remove(column2);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Col1 validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column1);
    validateIndexMap(column1, false, false);
    validateForwardIndex(column1, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(column1, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);

    // Col2 validation.
    metadata = existingSegmentMetadata.getColumnMetadataFor(column2);
    validateIndexMap(column2, false, false);
    validateForwardIndex(column2, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, nothing should change.
    validateMetadataProperties(column2, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), false);
  }

  @Test
  public void testEnableForwardIndexInRawModeForMVForwardIndexDisabledColumnWithDuplicates()
      throws Exception {
    String column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.get(
        RANDOM.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _noDictionaryColumns.add(column);
      _invertedIndexColumns.remove(column);
      _fieldConfigMap.remove(column);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Column validation.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
    validateIndexMap(column, false, false);
    validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());
    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), true);
  }

  @Test
  public void testEnableForwardIndexInRawModeForSingleForwardIndexDisabledColumn()
      throws Exception {
    List<String> forwardIndexDisabledColumns = new ArrayList<>(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    forwardIndexDisabledColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    for (String column : forwardIndexDisabledColumns) {
      SegmentMetadataImpl existingSegmentMetadata;
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        _segmentDirectory = segmentDirectory;
        _writer = writer;
        existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

        _noDictionaryColumns.add(column);
        _invertedIndexColumns.remove(column);
        _fieldConfigMap.remove(column);
        updateIndices();
        // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
      }

      ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
      validateIndexMap(column, false, false);
      validateForwardIndex(column, CompressionCodec.LZ4, metadata.isSorted());

      // In column metadata, nothing should change.
      validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
          metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexForInvertedIndexDisabledColumn()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Validate nothing has changed
    validateIndexMap(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, true, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);

    // In column metadata, nothing should change.
    ColumnMetadata metadata =
        existingSegmentMetadata.getColumnMetadataFor(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
    validateMetadataProperties(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
  }

  @Test
  public void testEnableForwardIndexForDictionaryDisabledColumns()
      throws Exception {
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      _fieldConfigMap.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITH_RANGE_INDEX);
      updateIndices();
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Validate nothing has changed
    validateIndexMap(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    validateIndexMap(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, false, true);
    validateIndexesForForwardIndexDisabledColumns(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);

    // In column metadata, nothing should change.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
    validateMetadataProperties(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
    metadata = existingSegmentMetadata.getColumnMetadataFor(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER);
    validateMetadataProperties(DIM_RAW_MV_FORWARD_INDEX_DISABLED_INTEGER, metadata.hasDictionary(),
        metadata.getColumnMaxLength(), metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(),
        metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
        metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(), metadata.getMaxValue(),
        false);
  }

  @Test
  public void testAddOtherIndexForForwardIndexDisabledColumn()
      throws Exception {
    String column = RANDOM.nextBoolean() ? DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING
        : DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES;
    SegmentMetadataImpl existingSegmentMetadata;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;
      existingSegmentMetadata = segmentDirectory.getSegmentMetadata();

      _rangeIndexColumns.add(column);
      IndexLoadingConfig indexLoadingConfig = createIndexLoadingConfig();
      RangeIndexHandler rangeIndexHandler = new RangeIndexHandler(segmentDirectory, indexLoadingConfig);
      rangeIndexHandler.updateIndices(writer);

      // Validate forward index exists before calling post cleanup
      validateIndexMap(column, true, false);

      rangeIndexHandler.postUpdateIndicesCleanup(writer);
      // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    }

    // Validate index map including range index. Forward index should not exist, range index and dictionary should
    validateIndexMap(column, true, true);
    String segmentDir = TEMP_DIR + "/" + SEGMENT_NAME + "/v3";
    File idxMapFile = new File(segmentDir, V1Constants.INDEX_MAP_FILE_NAME);
    String indexMapStr = FileUtils.readFileToString(idxMapFile, StandardCharsets.UTF_8);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index.startOffset"), 1, column);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index.size"), 1, column);

    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    ColumnMetadata metadata = existingSegmentMetadata.getColumnMetadataFor(column);
    validateMetadataProperties(column, true, 7, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), true);

    // Validate that expected metadata properties don't match. totalNumberOfEntries will definitely not match since
    // duplicates will be removed, but maxNumberOfMultiValues may still match if the row with max multi-values didn't
    // have any duplicates.
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
    assertNotEquals(metadata.getTotalNumberOfEntries(), columnMetadata.getTotalNumberOfEntries());
  }

  @Test
  public void testAddOtherIndexWhenForwardIndexDisabledAndInvertedIndexOrDictionaryDisabled()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      _rangeIndexColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      IndexLoadingConfig indexLoadingConfig = createIndexLoadingConfig();
      RangeIndexHandler rangeIndexHandler = new RangeIndexHandler(segmentDirectory, indexLoadingConfig);
      try {
        rangeIndexHandler.updateIndices(writer);
        fail("Creating the range index on forward index and inverted index disabled column should fail");
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Forward index disabled column "
            + "DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX must have an inverted index to regenerate the "
            + "forward index. Regeneration of the forward index is required to create new indexes as well. Please "
            + "refresh or back-fill the forward index");
      }

      _rangeIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER_WITHOUT_INV_IDX);
      _rangeIndexColumns.add(DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER);
      indexLoadingConfig = createIndexLoadingConfig();
      rangeIndexHandler = new RangeIndexHandler(segmentDirectory, indexLoadingConfig);
      try {
        rangeIndexHandler.updateIndices(writer);
        fail("Creating the range index on forward index and inverted index disabled column should fail");
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Forward index disabled column "
            + "DIM_RAW_SV_FORWARD_INDEX_DISABLED_INTEGER must have a dictionary to regenerate the forward index. "
            + "Regeneration of the forward index is required to create new indexes as well. Please refresh or "
            + "back-fill the forward index");
      }
    }
  }

  private void validateIndexesForForwardIndexDisabledColumns(String columnName)
      throws IOException, ConfigurationException {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      ColumnMetadata columnMetadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);
      if (columnMetadata.hasDictionary()) {
        assertTrue(reader.hasIndexFor(columnName, StandardIndexes.dictionary()));
        Dictionary dictionary = DictionaryIndexType.read(reader, columnMetadata);
        assertEquals(columnMetadata.getCardinality(), dictionary.length());
      } else {
        assertFalse(reader.hasIndexFor(columnName, StandardIndexes.dictionary()));
      }
      if (columnMetadata.isSorted()) {
        assertTrue(reader.hasIndexFor(columnName, StandardIndexes.forward()));
      } else {
        assertFalse(reader.hasIndexFor(columnName, StandardIndexes.forward()));
      }
    }
  }

  private void validateForwardIndex(String columnName, @Nullable CompressionCodec expectedCompressionType,
      boolean isSorted)
      throws IOException, ConfigurationException {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      validateForwardIndex(segmentDirectory, reader, columnName, expectedCompressionType, isSorted);
    }
  }

  private void validateForwardIndex(SegmentDirectory segmentDirectory, SegmentDirectory.Reader reader,
      String columnName, @Nullable CompressionCodec expectedCompressionType, boolean isSorted)
      throws IOException {
    ColumnMetadata columnMetadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);
    boolean isSingleValue = columnMetadata.isSingleValue();

    if (expectedCompressionType == null) {
      assertTrue(reader.hasIndexFor(columnName, StandardIndexes.dictionary()));
    } else {
      assertFalse(reader.hasIndexFor(columnName, StandardIndexes.dictionary()));
    }
    assertTrue(reader.hasIndexFor(columnName, StandardIndexes.forward()));

    // Check Compression type in header
    ForwardIndexReader<?> fwdIndexReader = ForwardIndexType.read(reader, columnMetadata);
    ChunkCompressionType fwdIndexCompressionType = fwdIndexReader.getCompressionType();
    if (expectedCompressionType != null) {
      assertNotNull(fwdIndexCompressionType);
      assertEquals(fwdIndexCompressionType.name(), expectedCompressionType.name());
    } else {
      assertNull(fwdIndexCompressionType);
    }

    try (ForwardIndexReader<?> forwardIndexReader = ForwardIndexType.read(reader, columnMetadata)) {
      Dictionary dictionary = null;
      if (columnMetadata.hasDictionary()) {
        dictionary = DictionaryIndexType.read(reader, columnMetadata);
      }
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(forwardIndexReader, dictionary, null,
          columnMetadata.getMaxNumberOfMultiValues());

      for (int rowIdx = 0; rowIdx < columnMetadata.getTotalDocs(); rowIdx++) {
        // For MV forward index disabled columns cannot do this validation as we had to create a unique set of elements
        if (!MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(columnName) && (rowIdx % 10 == 0)) {
          Object val = columnReader.getValue(rowIdx);
          DataType dataType = columnMetadata.getDataType();

          switch (dataType) {
            case STRING: {
              if (isSingleValue) {
                assertEquals((String) val, "testRow");
              } else {
                Object[] values = (Object[]) val;
                for (Object value : values) {
                  assertEquals((String) value, "testRow");
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
                for (Object value : values) {
                  assertEquals((int) value, 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
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
                for (Object value : values) {
                  assertEquals((long) value, 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
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
                for (Object value : values) {
                  assertEquals((byte[]) value, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
                }
              }
              break;
            }
            case BIG_DECIMAL: {
              assertTrue(isSingleValue);
              assertEquals(val, BigDecimal.valueOf(1001));
              break;
            }
            default:
              // Unreachable code.
              throw new IllegalStateException("Invalid datatype for column=" + columnName);
          }
        } else if (MV_FORWARD_INDEX_DISABLED_COLUMNS.contains(columnName)) {
          Object val = columnReader.getValue(rowIdx);
          DataType dataType = columnMetadata.getDataType();

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

  private void testIndexExists(String columnName, IndexType<?, ?, ?> indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(columnName, indexType));
    }
  }

  private void validateIndexMap(String columnName, boolean dictionaryEnabled, boolean forwardIndexDisabled)
      throws IOException {
    // Panic validation to make sure all columns have only one forward index entry in index map.
    File idxMapFile = new File(INDEX_DIR, "v3/" + V1Constants.INDEX_MAP_FILE_NAME);
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
      int cardinality, int totalDocs, DataType dataType, FieldSpec.FieldType fieldType, boolean isSorted,
      boolean isSingleValue, int maxNumberOfMVEntries, int totalNumberOfEntries, boolean isAutoGenerated,
      Comparable<?> minValue, Comparable<?> maxValue, boolean isRegeneratedMVColumnWithDuplicates)
      throws IOException, ConfigurationException {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
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
      if (dataType == DataType.STRING || dataType == DataType.BYTES) {
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
