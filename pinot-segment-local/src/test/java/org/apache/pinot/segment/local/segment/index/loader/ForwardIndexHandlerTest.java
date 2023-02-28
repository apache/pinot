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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@SuppressWarnings("rawtypes")
public class ForwardIndexHandlerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ForwardIndexHandlerTest");
  private static final String TABLE_NAME = "myTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);

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

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(DIM_SNAPPY_STRING, DIM_SNAPPY_LONG, DIM_SNAPPY_INTEGER, DIM_SNAPPY_BYTES, METRIC_SNAPPY_BIG_DECIMAL,
          METRIC_SNAPPY_INTEGER);

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

  private static final List<String> DICT_ENABLED_COLUMNS_WITH_FORWARD_INDEX =
      Arrays.asList(DIM_DICT_INTEGER, DIM_DICT_LONG, DIM_DICT_STRING, DIM_DICT_BYES, DIM_DICT_MV_BYTES,
          DIM_DICT_MV_STRING, DIM_DICT_MV_INTEGER, DIM_DICT_MV_LONG);

  private static final List<String> SV_FORWARD_INDEX_DISABLED_COLUMNS =
      Arrays.asList(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_SV_FORWARD_INDEX_DISABLED_LONG,
          DIM_SV_FORWARD_INDEX_DISABLED_STRING, DIM_SV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_COLUMNS =
      Arrays.asList(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_STRING, DIM_MV_FORWARD_INDEX_DISABLED_BYTES);

  private static final List<String> MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS =
      Arrays.asList(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG,
          DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES);

  private static final List<FieldConfig.CompressionCodec> ALL_COMPRESSION_TYPES =
      Arrays.asList(FieldConfig.CompressionCodec.values());

  private final Set<String> _invertedIndexColumns = createInvertedIndexColumns();
  private final Set<String> _rangeIndexColumns = createRangeIndexColumns();
  private final Set<String> _noDictionaryColumns = createNoDictionaryColumns();
  private final Map<String, FieldConfig> _fieldConfigs = createFieldConfigs();
  private final Schema _schema = createSchema();
  private final List<GenericRow> _testData = createTestData();

  private Set<String> createInvertedIndexColumns() {
    Set<String> invertedIndexColumns = new HashSet<>();
    invertedIndexColumns.addAll(SV_FORWARD_INDEX_DISABLED_COLUMNS);
    invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_COLUMNS);
    invertedIndexColumns.addAll(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS);
    return invertedIndexColumns;
  }

  private Set<String> createRangeIndexColumns() {
    return new HashSet<>();
  }

  private Set<String> createNoDictionaryColumns() {
    Set<String> noDictionaryColumns = new HashSet<>();
    noDictionaryColumns.addAll(RAW_SNAPPY_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_ZSTANDARD_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_PASS_THROUGH_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_LZ4_INDEX_COLUMNS);
    return noDictionaryColumns;
  }

  private Map<String, FieldConfig> createFieldConfigs() {
    Map<String, FieldConfig> fieldConfigs = new HashMap<>();
    for (String column : RAW_SNAPPY_INDEX_COLUMNS) {
      fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.SNAPPY));
    }
    for (String column : RAW_ZSTANDARD_INDEX_COLUMNS) {
      fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.ZSTANDARD));
    }
    for (String column : RAW_PASS_THROUGH_INDEX_COLUMNS) {
      fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.PASS_THROUGH));
    }
    for (String column : RAW_LZ4_INDEX_COLUMNS) {
      fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.LZ4));
    }
    for (String column : SV_FORWARD_INDEX_DISABLED_COLUMNS) {
      fieldConfigs.put(column, getForwardIndexDisabledFieldConfig(column));
    }
    for (String column : MV_FORWARD_INDEX_DISABLED_COLUMNS) {
      fieldConfigs.put(column, getForwardIndexDisabledFieldConfig(column));
    }
    for (String column : MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS) {
      fieldConfigs.put(column, getForwardIndexDisabledFieldConfig(column));
    }
    return fieldConfigs;
  }

  private static FieldConfig getRawFieldConfig(String column, FieldConfig.CompressionCodec compressionCodec) {
    return new FieldConfig(column, FieldConfig.EncodingType.RAW, Collections.emptyList(), compressionCodec, null);
  }

  private static FieldConfig getForwardIndexDisabledFieldConfig(String column) {
    return new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY,
        Collections.singletonList(FieldConfig.IndexType.INVERTED), null,
        Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.toString(true)));
  }

  private Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
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
        .addSingleValueDimension(DIM_DICT_BYES, FieldSpec.DataType.BYTES)
        .addMetric(METRIC_PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addMetric(METRIC_SNAPPY_INTEGER, FieldSpec.DataType.INT).addMetric(METRIC_LZ4_INTEGER, FieldSpec.DataType.INT)
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
        .addMultiValueDimension(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES, FieldSpec.DataType.BYTES).build();
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

      // Forward index disabled MV columns
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, tempMVIntRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_LONG, tempMVLongRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_STRING, tempMVStringRowsForwardIndexDisabled[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_BYTES, tempMVByteRowsForwardIndexDisabled[i]);

      // Forward index disabled MV columns with duplicates
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_INTEGER, tempMVIntRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_LONG, tempMVLongRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING, tempMVStringRows[i]);
      row.putValue(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES, tempMVByteRows[i]);

      rows.add(row);
    }
    return rows;
  }

  @BeforeMethod
  public void setUpSegment()
      throws Exception {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(TEMP_DIR);

    buildSegment();
  }

  @AfterMethod
  public void deleteSegment() {
    // Delete index directly if it already exists.
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private void buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(createTableConfig(), _schema);
    config.setOutDir(TEMP_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(_testData)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private static TableConfig createTableConfig(Set<String> invertedIndexColumns, Set<String> rangeIndexColumns,
      Set<String> noDictionaryColumns, Map<String, FieldConfig> fieldConfigs) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setCreateInvertedIndexDuringSegmentGeneration(true)
        .setInvertedIndexColumns(new ArrayList<>(invertedIndexColumns))
        .setRangeIndexColumns(new ArrayList<>(rangeIndexColumns))
        .setNoDictionaryColumns(new ArrayList<>(noDictionaryColumns))
        .setFieldConfigList(new ArrayList<>(fieldConfigs.values())).build();
  }

  private TableConfig createTableConfig() {
    return createTableConfig(_invertedIndexColumns, _rangeIndexColumns, _noDictionaryColumns, _fieldConfigs);
  }

  private IndexLoadingConfig createIndexLoadingConfig() {
    return new IndexLoadingConfig(createTableConfig(), _schema);
  }

  @Test
  public void testComputeOperation()
      throws Exception {
    // Setup
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    // NO-OP
    IndexLoadingConfig indexLoadingConfig = createIndexLoadingConfig();
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, indexLoadingConfig);
    Map<String, ForwardIndexHandler.Operation> operationMap = fwdIndexHandler.computeOperation(writer);
    assertTrue(operationMap.isEmpty());

    // DISABLE DICTIONARY

    // TEST1: Disable dictionary for a dictionary SV column.
    _noDictionaryColumns.add(DIM_DICT_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_DICT_INTEGER, ForwardIndexHandler.Operation.DISABLE_DICTIONARY));
    _noDictionaryColumns.remove(DIM_DICT_INTEGER);

    // TEST2: Disable dictionary for a dictionary MV column.
    _noDictionaryColumns.add(DIM_DICT_MV_BYTES);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_DICT_MV_BYTES, ForwardIndexHandler.Operation.DISABLE_DICTIONARY));
    _noDictionaryColumns.remove(DIM_DICT_MV_BYTES);

    // TEST3: Disable dictionary and enable inverted index. Should be a no-op.
    _invertedIndexColumns.add(DIM_DICT_STRING);
    _noDictionaryColumns.add(DIM_DICT_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertTrue(operationMap.isEmpty());
    _invertedIndexColumns.remove(DIM_DICT_STRING);
    _noDictionaryColumns.remove(DIM_DICT_STRING);

    // ENABLE DICTIONARY

    // TEST1: Enable dictionary for a RAW_ZSTANDARD_INDEX_COLUMN.
    _noDictionaryColumns.remove(DIM_ZSTANDARD_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_ZSTANDARD_INTEGER, ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    _noDictionaryColumns.add(DIM_ZSTANDARD_INTEGER);

    // TEST2: Enable dictionary for an MV column.
    _noDictionaryColumns.remove(DIM_MV_PASS_THROUGH_STRING);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_MV_PASS_THROUGH_STRING, ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    _noDictionaryColumns.add(DIM_MV_PASS_THROUGH_STRING);

    // TEST3: Enable dictionary for a dict column. Should be a No-op.
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertTrue(operationMap.isEmpty());

    // TEST4: Add text index. ForwardIndexHandler should be a No-Op.
    _fieldConfigs.put(DIM_DICT_INTEGER, new FieldConfig(DIM_DICT_INTEGER, FieldConfig.EncodingType.DICTIONARY,
        Collections.singletonList(FieldConfig.IndexType.TEXT), null, null));
    FieldConfig existingFieldConfig = _fieldConfigs.put(DIM_LZ4_INTEGER,
        new FieldConfig(DIM_LZ4_INTEGER, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.TEXT), FieldConfig.CompressionCodec.LZ4, null));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertTrue(operationMap.isEmpty());
    _fieldConfigs.remove(DIM_DICT_INTEGER);
    _fieldConfigs.put(DIM_LZ4_INTEGER, existingFieldConfig);

    // TEST5: Add text index and enable dictionary.
    _noDictionaryColumns.remove(METRIC_LZ4_INTEGER);
    existingFieldConfig = _fieldConfigs.put(METRIC_LZ4_INTEGER,
        new FieldConfig(METRIC_LZ4_INTEGER, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(METRIC_LZ4_INTEGER, ForwardIndexHandler.Operation.ENABLE_DICTIONARY));
    _noDictionaryColumns.add(METRIC_LZ4_INTEGER);
    _fieldConfigs.put(METRIC_LZ4_INTEGER, existingFieldConfig);

    // CHANGE COMPRESSION

    // TEST1: Change compression type
    existingFieldConfig =
        _fieldConfigs.put(DIM_LZ4_STRING, getRawFieldConfig(DIM_LZ4_STRING, FieldConfig.CompressionCodec.SNAPPY));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_LZ4_STRING, ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE));
    _fieldConfigs.put(DIM_LZ4_STRING, existingFieldConfig);

    // TEST2: Change compression and add index
    existingFieldConfig = _fieldConfigs.put(DIM_ZSTANDARD_STRING,
        new FieldConfig(DIM_ZSTANDARD_STRING, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.TEXT), FieldConfig.CompressionCodec.SNAPPY, null));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.singletonMap(DIM_ZSTANDARD_STRING,
        ForwardIndexHandler.Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE));
    _fieldConfigs.put(DIM_ZSTANDARD_STRING, existingFieldConfig);

    // DISABLE FORWARD INDEX

    // TEST1: Disable forward index for a dictionary column with forward index enabled
    _invertedIndexColumns.add(DIM_DICT_INTEGER);
    _fieldConfigs.put(DIM_DICT_INTEGER, getForwardIndexDisabledFieldConfig(DIM_DICT_INTEGER));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.singletonMap(DIM_DICT_INTEGER,
        ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX_FOR_DICT_COLUMN));
    _invertedIndexColumns.remove(DIM_DICT_INTEGER);
    _fieldConfigs.remove(DIM_DICT_INTEGER);

    // TEST2: Disable forward index for a raw column with forward index enabled and enable inverted index and
    // dictionary
    _invertedIndexColumns.add(DIM_LZ4_INTEGER);
    _noDictionaryColumns.remove(DIM_LZ4_INTEGER);
    existingFieldConfig = _fieldConfigs.put(DIM_LZ4_INTEGER, getForwardIndexDisabledFieldConfig(DIM_LZ4_INTEGER));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap,
        Collections.singletonMap(DIM_LZ4_INTEGER, ForwardIndexHandler.Operation.DISABLE_FORWARD_INDEX_FOR_RAW_COLUMN));
    _invertedIndexColumns.remove(DIM_LZ4_INTEGER);
    _noDictionaryColumns.add(DIM_LZ4_INTEGER);
    _fieldConfigs.put(DIM_LZ4_INTEGER, existingFieldConfig);

    // TEST3: Enable raw forward index for a forward index disabled column
    _invertedIndexColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    _noDictionaryColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    existingFieldConfig = _fieldConfigs.put(DIM_SV_FORWARD_INDEX_DISABLED_LONG,
        getRawFieldConfig(DIM_SV_FORWARD_INDEX_DISABLED_LONG, FieldConfig.CompressionCodec.SNAPPY));
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.singletonMap(DIM_SV_FORWARD_INDEX_DISABLED_LONG,
        ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX_FOR_RAW_COLUMN));
    _invertedIndexColumns.add(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    _noDictionaryColumns.remove(DIM_SV_FORWARD_INDEX_DISABLED_LONG);
    _fieldConfigs.put(DIM_SV_FORWARD_INDEX_DISABLED_LONG, existingFieldConfig);

    // TEST4: Enable forward index and dictionary for a forward index disabled column
    _invertedIndexColumns.remove(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    existingFieldConfig = _fieldConfigs.remove(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    operationMap = fwdIndexHandler.computeOperation(writer);
    assertEquals(operationMap, Collections.singletonMap(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER,
        ForwardIndexHandler.Operation.ENABLE_FORWARD_INDEX_FOR_DICT_COLUMN));
    _invertedIndexColumns.add(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER);
    _fieldConfigs.put(DIM_MV_FORWARD_INDEX_DISABLED_INTEGER, existingFieldConfig);

    // Tear down
    segmentDirectory.close();
  }

  @Test
  public void testDisableDictionary()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = Arrays.asList(DIM_DICT_STRING, DIM_DICT_MV_BYTES);
    for (String column : columns) {
      _noDictionaryColumns.add(column);
      _fieldConfigs.put(column, getRawFieldConfig(DIM_DICT_STRING, FieldConfig.CompressionCodec.LZ4));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (String column : columns) {
      _noDictionaryColumns.remove(column);
      _fieldConfigs.remove(column);
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      testIndexExists(column, ColumnIndexType.FORWARD_INDEX);
      validateIndexMap(column, false, false);
      validateForwardIndex(column, FieldConfig.CompressionCodec.LZ4);
      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
          metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableDictionary()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = Arrays.asList(DIM_SNAPPY_INTEGER, DIM_LZ4_LONG, DIM_ZSTANDARD_STRING, DIM_PASS_THROUGH_BYTES,
        METRIC_LZ4_BIG_DECIMAL);
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      _noDictionaryColumns.remove(column);
      existingFieldConfigs.add(_fieldConfigs.remove(column));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      _noDictionaryColumns.add(column);
      _fieldConfigs.put(column, existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      testIndexExists(column, ColumnIndexType.FORWARD_INDEX);
      testIndexExists(column, ColumnIndexType.DICTIONARY);
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null);
      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, true, getDictionaryElementSize(metadata.getDataType()),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  private int getDictionaryElementSize(FieldSpec.DataType dataType) {
    switch (dataType) {
      case STRING:
      case BYTES:
        return 7;
      case BIG_DECIMAL:
        return 4;
      default:
        return 0;
    }
  }

  @Test
  public void testChangeCompression()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns =
        Arrays.asList(DIM_PASS_THROUGH_INTEGER, DIM_LZ4_LONG, DIM_ZSTANDARD_STRING, DIM_PASS_THROUGH_BYTES,
            METRIC_LZ4_BIG_DECIMAL);
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      existingFieldConfigs.add(
          _fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.SNAPPY)));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      _fieldConfigs.put(columns.get(i), existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      testIndexExists(column, ColumnIndexType.FORWARD_INDEX);
      validateIndexMap(column, false, false);
      validateForwardIndex(column, FieldConfig.CompressionCodec.SNAPPY);
      // Nothing should change when a forwardIndex is rewritten for compressionType change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableForwardIndexWithDictionary()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = Arrays.asList(DIM_DICT_INTEGER, DIM_DICT_MV_LONG, DIM_DICT_MV_STRING, DIM_DICT_MV_BYTES);
    for (String column : columns) {
      _invertedIndexColumns.add(column);
      _fieldConfigs.put(column, getForwardIndexDisabledFieldConfig(column));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (String column : columns) {
      _invertedIndexColumns.remove(column);
      _fieldConfigs.remove(column);
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);
      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, true, getDictionaryElementSize(metadata.getDataType()),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testDisableRawForwardIndex()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns =
        Arrays.asList(DIM_SNAPPY_INTEGER, DIM_ZSTANDARD_LONG, DIM_LZ4_STRING, DIM_MV_PASS_THROUGH_BYTES);
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      _invertedIndexColumns.add(column);
      _noDictionaryColumns.remove(column);
      existingFieldConfigs.add(_fieldConfigs.put(column, getForwardIndexDisabledFieldConfig(column)));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      _invertedIndexColumns.remove(column);
      _noDictionaryColumns.add(column);
      _fieldConfigs.put(column, existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      validateIndexMap(column, true, true);
      validateIndexesForForwardIndexDisabledColumns(column);
      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, true, getDictionaryElementSize(metadata.getDataType()),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexWithDictionary()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = Arrays.asList(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_SV_FORWARD_INDEX_DISABLED_LONG,
        DIM_MV_FORWARD_INDEX_DISABLED_STRING, DIM_MV_FORWARD_INDEX_DISABLED_BYTES);
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      _invertedIndexColumns.remove(column);
      existingFieldConfigs.add(_fieldConfigs.remove(column));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      _invertedIndexColumns.add(column);
      _fieldConfigs.put(column, existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null);
      // In column metadata, nothing should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableRawForwardIndex()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = Arrays.asList(DIM_SV_FORWARD_INDEX_DISABLED_INTEGER, DIM_SV_FORWARD_INDEX_DISABLED_LONG,
        DIM_MV_FORWARD_INDEX_DISABLED_STRING, DIM_MV_FORWARD_INDEX_DISABLED_BYTES);
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      _invertedIndexColumns.remove(column);
      _noDictionaryColumns.add(column);
      existingFieldConfigs.add(_fieldConfigs.put(column, getRawFieldConfig(column, FieldConfig.CompressionCodec.LZ4)));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      _invertedIndexColumns.add(column);
      _noDictionaryColumns.remove(column);
      _fieldConfigs.put(column, existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      validateIndexMap(column, false, false);
      validateForwardIndex(column, FieldConfig.CompressionCodec.LZ4);
      // In column metadata, nothing other than hasDictionary and dictionaryElementSize should change.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, false, 0, metadata.getCardinality(), metadata.getTotalDocs(),
          metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
          metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
          metadata.getMinValue(), metadata.getMaxValue(), false);
    }
  }

  @Test
  public void testEnableForwardIndexForColumnWithDuplicates()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    List<String> columns = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS;
    List<FieldConfig> existingFieldConfigs = new ArrayList<>();
    for (String column : columns) {
      _invertedIndexColumns.remove(column);
      existingFieldConfigs.add(_fieldConfigs.remove(column));
    }
    ForwardIndexHandler fwdIndexHandler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
    fwdIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    fwdIndexHandler.postUpdateIndicesCleanup(writer);
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      _invertedIndexColumns.add(column);
      _fieldConfigs.put(column, existingFieldConfigs.get(i));
    }

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    for (String column : columns) {
      validateIndexMap(column, true, false);
      validateForwardIndex(column, null);
      // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward
      // index regeneration.
      ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validateMetadataProperties(column, metadata.hasDictionary(), metadata.getColumnMaxLength(),
          metadata.getCardinality(), metadata.getTotalDocs(), metadata.getDataType(), metadata.getFieldType(),
          metadata.isSorted(), metadata.isSingleValue(), metadata.getMaxNumberOfMultiValues(),
          metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(), metadata.getMinValue(),
          metadata.getMaxValue(), true);
    }
  }

  @Test
  public void testAddOtherIndexForForwardIndexDisabledColumn()
      throws Exception {
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();

    // Add column to range index list. Must be a numerical type.
    Random rand = new Random();
    String column;
    do {
      column = MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.get(
          rand.nextInt(MV_FORWARD_INDEX_DISABLED_DUPLICATES_COLUMNS.size()));
    } while (!column.equals(DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_STRING) && !column.equals(
        DIM_MV_FORWARD_INDEX_DISABLED_DUPLICATES_BYTES));
    int originalTotalNumberOfEntries =
        segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column).getTotalNumberOfEntries();
    _rangeIndexColumns.add(column);
    RangeIndexHandler rangeIndexHandler = new RangeIndexHandler(segmentDirectory, createIndexLoadingConfig());
    rangeIndexHandler.updateIndices(writer, IndexingOverrides.getIndexCreatorProvider());
    _rangeIndexColumns.remove(column);

    // Validate forward index exists before calling post cleanup
    validateIndexMap(column, true, false);

    rangeIndexHandler.postUpdateIndicesCleanup(writer);

    // Tear down before validation. Because columns.psf and index map cleanup happens at segmentDirectory.close()
    segmentDirectory.close();

    // Validate index map including range index. Forward index should not exist, range index and dictionary should
    validateIndexMap(column, true, true);
    File indexMapFile = SegmentDirectoryPaths.findFile(INDEX_DIR, V1Constants.INDEX_MAP_FILE_NAME);
    assertNotNull(indexMapFile);
    String indexMapStr = FileUtils.readFileToString(indexMapFile, StandardCharsets.UTF_8);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index" + ".startOffset"), 1, column);
    assertEquals(StringUtils.countMatches(indexMapStr, column + ".range_index" + ".size"), 1, column);

    // In column metadata, some values can change since MV columns with duplicates lose the duplicates on forward index
    // regeneration.
    ColumnMetadata metadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    validateMetadataProperties(column, true, 7, metadata.getCardinality(), metadata.getTotalDocs(),
        metadata.getDataType(), metadata.getFieldType(), metadata.isSorted(), metadata.isSingleValue(),
        metadata.getMaxNumberOfMultiValues(), metadata.getTotalNumberOfEntries(), metadata.isAutoGenerated(),
        metadata.getMinValue(), metadata.getMaxValue(), true);

    // Validate that expected metadata properties don't match. totalNumberOfEntries will definitely not match since
    // duplicates will be removed, but maxNumberOfMultiValues may still match if the row with max multi-values didn't
    // have any duplicates.
    assertNotEquals(metadata.getTotalNumberOfEntries(), originalTotalNumberOfEntries);
  }

  private void validateIndexesForForwardIndexDisabledColumns(String columnName)
      throws IOException {
    // Setup
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();
    ColumnMetadata columnMetadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);

    assertTrue(writer.hasIndexFor(columnName, ColumnIndexType.DICTIONARY));
    if (columnMetadata.isSorted()) {
      assertTrue(writer.hasIndexFor(columnName, ColumnIndexType.FORWARD_INDEX));
    } else {
      assertFalse(writer.hasIndexFor(columnName, ColumnIndexType.FORWARD_INDEX));
    }

    Dictionary dictionary = LoaderUtils.getDictionary(writer, columnMetadata);
    assertEquals(columnMetadata.getCardinality(), dictionary.length());

    segmentDirectory.close();
  }

  private void validateForwardIndex(String columnName, @Nullable FieldConfig.CompressionCodec expectedCompressionType)
      throws IOException {
    // Setup
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
    SegmentDirectory.Writer writer = segmentDirectory.createWriter();
    ColumnMetadata columnMetadata = segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);
    boolean isSingleValue = columnMetadata.isSingleValue();

    if (expectedCompressionType == null) {
      assertTrue(writer.hasIndexFor(columnName, ColumnIndexType.DICTIONARY));
    } else {
      assertFalse(writer.hasIndexFor(columnName, ColumnIndexType.DICTIONARY));
    }
    assertTrue(writer.hasIndexFor(columnName, ColumnIndexType.FORWARD_INDEX));

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
                for (Object value : values) {
                  assertEquals((String) value, "testRow");
                }
              }
              break;
            }
            case INT: {
              if (isSingleValue) {
                assertEquals((int) val, 1001, columnName + " " + rowIdx + " " + expectedCompressionType);
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
                assertEquals((long) val, 1001L, columnName + " " + rowIdx + " " + expectedCompressionType);
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
                assertEquals(val, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
              } else {
                Object[] values = (Object[]) val;
                for (Object value : values) {
                  assertEquals(value, expectedVal, columnName + " " + rowIdx + " " + expectedCompressionType);
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

    segmentDirectory.close();
  }

  private void testIndexExists(String columnName, ColumnIndexType indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap)) {
      assertTrue(segmentDirectory.createReader().hasIndexFor(columnName, indexType));
    }
  }

  private void validateIndexMap(String columnName, boolean dictionaryEnabled, boolean forwardIndexDisabled)
      throws IOException {
    // Panic validation to make sure all columns have only one forward index entry in index map.
    File indexMapFile = SegmentDirectoryPaths.findFile(INDEX_DIR, V1Constants.INDEX_MAP_FILE_NAME);
    assertNotNull(indexMapFile);
    String indexMapStr = FileUtils.readFileToString(indexMapFile, StandardCharsets.UTF_8);
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
