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
package org.apache.pinot.tools.segment.converter;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PinotSegmentConverterTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PinotSegmentConverterTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_SV_COLUMN = "intSVColumn";
  private static final String LONG_SV_COLUMN = "longSVColumn";
  private static final String FLOAT_SV_COLUMN = "floatSVColumn";
  private static final String DOUBLE_SV_COLUMN = "doubleSVColumn";
  private static final String STRING_SV_COLUMN = "stringSVColumn";
  private static final String BYTES_SV_COLUMN = "bytesSVColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_SV_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_SV_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_SV_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_SV_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_SV_COLUMN, DataType.BYTES).addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG).addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING).
          build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private String _segmentDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    GenericRow record = new GenericRow();
    record.putValue(INT_SV_COLUMN, 1);
    record.putValue(LONG_SV_COLUMN, 2L);
    record.putValue(FLOAT_SV_COLUMN, 3.0f);
    record.putValue(DOUBLE_SV_COLUMN, 4.0);
    record.putValue(STRING_SV_COLUMN, "5");
    record.putValue(BYTES_SV_COLUMN, new byte[]{6, 12, 34, 56});
    record.putValue(INT_MV_COLUMN, new Object[]{7, 8});
    record.putValue(LONG_MV_COLUMN, new Object[]{9L, 10L});
    record.putValue(FLOAT_MV_COLUMN, new Object[]{11.0f, 12.0f});
    record.putValue(DOUBLE_MV_COLUMN, new Object[]{13.0, 14.0});
    record.putValue(STRING_MV_COLUMN, new Object[]{"15", "16"});

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(List.of(record)));
    driver.build();

    _segmentDir = driver.getOutputDirectory().getPath();
  }

  @Test
  public void testAvroConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.avro");
    PinotSegmentToAvroConverter avroConverter = new PinotSegmentToAvroConverter(_segmentDir, outputFile.getPath());
    avroConverter.convert();

    try (AvroRecordReader recordReader = new AvroRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), 1);
      assertEquals(record.getValue(LONG_SV_COLUMN), 2L);
      assertEquals(record.getValue(FLOAT_SV_COLUMN), 3.0f);
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), 4.0);
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), new byte[]{6, 12, 34, 56});
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{7, 8});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{9L, 10L});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{11.0f, 12.0f});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{13.0, 14.0});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testCsvConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.csv");
    PinotSegmentToCsvConverter csvConverter =
        new PinotSegmentToCsvConverter(_segmentDir, outputFile.getPath(), CSVRecordReaderConfig.DEFAULT_DELIMITER,
            CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER, true);
    csvConverter.convert();

    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), "1");
      assertEquals(record.getValue(LONG_SV_COLUMN), "2");
      assertEquals(record.getValue(FLOAT_SV_COLUMN), "3.0");
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), "4.0");
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), BytesUtils.toHexString(new byte[]{6, 12, 34, 56}));
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{"7", "8"});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{"9", "10"});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{"11.0", "12.0"});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{"13.0", "14.0"});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testJsonConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.json");
    PinotSegmentToJsonConverter jsonConverter = new PinotSegmentToJsonConverter(_segmentDir, outputFile.getPath());
    jsonConverter.convert();

    try (JSONRecordReader recordReader = new JSONRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), 1);
      assertEquals(record.getValue(LONG_SV_COLUMN), 2);
      assertEquals(record.getValue(FLOAT_SV_COLUMN), 3.0);
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), 4.0);
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), BytesUtils.toHexString(new byte[]{6, 12, 34, 56}));
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{7, 8});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{9, 10});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{11.0, 12.0});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{13.0, 14.0});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testParquetConverter()
      throws Exception {
    File outputFile = new File(TEMP_DIR, "segment.parquet");
    PinotSegmentToParquetConverter parquetConverter =
        new PinotSegmentToParquetConverter(_segmentDir, outputFile.getPath());
    parquetConverter.convert();

    try (ParquetRecordReader recordReader = new ParquetRecordReader()) {
      recordReader.init(outputFile, SCHEMA.getFieldSpecMap().keySet(), null);

      GenericRow record = recordReader.next();
      assertEquals(record.getValue(INT_SV_COLUMN), 1);
      assertEquals(record.getValue(LONG_SV_COLUMN), 2L);
      assertEquals(record.getValue(FLOAT_SV_COLUMN), 3.0f);
      assertEquals(record.getValue(DOUBLE_SV_COLUMN), 4.0);
      assertEquals(record.getValue(STRING_SV_COLUMN), "5");
      assertEquals(record.getValue(BYTES_SV_COLUMN), new byte[]{6, 12, 34, 56});
      assertEquals(record.getValue(INT_MV_COLUMN), new Object[]{7, 8});
      assertEquals(record.getValue(LONG_MV_COLUMN), new Object[]{9L, 10L});
      assertEquals(record.getValue(FLOAT_MV_COLUMN), new Object[]{11.0f, 12.0f});
      assertEquals(record.getValue(DOUBLE_MV_COLUMN), new Object[]{13.0, 14.0});
      assertEquals(record.getValue(STRING_MV_COLUMN), new Object[]{"15", "16"});

      assertFalse(recordReader.hasNext());
    }
  }

  private static final String UUID_SV_COLUMN = "uuidSVColumn";
  private static final String UUID_MV_COLUMN = "uuidMVColumn";
  private static final String UUID_SV_VALUE = "12345678-1234-1234-1234-1234567890ab";
  private static final String UUID_MV_VALUE_1 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_MV_VALUE_2 = "550e8400-e29b-41d4-a716-446655440001";

  /// Builds a segment with SV + MV UUID columns and converts it through both the Avro and Parquet converters. UUID is
  /// stored as a 16-byte value but exported as a string{logicalType:uuid} field, so this exercises the uuid
  /// Conversion registered on each writer's data model (getAvroDataModel) — including the distinct Parquet write path
  /// (AvroParquetWriter.withDataModel), which a plain writer without the Conversion could not serialize.
  @Test
  public void testUuidConverters()
      throws Exception {
    Schema uuidSchema = new Schema.SchemaBuilder().addSingleValueDimension(UUID_SV_COLUMN, DataType.UUID)
        .addMultiValueDimension(UUID_MV_COLUMN, DataType.UUID).build();
    TableConfig uuidTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("uuidTable").build();

    GenericRow record = new GenericRow();
    record.putValue(UUID_SV_COLUMN, UUID_SV_VALUE);
    record.putValue(UUID_MV_COLUMN, new Object[]{UUID_MV_VALUE_1, UUID_MV_VALUE_2});

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(uuidTableConfig, uuidSchema);
    config.setTableName("uuidTable");
    config.setSegmentName("uuidSegment");
    config.setOutDir(new File(TEMP_DIR, "uuidSegment").getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(record)));
    driver.build();
    String segmentDir = driver.getOutputDirectory().getPath();

    File avroOut = new File(TEMP_DIR, "uuidSegment.avro");
    new PinotSegmentToAvroConverter(segmentDir, avroOut.getPath()).convert();
    try (AvroRecordReader reader = new AvroRecordReader()) {
      reader.init(avroOut, uuidSchema.getFieldSpecMap().keySet(), null);
      assertUuidRecord(reader.next());
      assertFalse(reader.hasNext());
    }

    File parquetOut = new File(TEMP_DIR, "uuidSegment.parquet");
    new PinotSegmentToParquetConverter(segmentDir, parquetOut.getPath()).convert();
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(parquetOut, uuidSchema.getFieldSpecMap().keySet(), null);
      assertUuidRecord(reader.next());
      assertFalse(reader.hasNext());
    }
  }

  private static final String BOOLEAN_SV_COLUMN = "boolSVColumn";
  private static final String TIMESTAMP_SV_COLUMN = "tsSVColumn";
  private static final String BIG_DECIMAL_SV_COLUMN = "bigDecimalSVColumn";
  private static final String BOOLEAN_MV_COLUMN = "boolMVColumn";
  private static final String TIMESTAMP_MV_COLUMN = "tsMVColumn";
  private static final long TIMESTAMP_VALUE = 1609491661001L;
  // Beyond long precision and with a non-trivial scale, to prove the exported Avro `big-decimal` carries arbitrary
  // precision and the value's own scale. Trailing zeros are deliberately avoided: ingestion's SpecialValueTransformer
  // strips them, so a value like "123.45000" would already be "123.45" by the time it reaches the segment.
  private static final BigDecimal BIG_DECIMAL_VALUE = new BigDecimal("-9999999999999999999999.12345");

  /// Builds a segment with BOOLEAN, TIMESTAMP and BIG_DECIMAL columns and converts it through both the Avro and
  /// Parquet converters. These are exported using their Avro logical types (`boolean`, `long{timestamp-millis}` and
  /// `bytes{big-decimal}`) rather than their stored types, so this covers the stored-int-to-Boolean coercion and the
  /// big-decimal Conversion on both write paths — including Parquet's distinct AvroParquetWriter.withDataModel path.
  /// Before the fix, BOOLEAN exported as a bare `int`, TIMESTAMP as a bare `long`, and BIG_DECIMAL threw.
  @Test
  public void testLogicalTypeConverters()
      throws Exception {
    Schema logicalSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension(BOOLEAN_SV_COLUMN, DataType.BOOLEAN)
        .addSingleValueDimension(TIMESTAMP_SV_COLUMN, DataType.TIMESTAMP)
        .addSingleValueDimension(BIG_DECIMAL_SV_COLUMN, DataType.BIG_DECIMAL)
        .addMultiValueDimension(BOOLEAN_MV_COLUMN, DataType.BOOLEAN)
        .addMultiValueDimension(TIMESTAMP_MV_COLUMN, DataType.TIMESTAMP)
        .build();
    TableConfig logicalTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("logicalTable").build();

    GenericRow record = new GenericRow();
    record.putValue(BOOLEAN_SV_COLUMN, true);
    record.putValue(TIMESTAMP_SV_COLUMN, new Timestamp(TIMESTAMP_VALUE));
    record.putValue(BIG_DECIMAL_SV_COLUMN, BIG_DECIMAL_VALUE);
    record.putValue(BOOLEAN_MV_COLUMN, new Object[]{true, false});
    record.putValue(TIMESTAMP_MV_COLUMN, new Object[]{new Timestamp(TIMESTAMP_VALUE), new Timestamp(0L)});

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(logicalTableConfig, logicalSchema);
    config.setTableName("logicalTable");
    config.setSegmentName("logicalSegment");
    config.setOutDir(new File(TEMP_DIR, "logicalSegment").getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(record)));
    driver.build();
    String segmentDir = driver.getOutputDirectory().getPath();

    File avroOut = new File(TEMP_DIR, "logicalSegment.avro");
    new PinotSegmentToAvroConverter(segmentDir, avroOut.getPath()).convert();
    try (AvroRecordReader reader = new AvroRecordReader()) {
      reader.init(avroOut, logicalSchema.getFieldSpecMap().keySet(), null);
      assertLogicalTypeRecord(reader.next());
      assertFalse(reader.hasNext());
    }

    File parquetOut = new File(TEMP_DIR, "logicalSegment.parquet");
    new PinotSegmentToParquetConverter(segmentDir, parquetOut.getPath()).convert();
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(parquetOut, logicalSchema.getFieldSpecMap().keySet(), null);
      assertLogicalTypeRecord(reader.next());
      assertFalse(reader.hasNext());
    }
  }

  private static void assertLogicalTypeRecord(GenericRow record) {
    // BOOLEAN reads back as a Boolean rather than the stored int, TIMESTAMP as a Timestamp rather than a bare long.
    assertEquals(record.getValue(BOOLEAN_SV_COLUMN), Boolean.TRUE);
    assertEquals(record.getValue(TIMESTAMP_SV_COLUMN), new Timestamp(TIMESTAMP_VALUE));
    BigDecimal bigDecimal = (BigDecimal) record.getValue(BIG_DECIMAL_SV_COLUMN);
    assertEquals(bigDecimal, BIG_DECIMAL_VALUE);
    assertEquals(bigDecimal.scale(), BIG_DECIMAL_VALUE.scale(), "big-decimal must preserve the value's own scale");
    assertEquals(record.getValue(BOOLEAN_MV_COLUMN), new Object[]{Boolean.TRUE, Boolean.FALSE});
    assertEquals(record.getValue(TIMESTAMP_MV_COLUMN),
        new Object[]{new Timestamp(TIMESTAMP_VALUE), new Timestamp(0L)});
  }

  private static void assertUuidRecord(GenericRow record) {
    // The reader may surface the uuid value as a String/UUID/byte[]; UuidUtils.toBytes normalizes all of them.
    assertEquals(UuidUtils.toBytes(record.getValue(UUID_SV_COLUMN)), UuidUtils.toBytes(UUID_SV_VALUE));
    Object[] mv = (Object[]) record.getValue(UUID_MV_COLUMN);
    assertEquals(mv.length, 2);
    assertEquals(UuidUtils.toBytes(mv[0]), UuidUtils.toBytes(UUID_MV_VALUE_1));
    assertEquals(UuidUtils.toBytes(mv[1]), UuidUtils.toBytes(UUID_MV_VALUE_2));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
