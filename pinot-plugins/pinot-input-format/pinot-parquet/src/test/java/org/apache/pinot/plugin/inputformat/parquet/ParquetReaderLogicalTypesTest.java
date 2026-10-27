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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// TODO: add test for other logical types
public class ParquetReaderLogicalTypesTest {

  @Test
  public void testDecimalType() {
    String schemaStr = "message DecimalExample {"
        + "required int32 decimal_from_int32 (DECIMAL(9, 2));"
        + "required int64 decimal_from_int64 (DECIMAL(18, 2));"
        + "required fixed_len_byte_array(16) decimal_from_fixed (DECIMAL(38, 9));"
        + "required binary decimal_from_binary (DECIMAL(38, 9));"
        + "required binary decimal_trailing_zero (DECIMAL(10, 2));"
        + "required binary decimal_precision18 (DECIMAL(18, 6));"
        + "}";

    byte[] byteArray = new BigDecimal("12345678901234567890123456789.012345678").unscaledValue().toByteArray();
    byte[] paddedArray = new byte[16];
    System.arraycopy(byteArray, 0, paddedArray, 16 - byteArray.length, byteArray.length);
    Map<String, Object> row = new HashMap<>();
    row.put("decimal_from_int32", new BigDecimal("123.45").unscaledValue().intValue());
    row.put("decimal_from_int64", new BigDecimal("1234567890123.45").unscaledValue().longValue());
    row.put("decimal_from_fixed", Binary.fromConstantByteArray(paddedArray));
    row.put("decimal_from_binary", Binary.fromConstantByteArray(byteArray));
    row.put("decimal_trailing_zero",
        Binary.fromConstantByteArray(new BigDecimal("1.10").unscaledValue().toByteArray()));
    row.put("decimal_precision18",
        Binary.fromConstantByteArray(new BigDecimal("123456789012.345678").unscaledValue().toByteArray()));

    String outputPath = null;
    try {
      outputPath = writeToFile(schemaStr, row);
      try (ParquetNativeRecordReader recordReader = new ParquetNativeRecordReader()) {
        recordReader.init(new File(outputPath), null, null);
        recordReader.rewind();
        GenericRow genericRow = recordReader.next();
        assertEquals(genericRow.getValue("decimal_from_int32"), new BigDecimal("123.45"));
        assertEquals(genericRow.getValue("decimal_from_int64"), new BigDecimal("1234567890123.45"));
        assertEquals(genericRow.getValue("decimal_from_fixed"),
            new BigDecimal("12345678901234567890123456789.012345678"));
        assertEquals(genericRow.getValue("decimal_from_binary"),
            new BigDecimal("12345678901234567890123456789.012345678"));
        assertEquals(genericRow.getValue("decimal_trailing_zero"), new BigDecimal("1.10"));
        assertEquals(genericRow.getValue("decimal_precision18"), new BigDecimal("123456789012.345678"));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (outputPath != null) {
        FileUtils.deleteQuietly(new File(outputPath));
      }
    }
  }

  /// Unsigned 32-bit integers (`INT32` annotated `INTEGER(32, false)` / legacy `UINT_32`) can exceed
  /// `Integer.MAX_VALUE`, so every reader must widen them to `Long` instead of returning the signed bit pattern.
  @Test
  public void testUnsignedInt32Type()
      throws IOException {
    String schemaStr = "message UnsignedExample {"
        + "required int32 uint32_new (INTEGER(32, false));"
        + "required int32 uint32_legacy (UINT_32);"
        + "required int32 uint16 (INTEGER(16, false));"
        + "}";
    long largeUnsigned = 3540738458L;
    long smallUnsigned = 7L;
    File dataFile = new File(FileUtils.getTempDirectory(), "unsigned-int32-" + System.nanoTime() + ".parquet");
    MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      for (long value : new long[]{largeUnsigned, smallUnsigned}) {
        Group group = groupFactory.newGroup();
        group.append("uint32_new", (int) value);
        group.append("uint32_legacy", (int) value);
        group.append("uint16", 65535);
        writer.write(group);
      }
    }
    try {
      List<RecordReader> recordReaders =
          List.of(new ParquetNativeRecordReader(), new ParquetAvroRecordReader(), new ParquetRecordReader());
      for (RecordReader recordReader : recordReaders) {
        String readerName = recordReader.getClass().getSimpleName();
        try (recordReader) {
          recordReader.init(dataFile, null, null);
          List<GenericRow> rows = new ArrayList<>();
          while (recordReader.hasNext()) {
            rows.add(recordReader.next());
          }
          assertEquals(rows.size(), 2, readerName);
          assertEquals(rows.get(0).getValue("uint32_new"), largeUnsigned, readerName);
          assertEquals(rows.get(0).getValue("uint32_legacy"), largeUnsigned, readerName);
          assertEquals(rows.get(1).getValue("uint32_new"), smallUnsigned, readerName);
          assertEquals(rows.get(1).getValue("uint32_legacy"), smallUnsigned, readerName);
          // Narrower unsigned widths always fit in a signed int.
          assertEquals(rows.get(0).getValue("uint16"), 65535, readerName);
        }
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  /// The native reader widens unsigned 32-bit values at every nesting level: in a list and in a struct.
  @Test
  public void testNestedUnsignedInt32TypeWithNativeReader()
      throws IOException {
    String schemaStr = "message NestedUnsignedExample {"
        + "required group uint32_list (LIST) { repeated group list { required int32 element (INTEGER(32, false)); } }"
        + "required group uint32_struct { required int32 value (UINT_32); }"
        + "}";
    long largeUnsigned = 3540738458L;
    File dataFile = new File(FileUtils.getTempDirectory(), "nested-unsigned-int32-" + System.nanoTime() + ".parquet");
    MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .build()) {
      Group group = new SimpleGroupFactory(schema).newGroup();
      Group list = group.addGroup("uint32_list");
      list.addGroup("list").append("element", (int) largeUnsigned);
      list.addGroup("list").append("element", 7);
      group.addGroup("uint32_struct").append("value", (int) largeUnsigned);
      writer.write(group);
    }
    try (ParquetNativeRecordReader recordReader = new ParquetNativeRecordReader()) {
      recordReader.init(dataFile, null, null);
      GenericRow row = recordReader.next();
      assertEquals((Object[]) row.getValue("uint32_list"), new Object[]{largeUnsigned, 7L});
      assertEquals(((Map<?, ?>) row.getValue("uint32_struct")).get("value"), largeUnsigned);
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testNativeReaderAdvancesPastRejectedUnsignedInt64()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required int64 value (INTEGER(64, false)); }");
    File dataFile = new File(FileUtils.getTempDirectory(), "rejected-uint64-" + System.nanoTime() + ".parquet");
    try {
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
          .withType(schema).build()) {
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        for (long value : new long[]{-1L, 7L, Long.MIN_VALUE}) {
          writer.write(groups.newGroup().append("value", value));
        }
      }
      try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
        reader.init(dataFile, null, null);
        assertTrue(reader.hasNext());
        expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
        assertTrue(reader.hasNext());
        assertEquals(reader.next(new GenericRow()).getValue("value"), 7L);
        assertTrue(reader.hasNext());
        expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
        assertFalse(reader.hasNext(), "Rejected records must count toward the consumed row group");
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testAvroReaderAdvancesPastRejectedInt96()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required int96 value; }");
    File dataFile = new File(FileUtils.getTempDirectory(), "rejected-int96-" + System.nanoTime() + ".parquet");
    try {
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
          .withType(schema).build()) {
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        // Year 1600 cannot be represented as signed Long epoch nanos; the next row is in range.
        writer.write(groups.newGroup().append("value", new NanoTime(2305448, 0L)));
        writer.write(groups.newGroup().append("value", new NanoTime(2440588, 7L)));
      }
      ParquetRecordReaderConfig config = new ParquetRecordReaderConfig();
      config.setUseParquetAvroRecordReader(true);
      config.setExtractRawTimeValues(true);
      try (ParquetRecordReader reader = new ParquetRecordReader()) {
        reader.init(dataFile, null, config);
        assertTrue(reader.useAvroParquetRecordReader());
        for (int iteration = 0; iteration < 2; iteration++) {
          if (iteration > 0) {
            reader.rewind();
          }
          assertTrue(reader.hasNext());
          expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
          assertTrue(reader.hasNext());
          assertEquals(reader.next(new GenericRow()).getValue("value"), 7L);
          assertFalse(reader.hasNext(), "Extraction failures must not retry the rejected Avro record");
        }
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  /// Write the given row object to a random parquet file and return the path of the file
  private String writeToFile(String schemaStr, Map<String, Object> rowObject)
      throws IOException {
    Path outputPath = new Path(FileUtils.getTempDirectory().toString(), "example.parquet");
    MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      Group group = groupFactory.newGroup();
      for (Map.Entry<String, Object> entry : rowObject.entrySet()) {
        if (entry.getValue() instanceof Integer) {
          group.append(entry.getKey(), (int) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof Long) {
          group.append(entry.getKey(), (long) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof Float) {
          group.append(entry.getKey(), (Float) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof Double) {
          group.append(entry.getKey(), (Double) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof NanoTime) {
          group.append(entry.getKey(), (NanoTime) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof String) {
          group.append(entry.getKey(), (String) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof Boolean) {
          group.append(entry.getKey(), (Boolean) entry.getValue());
          continue;
        }
        if (entry.getValue() instanceof Binary) {
          group.append(entry.getKey(), (Binary) entry.getValue());
        }
      }
      writer.write(group);
    }
    return outputPath.toString();
  }
}
