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
import java.util.HashMap;
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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * TODO: add test for other logical types
 */
public class ParquetReaderLogicalTypesTest {

  @Test
  public void testDecimalType() {
    String schemaStr = "message DecimalExample {"
        + "required int32 decimal_from_int32 (DECIMAL(9, 2));"
        + "required int64 decimal_from_int64 (DECIMAL(18, 2));"
        + "required fixed_len_byte_array(16) decimal_from_fixed (DECIMAL(38, 9));"
        + "required binary decimal_from_binary (DECIMAL(38, 9));"
        + "}";

    byte[] byteArray = new BigDecimal("12345678901234567890123456789.012345678").unscaledValue().toByteArray();
    byte[] paddedArray = new byte[16];
    System.arraycopy(byteArray, 0, paddedArray, 16 - byteArray.length, byteArray.length);
    Map<String, Object> row = new HashMap<>();
    row.put("decimal_from_int32", new BigDecimal("123.45").unscaledValue().intValue());
    row.put("decimal_from_int64", new BigDecimal("1234567890123.45").unscaledValue().longValue());
    row.put("decimal_from_fixed", Binary.fromConstantByteArray(paddedArray));
    row.put("decimal_from_binary", Binary.fromConstantByteArray(byteArray));

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
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (outputPath != null) {
        FileUtils.deleteQuietly(new File(outputPath));
      }
    }
  }

  /**
   * Write the given row object to a random parquet file and return the path of the file
   */
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
