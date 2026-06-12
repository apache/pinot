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
package org.apache.pinot.spi.data.readers;

import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.readers.RecordReaderFactory.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class RecordReaderFactoryTest {

  @Test
  public void testGetRecordReaderClassName() {
    assertEquals(getRecordReaderClassName("avro"), DEFAULT_AVRO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("gzipped_avro"), DEFAULT_AVRO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("csv"), DEFAULT_CSV_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("json"), DEFAULT_JSON_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("thrift"), DEFAULT_THRIFT_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("orc"), DEFAULT_ORC_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("parquet"), DEFAULT_PARQUET_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("proto"), DEFAULT_PROTO_RECORD_READER_CLASS);
  }

  @Test
  public void testGetRecordReaderConfigClassName() {
    assertEquals(getRecordReaderConfigClassName("avro"), DEFAULT_AVRO_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("gzipped_avro"), DEFAULT_AVRO_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("csv"), DEFAULT_CSV_RECORD_READER_CONFIG_CLASS);
    assertNull(getRecordReaderConfigClassName("json"));
    assertEquals(getRecordReaderConfigClassName("thrift"), DEFAULT_THRIFT_RECORD_READER_CONFIG_CLASS);
    assertNull(getRecordReaderConfigClassName("orc"));
    assertEquals(getRecordReaderConfigClassName("parquet"), DEFAULT_PARQUET_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("proto"), DEFAULT_PROTO_RECORD_READER_CONFIG_CLASS);
  }

  @Test
  public void testGetRecordReaderClassNameWithUnknownFormat() {
    // Unknown string formats should return null
    // Note: "protobuf" is not registered directly - use FileFormat.fromString("protobuf") first
    assertNull(getRecordReaderClassName("unknown"));
    assertNull(getRecordReaderClassName("xml"));
    assertNull(getRecordReaderClassName(""));
    assertNull(getRecordReaderClassName("protobuf")); // Use FileFormat.fromString("protobuf") -> PROTO instead
  }

  @Test
  public void testGetRecordReaderConfigClassNameWithUnknownFormat() {
    // Unknown string formats should return null
    assertNull(getRecordReaderConfigClassName("unknown"));
    assertNull(getRecordReaderConfigClassName("xml"));
    assertNull(getRecordReaderConfigClassName(""));
    assertNull(getRecordReaderConfigClassName("protobuf")); // Use FileFormat.fromString("protobuf") -> PROTO instead
  }

  @Test
  public void testGetRecordReaderClassNameCaseInsensitivity() {
    // Test case insensitivity for string-based lookup (uses FileFormat enum names)
    assertEquals(getRecordReaderClassName("AVRO"), DEFAULT_AVRO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("Avro"), DEFAULT_AVRO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("avro"), DEFAULT_AVRO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("CSV"), DEFAULT_CSV_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("Csv"), DEFAULT_CSV_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("PARQUET"), DEFAULT_PARQUET_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("Parquet"), DEFAULT_PARQUET_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("PROTO"), DEFAULT_PROTO_RECORD_READER_CLASS);
    assertEquals(getRecordReaderClassName("Proto"), DEFAULT_PROTO_RECORD_READER_CLASS);
  }

  @Test
  public void testGetRecordReaderConfigClassNameCaseInsensitivity() {
    // Test case insensitivity for string-based lookup (uses FileFormat enum names)
    assertEquals(getRecordReaderConfigClassName("AVRO"), DEFAULT_AVRO_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("Avro"), DEFAULT_AVRO_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("CSV"), DEFAULT_CSV_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("Csv"), DEFAULT_CSV_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("PROTO"), DEFAULT_PROTO_RECORD_READER_CONFIG_CLASS);
    assertEquals(getRecordReaderConfigClassName("Proto"), DEFAULT_PROTO_RECORD_READER_CONFIG_CLASS);
  }

  @Test
  public void testProtobufToProtoConversionFlow() {
    // Demonstrate the intended flow: "protobuf" -> FileFormat.fromString() -> FileFormat.PROTO
    // Then use FileFormat.PROTO with RecordReaderFactory
    FileFormat fileFormat = FileFormat.fromString("protobuf");
    assertEquals(fileFormat, FileFormat.PROTO);
    assertEquals(getRecordReaderClassName(fileFormat.name()), DEFAULT_PROTO_RECORD_READER_CLASS);

    // "proto" also works via FileFormat.fromString()
    assertEquals(FileFormat.fromString("proto"), FileFormat.PROTO);
    assertEquals(FileFormat.fromString("PROTO"), FileFormat.PROTO);
  }

  @Test
  public void testDefaultClassConstants() {
    // Verify the default class constants are set correctly
    assertNotNull(DEFAULT_AVRO_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_CSV_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_JSON_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_THRIFT_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_ORC_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_PARQUET_RECORD_READER_CLASS);
    assertNotNull(DEFAULT_PROTO_RECORD_READER_CLASS);

    // Verify class name format
    assertEquals(DEFAULT_AVRO_RECORD_READER_CLASS, "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
    assertEquals(DEFAULT_CSV_RECORD_READER_CLASS, "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
    assertEquals(DEFAULT_JSON_RECORD_READER_CLASS, "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
    assertEquals(DEFAULT_PROTO_RECORD_READER_CLASS,
        "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReader");
  }

  @Test
  public void testFileFormatPinotAndOther() {
    // PINOT and OTHER formats don't have default record readers
    assertNull(getRecordReaderClassName("PINOT"));
    assertNull(getRecordReaderClassName("OTHER"));
  }
}
