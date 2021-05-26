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
import static org.testng.Assert.*;

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
  }

  @Test
  public void testGetRecordReaderConfigClassName() {
    assertNull(getRecordReaderConfigClassName("avro"));
    assertNull(getRecordReaderConfigClassName("gzipped_avro"));
    assertEquals(getRecordReaderConfigClassName("csv"), DEFAULT_CSV_RECORD_READER_CONFIG_CLASS);
    assertNull(getRecordReaderConfigClassName("json"));
    assertEquals(getRecordReaderConfigClassName("thrift"), DEFAULT_THRIFT_RECORD_READER_CONFIG_CLASS);
    assertNull(getRecordReaderConfigClassName("orc"));
    assertEquals(getRecordReaderConfigClassName("parquet"), DEFAULT_PARQUET_RECORD_READER_CONFIG_CLASS);
  }
}
