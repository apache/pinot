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
package org.apache.pinot.core.util;

import java.nio.ByteBuffer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentProcessorAvroUtilsTest {

  /**
   * Regression test: UUID columns are exported with an Avro {@code string{logicalType:uuid}} schema, so the
   * 16-byte canonical form held internally must be converted to a canonical UUID string at write time. Wrapping
   * it as a ByteBuffer (the default for plain BYTES columns) would be rejected by GenericDatumWriter against a
   * STRING-typed schema.
   */
  @Test
  public void testConvertGenericRowToAvroRecordRendersUuidBytesAsCanonicalString() {
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema recordSchema = SchemaBuilder.record("record").fields()
        .name("uuidCol").type(uuidSchema).noDefault()
        .name("bytesCol").type(bytesSchema).noDefault()
        .endRecord();

    String canonical = "12345678-1234-1234-1234-1234567890ab";
    byte[] uuidBytes = UuidUtils.toBytes(canonical);
    byte[] rawBytes = new byte[]{1, 2, 3, 4};

    GenericRow row = new GenericRow();
    row.putValue("uuidCol", uuidBytes);
    row.putValue("bytesCol", rawBytes);

    GenericData.Record reusableRecord = new GenericData.Record(recordSchema);
    SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, reusableRecord);

    assertEquals(reusableRecord.get("uuidCol"), canonical,
        "UUID byte[] must be converted to canonical UUID string for string{logicalType:uuid} fields");
    assertEquals(reusableRecord.get("bytesCol"), ByteBuffer.wrap(rawBytes),
        "Plain BYTES columns must continue to be wrapped as ByteBuffer");
  }
}
