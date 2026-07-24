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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentProcessorAvroUtilsTest {

  /// convertGenericRowToAvroRecord keeps a UUID value as its raw 16-byte form (it does NOT render a string here); the
  /// rendering is deferred to the uuid Conversion on the writer's data model. Plain BYTES columns are still wrapped as
  /// ByteBuffer.
  @Test
  public void testConvertGenericRowToAvroRecordKeepsUuidBytesRaw() {
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    Schema recordSchema = SchemaBuilder.record("record").fields()
        .name("uuidCol").type(uuidSchema).noDefault()
        .name("bytesCol").type().bytesType().noDefault()
        .endRecord();

    byte[] uuidBytes = UuidUtils.toBytes("12345678-1234-1234-1234-1234567890ab");
    byte[] rawBytes = {1, 2, 3, 4};
    GenericRow row = new GenericRow();
    row.putValue("uuidCol", uuidBytes);
    row.putValue("bytesCol", rawBytes);

    GenericData.Record record = new GenericData.Record(recordSchema);
    SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, record);

    assertTrue(record.get("uuidCol") instanceof byte[], "UUID must be left as raw byte[] for the uuid Conversion");
    assertEquals((byte[]) record.get("uuidCol"), uuidBytes);
    assertEquals(record.get("bytesCol"), ByteBuffer.wrap(rawBytes), "BYTES byte[] must be wrapped as ByteBuffer");
  }

  /// End-to-end: a GenericDatumWriter built with getAvroDataModel() serializes the raw 16-byte UUID values as their
  /// canonical string (via the registered uuid Conversion) for both SV and MV, and the on-disk value reads back as
  /// that string with a vanilla reader. Without the registered Conversion the write would fail (a ByteBuffer/byte[]
  /// cannot be written to a string{logicalType:uuid} field).
  @Test
  public void testUuidRoundTripThroughAvroDataModel()
      throws Exception {
    org.apache.pinot.spi.data.Schema pinotSchema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("uuidSchema")
        .addSingleValueDimension("uuidSv", DataType.UUID)
        .addMultiValueDimension("uuidMv", DataType.UUID)
        .build();
    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchema);

    String svCanonical = "12345678-1234-1234-1234-1234567890ab";
    String mvCanonical = "550e8400-e29b-41d4-a716-446655440000";
    GenericRow row = new GenericRow();
    row.putValue("uuidSv", UuidUtils.toBytes(svCanonical));
    row.putValue("uuidMv", new Object[]{UuidUtils.toBytes(mvCanonical)});
    GenericData.Record record =
        SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, new GenericData.Record(avroSchema));

    File tmp = File.createTempFile("uuidRoundTrip", ".avro");
    try {
      GenericData model = SegmentProcessorAvroUtils.getAvroDataModel();
      try (DataFileWriter<GenericData.Record> writer =
          new DataFileWriter<>(new GenericDatumWriter<>(avroSchema, model))) {
        writer.create(avroSchema, tmp);
        writer.append(record);
      }
      // Read back with a vanilla reader: the on-disk representation must be the canonical UUID string.
      try (DataFileReader<GenericRecord> reader = new DataFileReader<>(tmp, new GenericDatumReader<>(avroSchema))) {
        assertTrue(reader.hasNext());
        GenericRecord read = reader.next();
        assertEquals(read.get("uuidSv").toString(), svCanonical, "SV UUID must serialize as canonical string");
        List<?> mv = (List<?>) read.get("uuidMv");
        assertEquals(mv.size(), 1);
        assertEquals(mv.get(0).toString(), mvCanonical, "MV UUID element must serialize as canonical string");
      }
      // Read back WITH the data model: the uuid Conversion's fromCharSequence must reconstruct the raw 16-byte value.
      try (DataFileReader<GenericRecord> reader = new DataFileReader<>(tmp,
          new GenericDatumReader<>(avroSchema, avroSchema, SegmentProcessorAvroUtils.getAvroDataModel()))) {
        GenericRecord read = reader.next();
        assertEquals((byte[]) read.get("uuidSv"), UuidUtils.toBytes(svCanonical),
            "reading with the model must convert the uuid string back to raw bytes");
        assertEquals((byte[]) ((List<?>) read.get("uuidMv")).get(0), UuidUtils.toBytes(mvCanonical),
            "MV uuid element must convert back to raw bytes when reading with the model");
      }
    } finally {
      FileUtils.deleteQuietly(tmp);
    }
  }

  /// convertPinotSchemaToAvroSchema must emit SV UUID as string{logicalType:uuid} and MV UUID as
  /// array<string{logicalType:uuid}>, which is what the uuid Conversion above pairs with.
  @Test
  public void testConvertPinotSchemaToAvroSchemaEmitsUuidLogicalType() {
    org.apache.pinot.spi.data.Schema pinotSchema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("uuidSchema")
        .addSingleValueDimension("uuidSv", DataType.UUID)
        .addMultiValueDimension("uuidMv", DataType.UUID)
        .build();

    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchema);

    Schema svFieldSchema = avroSchema.getField("uuidSv").schema();
    assertEquals(svFieldSchema.getType(), Schema.Type.STRING, "SV UUID must be string{logicalType:uuid}");
    assertEquals(LogicalTypes.fromSchemaIgnoreInvalid(svFieldSchema), LogicalTypes.uuid(),
        "SV UUID must carry the uuid logical type");

    Schema mvFieldSchema = avroSchema.getField("uuidMv").schema();
    assertEquals(mvFieldSchema.getType(), Schema.Type.ARRAY, "MV UUID must be emitted as an array");
    Schema mvElementSchema = mvFieldSchema.getElementType();
    assertEquals(mvElementSchema.getType(), Schema.Type.STRING, "MV UUID elements must be string{logicalType:uuid}");
    assertEquals(LogicalTypes.fromSchemaIgnoreInvalid(mvElementSchema), LogicalTypes.uuid(),
        "MV UUID elements must carry the uuid logical type");
  }
}
