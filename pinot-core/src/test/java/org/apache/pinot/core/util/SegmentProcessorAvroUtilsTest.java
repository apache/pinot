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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.LogicalType;
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
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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

  /// The generated Avro schema must describe the *logical* Pinot type. Switching on the stored type instead emitted
  /// `int` for BOOLEAN, a bare `long` for TIMESTAMP, and rejected BIG_DECIMAL outright.
  @Test
  public void testConvertPinotSchemaToAvroSchemaUsesLogicalTypes() {
    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(allTypesPinotSchema());

    assertFieldType(avroSchema, "intSv", Schema.Type.INT, null);
    assertFieldType(avroSchema, "longSv", Schema.Type.LONG, null);
    assertFieldType(avroSchema, "floatSv", Schema.Type.FLOAT, null);
    assertFieldType(avroSchema, "doubleSv", Schema.Type.DOUBLE, null);
    assertFieldType(avroSchema, "boolSv", Schema.Type.BOOLEAN, null);
    assertFieldType(avroSchema, "tsSv", Schema.Type.LONG, "timestamp-millis");
    assertFieldType(avroSchema, "bigDecimalSv", Schema.Type.BYTES, "big-decimal");
    assertFieldType(avroSchema, "stringSv", Schema.Type.STRING, null);
    assertFieldType(avroSchema, "jsonSv", Schema.Type.STRING, null);
    assertFieldType(avroSchema, "bytesSv", Schema.Type.BYTES, null);
    assertFieldType(avroSchema, "uuidSv", Schema.Type.STRING, "uuid");

    assertElementType(avroSchema, "intMv", Schema.Type.INT, null);
    assertElementType(avroSchema, "boolMv", Schema.Type.BOOLEAN, null);
    assertElementType(avroSchema, "tsMv", Schema.Type.LONG, "timestamp-millis");
    assertElementType(avroSchema, "bigDecimalMv", Schema.Type.BYTES, "big-decimal");
    assertElementType(avroSchema, "stringMv", Schema.Type.STRING, null);
    assertElementType(avroSchema, "bytesMv", Schema.Type.BYTES, null);
    assertElementType(avroSchema, "uuidMv", Schema.Type.STRING, "uuid");
  }

  /// This mapping is duplicated in `AvroUtils.getAvroSchemaFromPinotSchema` (pinot-avro-base), because neither module
  /// can depend on the other, yet both feed the same writers and the same shared data model — `PinotSegmentToAvro
  /// Converter` even pairs one class's schema with the other's data model. Any divergence would silently produce
  /// records the writer cannot serialize, so pin the two together here.
  @Test
  public void testConvertPinotSchemaToAvroSchemaMatchesAvroUtils() {
    org.apache.pinot.spi.data.Schema pinotSchema = allTypesPinotSchema();

    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchema);
    Schema avroUtilsSchema = AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema);

    assertEquals(avroSchema.getFields().size(), avroUtilsSchema.getFields().size());
    for (Schema.Field field : avroSchema.getFields()) {
      Schema.Field avroUtilsField = avroUtilsSchema.getField(field.name());
      assertNotNull(avroUtilsField, "AvroUtils is missing field: " + field.name());
      assertEquals(field.schema(), avroUtilsField.schema(), "schema mismatch for field: " + field.name());
    }
  }

  /// Full ingest round trip for every supported type, SV and MV: Pinot's internal (stored) values are written through
  /// the shared data model, read back with the production `AvroRecordReader`, run through the same data-type
  /// transformation ingestion applies, and must land on the values we started with.
  @Test
  public void testAllTypesRoundTripThroughAvroDataModel()
      throws Exception {
    org.apache.pinot.spi.data.Schema pinotSchema = allTypesPinotSchema();
    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchema);

    byte[] uuidBytes = UuidUtils.toBytes("12345678-1234-1234-1234-1234567890ab");
    byte[] otherUuidBytes = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440000");
    byte[] rawBytes = {1, 2, 3, 4};
    // A scale the value's own magnitude does not imply, to prove `big-decimal` carries the scale per value rather
    // than pinning one for the column the way `decimal(precision, scale)` would.
    BigDecimal bigDecimal = new BigDecimal("123.45000");
    BigDecimal hugeBigDecimal = new BigDecimal("-9999999999999999999999.12345");

    GenericRow row = new GenericRow();
    row.putValue("intSv", 1);
    row.putValue("longSv", 2L);
    row.putValue("floatSv", 3.0f);
    row.putValue("doubleSv", 4.0);
    // BOOLEAN and TIMESTAMP arrive in Pinot's stored form: int 0/1 and epoch millis.
    row.putValue("boolSv", 1);
    row.putValue("tsSv", 1609491661001L);
    row.putValue("bigDecimalSv", bigDecimal);
    row.putValue("stringSv", "5");
    row.putValue("jsonSv", "{\"a\":1}");
    row.putValue("bytesSv", rawBytes);
    row.putValue("uuidSv", uuidBytes);
    row.putValue("intMv", new Object[]{7, 8});
    row.putValue("longMv", new Object[]{9L, 10L});
    row.putValue("floatMv", new Object[]{11.0f, 12.0f});
    row.putValue("doubleMv", new Object[]{13.0, 14.0});
    row.putValue("boolMv", new Object[]{0, 1});
    row.putValue("tsMv", new Object[]{1609491661001L, 0L});
    row.putValue("bigDecimalMv", new Object[]{bigDecimal, hugeBigDecimal});
    row.putValue("stringMv", new Object[]{"15", "16"});
    row.putValue("bytesMv", new Object[]{rawBytes, new byte[]{9}});
    row.putValue("uuidMv", new Object[]{uuidBytes, otherUuidBytes});

    File tmpDir = Files.createTempDirectory("allTypesRoundTrip").toFile();
    try {
      File avroFile = new File(tmpDir, "data.avro");
      GenericData.Record record =
          SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, new GenericData.Record(avroSchema));
      try (DataFileWriter<GenericData.Record> writer =
          new DataFileWriter<>(new GenericDatumWriter<>(avroSchema, SegmentProcessorAvroUtils.getAvroDataModel()))) {
        writer.create(avroSchema, avroFile);
        writer.append(record);
      }

      GenericRow readRow;
      try (AvroRecordReader recordReader = new AvroRecordReader()) {
        recordReader.init(avroFile, pinotSchema.getColumnNames(), null);
        assertTrue(recordReader.hasNext());
        readRow = recordReader.next();
      }
      // Apply the same data-type transformation ingestion runs, so the comparison is against Pinot's stored form.
      for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
        String column = fieldSpec.getName();
        readRow.putValue(column, DataTypeTransformerUtils.transformValue(column, readRow.getValue(column),
            PinotDataType.getPinotDataTypeForIngestion(fieldSpec)));
      }

      assertEquals(readRow.getValue("intSv"), 1);
      assertEquals(readRow.getValue("longSv"), 2L);
      assertEquals(readRow.getValue("floatSv"), 3.0f);
      assertEquals(readRow.getValue("doubleSv"), 4.0);
      assertEquals(readRow.getValue("boolSv"), 1);
      assertEquals(readRow.getValue("tsSv"), 1609491661001L);
      assertEquals(readRow.getValue("bigDecimalSv"), bigDecimal);
      assertEquals(((BigDecimal) readRow.getValue("bigDecimalSv")).scale(), bigDecimal.scale(),
          "big-decimal must preserve the value's own scale");
      assertEquals(readRow.getValue("stringSv"), "5");
      assertEquals(readRow.getValue("jsonSv"), "{\"a\":1}");
      assertEquals((byte[]) readRow.getValue("bytesSv"), rawBytes);
      assertEquals((byte[]) readRow.getValue("uuidSv"), uuidBytes);
      assertEquals((Object[]) readRow.getValue("intMv"), new Object[]{7, 8});
      assertEquals((Object[]) readRow.getValue("longMv"), new Object[]{9L, 10L});
      assertEquals((Object[]) readRow.getValue("floatMv"), new Object[]{11.0f, 12.0f});
      assertEquals((Object[]) readRow.getValue("doubleMv"), new Object[]{13.0, 14.0});
      assertEquals((Object[]) readRow.getValue("boolMv"), new Object[]{0, 1});
      assertEquals((Object[]) readRow.getValue("tsMv"), new Object[]{1609491661001L, 0L});
      assertEquals((Object[]) readRow.getValue("bigDecimalMv"), new Object[]{bigDecimal, hugeBigDecimal});
      assertEquals((Object[]) readRow.getValue("stringMv"), new Object[]{"15", "16"});
      // byte[] elements need element-wise comparison — Object[] equality would compare them by identity.
      assertBytesArrayEquals(readRow.getValue("bytesMv"), rawBytes, new byte[]{9});
      assertBytesArrayEquals(readRow.getValue("uuidMv"), uuidBytes, otherUuidBytes);
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  /// BOOLEAN is the one logical type with no Avro logical type to carry a `Conversion`, so the stored int 0/1 has to
  /// be coerced to Boolean when building the record — both for single values and for multi-value elements.
  @Test
  public void testConvertGenericRowToAvroRecordCoercesStoredBooleans() {
    org.apache.pinot.spi.data.Schema pinotSchema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("boolSchema")
        .addSingleValueDimension("boolSv", DataType.BOOLEAN)
        .addMultiValueDimension("boolMv", DataType.BOOLEAN)
        .build();
    Schema avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchema);

    GenericRow row = new GenericRow();
    row.putValue("boolSv", 0);
    row.putValue("boolMv", new Object[]{1, 0});

    GenericData.Record record =
        SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, new GenericData.Record(avroSchema));

    assertEquals(record.get("boolSv"), Boolean.FALSE);
    assertEquals(record.get("boolMv"), List.of(Boolean.TRUE, Boolean.FALSE));
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

  /// One column per supported data type, single-value and (where Pinot allows it) multi-value.
  private static org.apache.pinot.spi.data.Schema allTypesPinotSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("allTypes")
        .addSingleValueDimension("intSv", DataType.INT)
        .addSingleValueDimension("longSv", DataType.LONG)
        .addSingleValueDimension("floatSv", DataType.FLOAT)
        .addSingleValueDimension("doubleSv", DataType.DOUBLE)
        .addSingleValueDimension("boolSv", DataType.BOOLEAN)
        .addSingleValueDimension("tsSv", DataType.TIMESTAMP)
        .addSingleValueDimension("bigDecimalSv", DataType.BIG_DECIMAL)
        .addSingleValueDimension("stringSv", DataType.STRING)
        .addSingleValueDimension("jsonSv", DataType.JSON)
        .addSingleValueDimension("bytesSv", DataType.BYTES)
        .addSingleValueDimension("uuidSv", DataType.UUID)
        .addMultiValueDimension("intMv", DataType.INT)
        .addMultiValueDimension("longMv", DataType.LONG)
        .addMultiValueDimension("floatMv", DataType.FLOAT)
        .addMultiValueDimension("doubleMv", DataType.DOUBLE)
        .addMultiValueDimension("boolMv", DataType.BOOLEAN)
        .addMultiValueDimension("tsMv", DataType.TIMESTAMP)
        .addMultiValueDimension("bigDecimalMv", DataType.BIG_DECIMAL)
        .addMultiValueDimension("stringMv", DataType.STRING)
        .addMultiValueDimension("bytesMv", DataType.BYTES)
        .addMultiValueDimension("uuidMv", DataType.UUID)
        .build();
  }

  private static void assertBytesArrayEquals(Object actual, byte[]... expected) {
    byte[][] actualArray = (byte[][]) actual;
    assertEquals(actualArray.length, expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(actualArray[i], expected[i], "mismatch at index " + i);
    }
  }

  private static void assertFieldType(Schema avroSchema, String field, Schema.Type expectedType,
      @Nullable String expectedLogicalType) {
    assertAvroType(avroSchema.getField(field).schema(), field, expectedType, expectedLogicalType);
  }

  private static void assertElementType(Schema avroSchema, String field, Schema.Type expectedType,
      @Nullable String expectedLogicalType) {
    Schema fieldSchema = avroSchema.getField(field).schema();
    assertEquals(fieldSchema.getType(), Schema.Type.ARRAY, field + " must be an array");
    assertAvroType(fieldSchema.getElementType(), field, expectedType, expectedLogicalType);
  }

  private static void assertAvroType(Schema schema, String field, Schema.Type expectedType,
      @Nullable String expectedLogicalType) {
    assertEquals(schema.getType(), expectedType, "unexpected Avro type for " + field);
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    if (expectedLogicalType == null) {
      assertNull(logicalType, field + " must carry no logical type");
    } else {
      assertNotNull(logicalType, field + " must carry the " + expectedLogicalType + " logical type");
      assertEquals(logicalType.getName(), expectedLogicalType, "unexpected logical type for " + field);
    }
  }
}
