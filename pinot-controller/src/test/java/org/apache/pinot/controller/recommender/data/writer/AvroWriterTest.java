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
package org.apache.pinot.controller.recommender.data.writer;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.IntegerRange;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.controller.recommender.data.generator.DataGeneratorSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AvroWriterTest {
  private File _baseDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _baseDir = Files.createTempDirectory("avroWriterTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(_baseDir);
  }

  /// Guards that logical types round-trip through the generated Avro file: BOOLEAN is written as an Avro boolean
  /// (not an int) and TIMESTAMP as a timestamp-millis long. Before the fix the schema used the stored type, so
  /// BOOLEAN showed up as "int" and TIMESTAMP as a plain "long".
  @Test
  public void testLogicalTypesRoundTrip()
      throws Exception {
    List<String> columns = List.of("intCol", "longCol", "boolCol", "tsCol", "strCol");
    Map<String, DataType> dataTypes = new HashMap<>();
    dataTypes.put("intCol", DataType.INT);
    dataTypes.put("longCol", DataType.LONG);
    dataTypes.put("boolCol", DataType.BOOLEAN);
    dataTypes.put("tsCol", DataType.TIMESTAMP);
    dataTypes.put("strCol", DataType.STRING);
    Map<String, FieldType> fieldTypes = new HashMap<>();
    Map<String, Integer> cardinality = new HashMap<>();
    Map<String, Integer> length = new HashMap<>();
    for (String column : columns) {
      fieldTypes.put(column, FieldType.DIMENSION);
      cardinality.put(column, 5);
    }
    length.put("strCol", 6);

    DataGeneratorSpec spec =
        new DataGeneratorSpec(columns, cardinality, new HashMap<String, IntegerRange>(), new HashMap<>(),
            new HashMap<>(), length, dataTypes, fieldTypes, new HashMap<String, TimeUnit>(), new HashMap<>(),
            new HashMap<>());
    DataGenerator generator = new DataGenerator();
    generator.init(spec);

    int totalDocs = 10;
    AvroWriterSpec writerSpec = new AvroWriterSpec(generator, _baseDir, totalDocs, 1, 0);
    AvroWriter writer = new AvroWriter();
    writer.init(writerSpec);
    writer.write();

    // Schema assertions: original types are preserved, not their stored types.
    Schema avroSchema = AvroWriter.getAvroSchema(writerSpec.getSchema());
    assertEquals(nonNullBranch(avroSchema, "intCol").getType(), Schema.Type.INT);
    assertEquals(nonNullBranch(avroSchema, "longCol").getType(), Schema.Type.LONG);
    assertEquals(nonNullBranch(avroSchema, "boolCol").getType(), Schema.Type.BOOLEAN);
    Schema tsBranch = nonNullBranch(avroSchema, "tsCol");
    assertEquals(tsBranch.getType(), Schema.Type.LONG);
    assertEquals(tsBranch.getProp("logicalType"), "timestamp-millis");
    assertEquals(nonNullBranch(avroSchema, "strCol").getType(), Schema.Type.STRING);

    // Value assertions: the written records serialize and read back with the expected Java types.
    File avroFile = new File(_baseDir, "part-0.avro");
    assertTrue(avroFile.exists());
    int count = 0;
    try (DataFileReader<GenericRecord> reader = new DataFileReader<>(avroFile, new GenericDatumReader<>())) {
      for (GenericRecord record : reader) {
        assertTrue(record.get("boolCol") instanceof Boolean, "boolCol should be a Boolean");
        assertTrue(record.get("tsCol") instanceof Long, "tsCol should be a Long");
        assertTrue(record.get("intCol") instanceof Integer, "intCol should be an Integer");
        count++;
      }
    }
    assertEquals(count, totalDocs);
  }

  /// The recommender must support UUID columns end to end: the generated schema declares a logicalType "uuid" string
  /// and the values round-trip as canonical UUID strings. Before UUID generation was wired up, writing a UUID column
  /// threw because the number generator rejected the type.
  @Test
  public void testUuidRoundTrip()
      throws Exception {
    List<String> columns = List.of("uuidCol");
    Map<String, DataType> dataTypes = new HashMap<>();
    dataTypes.put("uuidCol", DataType.UUID);
    Map<String, FieldType> fieldTypes = new HashMap<>();
    Map<String, Integer> cardinality = new HashMap<>();
    for (String column : columns) {
      fieldTypes.put(column, FieldType.DIMENSION);
      cardinality.put(column, 5);
    }

    DataGeneratorSpec spec =
        new DataGeneratorSpec(columns, cardinality, new HashMap<String, IntegerRange>(), new HashMap<>(),
            new HashMap<>(), new HashMap<>(), dataTypes, fieldTypes, new HashMap<String, TimeUnit>(), new HashMap<>(),
            new HashMap<>());
    DataGenerator generator = new DataGenerator();
    generator.init(spec);

    int totalDocs = 10;
    AvroWriterSpec writerSpec = new AvroWriterSpec(generator, _baseDir, totalDocs, 1, 0);
    AvroWriter writer = new AvroWriter();
    writer.init(writerSpec);
    writer.write();

    Schema avroSchema = AvroWriter.getAvroSchema(writerSpec.getSchema());
    Schema uuidBranch = nonNullBranch(avroSchema, "uuidCol");
    assertEquals(uuidBranch.getType(), Schema.Type.STRING);
    assertEquals(uuidBranch.getProp("logicalType"), "uuid");

    File avroFile = new File(_baseDir, "part-0.avro");
    assertTrue(avroFile.exists());
    int count = 0;
    try (DataFileReader<GenericRecord> reader = new DataFileReader<>(avroFile, new GenericDatumReader<>())) {
      for (GenericRecord record : reader) {
        Object value = record.get("uuidCol");
        assertTrue(value instanceof CharSequence, "uuidCol should read back as a string");
        String uuid = value.toString();
        assertEquals(UUID.fromString(uuid).toString(), uuid, "value must be a canonical UUID string");
        count++;
      }
    }
    assertEquals(count, totalDocs);
  }

  /// The generator emits Pinot's stored form for BOOLEAN (int 0/1); coercion must map it to the right Boolean and
  /// leave non-boolean columns untouched.
  @Test
  public void testBooleanCoercion() {
    assertEquals(AvroRecordAppender.coerce(true, 0), Boolean.FALSE);
    assertEquals(AvroRecordAppender.coerce(true, 1), Boolean.TRUE);
    assertEquals(AvroRecordAppender.coerce(true, 5), Boolean.TRUE);
    // Non-boolean columns pass through unchanged.
    assertEquals(AvroRecordAppender.coerce(false, 1), 1);
    assertEquals(AvroRecordAppender.coerce(true, null), null);
  }

  private static Schema nonNullBranch(Schema recordSchema, String fieldName) {
    return AvroRecordAppender.nonNullBranch(recordSchema.getField(fieldName).schema());
  }
}
