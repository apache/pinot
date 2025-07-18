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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentProcessorAvroUtilsTest {


  @DataProvider
  public static Object[][] getTypeConversion() {
    return new Object[][] {
        new Object[] {FieldSpec.DataType.INT, org.apache.avro.Schema.Type.INT, null},
        new Object[] {FieldSpec.DataType.LONG, org.apache.avro.Schema.Type.LONG, null},
        new Object[] {FieldSpec.DataType.FLOAT, org.apache.avro.Schema.Type.FLOAT, null},
        new Object[] {FieldSpec.DataType.DOUBLE, org.apache.avro.Schema.Type.DOUBLE, null},
        new Object[] {FieldSpec.DataType.STRING, org.apache.avro.Schema.Type.STRING, null},
        new Object[] {FieldSpec.DataType.BIG_DECIMAL, org.apache.avro.Schema.Type.BYTES, "big-decimal"},
        new Object[] {FieldSpec.DataType.BYTES, org.apache.avro.Schema.Type.BYTES, null},
        new Object[] {FieldSpec.DataType.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN, null},
        new Object[] {FieldSpec.DataType.JSON, org.apache.avro.Schema.Type.STRING, null},
        new Object[] {FieldSpec.DataType.TIMESTAMP, org.apache.avro.Schema.Type.LONG, "timestamp-millis"}
    };
  }

  @DataProvider
  public static Object[][] getValueConversion() {
    return new Object[][] {
        new Object[] {FieldSpec.DataType.INT, 1, 1},
        new Object[] {FieldSpec.DataType.LONG, 1L, 1L},
        new Object[] {FieldSpec.DataType.FLOAT, 1.0f, 1.0f},
        new Object[] {FieldSpec.DataType.DOUBLE, 1.0d, 1.0d},
        new Object[] {FieldSpec.DataType.STRING, "test", "test"},
        new Object[] {FieldSpec.DataType.BIG_DECIMAL, new BigDecimal("1.0"), new BigDecimal("1.0")},
        new Object[] {FieldSpec.DataType.BYTES, new byte[] {1, 2, 3}, ByteBuffer.wrap(new byte[] {1, 2, 3})},
        new Object[] {FieldSpec.DataType.BOOLEAN, true, true},
        new Object[] {FieldSpec.DataType.JSON, "{\"key\":\"value\"}", "{\"key\":\"value\"}"},
        new Object[] {FieldSpec.DataType.TIMESTAMP,
            LocalDateTime.of(2025, 6, 25, 12, 0, 50).toInstant(ZoneOffset.UTC).toEpochMilli(),
            LocalDateTime.of(2025, 6, 25, 12, 0, 50).toInstant(ZoneOffset.UTC).toEpochMilli()
        }
    };
  }

  @Test(dataProvider = "getValueConversion")
  public void testConversion(
      FieldSpec.DataType dataType,
      Object initial,
      Object expected
  ) {
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("testField", initial);

    Schema.SchemaBuilder pinotSchemaBuilder = new Schema.SchemaBuilder();
    pinotSchemaBuilder.setSchemaName("testSchema");
    pinotSchemaBuilder.addDimensionField("testField", dataType, field -> field.setNullable(true));

    org.apache.avro.Schema avroSchema =
        SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchemaBuilder.build());

    GenericData.Record record = SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(
        genericRow,
        new GenericData.Record(avroSchema));

    Object actualValue = record.get("testField");
    assertEquals(actualValue, expected, "Unexpected value after conversion");
  }

  @Test(dataProvider = "getTypeConversion")
  public void convertNullableSchema(
      FieldSpec.DataType dataType,
      org.apache.avro.Schema.Type expectedType,
      @Nullable String expectedLogicalType
  ) {
    Schema.SchemaBuilder pinotSchemaBuilder = new Schema.SchemaBuilder();
    pinotSchemaBuilder.setSchemaName("testSchema");
    pinotSchemaBuilder.addDimensionField("testField", dataType, field -> field.setNullable(true));

    org.apache.avro.Schema avroSchema =
        SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchemaBuilder.build());
    assertNotNull(avroSchema);

    org.apache.avro.Schema.Field avroField = avroSchema.getField("testField");
    assertTrue(avroField.schema().isNullable(), "Field should be nullable");
    assertEquals(avroField.schema().getTypes().size(), 2, "Field should have two types (null and actual type)");

    org.apache.avro.Schema firstFieldSchema = avroField.schema().getTypes().get(0);
    assertEquals(firstFieldSchema.getType(), expectedType, "Unexpected physical type");
    assertEquals(firstFieldSchema.getProp("logicalType"), expectedLogicalType,
        "Unexpected logical type");
    org.apache.avro.Schema secondFieldSchema = avroField.schema().getTypes().get(1);
    assertEquals(secondFieldSchema.getType(), org.apache.avro.Schema.Type.NULL,
        "Second type should be null");
  }

  @Test(dataProvider = "getTypeConversion")
  public void convertNonNullableSchema(
      FieldSpec.DataType dataType,
      org.apache.avro.Schema.Type exepctedType,
      @Nullable String expectedLogicalType
  ) {
    Schema.SchemaBuilder pinotSchemaBuilder = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addDimensionField("testField", dataType, field -> field.setNullable(false));

    org.apache.avro.Schema avroSchema =
        SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchemaBuilder.build());
    assertNotNull(avroSchema);

    org.apache.avro.Schema.Field avroField = avroSchema.getField("testField");
    assertEquals(avroField.schema().getProp("logicalType"), expectedLogicalType,
        "Unexpected logical type");
    assertFalse(avroField.schema().isNullable(), "Field should not be nullable");
    assertEquals(avroField.schema().getType(), exepctedType, "Unexpected physical type");
  }

  @Test(dataProvider = "getTypeConversion")
  public void convertNullableArraySchema(
      FieldSpec.DataType dataType,
      org.apache.avro.Schema.Type expectedType,
      @Nullable String expectedLogicalType
  ) {
    if (dataType == FieldSpec.DataType.BOOLEAN || dataType == FieldSpec.DataType.BYTES) {
      // Pinot does not support boolean arrays in Avro representation yet
      // The main reason is to distinguish between BOOLEAN multivalues and single BYTES values
      // We could use LogicalTypes to do the distinction (see how it is done with BIG_DECIMAL),
      // but it is not implemented yet
      throw new SkipException("Pinot cannot handle boolean arrays in Avro representation yet");
    }

    Schema.SchemaBuilder pinotSchemaBuilder = new Schema.SchemaBuilder();
    pinotSchemaBuilder.setSchemaName("testSchema");
    pinotSchemaBuilder.addDimensionField("testField", dataType, field -> {
      field.setSingleValueField(false);
      field.setNullable(true);
    });

    org.apache.avro.Schema avroSchema =
        SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchemaBuilder.build());
    assertNotNull(avroSchema);

    org.apache.avro.Schema.Field avroField = avroSchema.getField("testField");

    org.apache.avro.Schema firstType = avroField.schema().getTypes().get(0);
    assertNotNull(firstType.getElementType(), "Field should be an array");
    assertEquals(firstType.getElementType().getType(), expectedType, "Unexpected element type");
    assertEquals(firstType.getElementType().getProp("logicalType"), expectedLogicalType,
        "Unexpected logical type for array element");

    org.apache.avro.Schema secondType = avroField.schema().getTypes().get(1);
    assertEquals(secondType.getType(), org.apache.avro.Schema.Type.NULL,
        "Second type should be null");
  }

  @Test(dataProvider = "getTypeConversion")
  public void convertNotNullArraySchema(
      FieldSpec.DataType dataType,
      org.apache.avro.Schema.Type expectedType,
      @Nullable String expectedLogicalType
  ) {
    if (dataType == FieldSpec.DataType.BOOLEAN || dataType == FieldSpec.DataType.BYTES) {
      // Pinot does not support boolean arrays in Avro representation yet
      // The main reason is to distinguish between BOOLEAN multivalues and single BYTES values
      // We could use LogicalTypes to do the distinction (see how it is done with BIG_DECIMAL),
      // but it is not implemented yet
      throw new SkipException("Pinot cannot handle boolean arrays in Avro representation yet");
    }

    Schema.SchemaBuilder pinotSchemaBuilder = new Schema.SchemaBuilder();
    pinotSchemaBuilder.setSchemaName("testSchema");
    pinotSchemaBuilder.addDimensionField("testField", dataType, field -> {
      field.setSingleValueField(false);
      field.setNullable(false);
    });

    org.apache.avro.Schema avroSchema =
        SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(pinotSchemaBuilder.build());
    assertNotNull(avroSchema);

    org.apache.avro.Schema.Field avroField = avroSchema.getField("testField");
    assertNotNull(avroField.schema().getElementType(), "Field should be an array");
    assertEquals(avroField.schema().getElementType().getType(), expectedType, "Unexpected element type");
    assertEquals(avroField.schema().getElementType().getProp("logicalType"), expectedLogicalType,
        "Unexpected logical type for array element");
  }
}
