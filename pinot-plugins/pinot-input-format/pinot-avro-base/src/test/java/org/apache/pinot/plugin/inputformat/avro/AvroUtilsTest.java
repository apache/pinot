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
package org.apache.pinot.plugin.inputformat.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.LogicalTypes;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AvroUtilsTest {

  private static final String AVRO_SCHEMA = "fake_avro_schema.avsc";
  private static final String AVRO_NESTED_SCHEMA = "fake_avro_nested_schema.avsc";

  @Test
  public void testGetPinotSchemaFromAvroSchemaNullFieldTypeMap()
      throws IOException {
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, null, null);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addSingleValueDimension("m1", DataType.INT).addSingleValueDimension("m2", DataType.INT)
        .addSingleValueDimension("hoursSinceEpoch", DataType.LONG).build();
    assertEquals(expectedSchema, inferredPinotSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithFieldTypeMap()
      throws IOException {
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        Map.of("d1", FieldType.DIMENSION,
            "d2", FieldType.DIMENSION,
            "d3", FieldType.DIMENSION,
            "hoursSinceEpoch", FieldType.DATE_TIME,
            "m1", FieldType.METRIC,
            "m2", FieldType.METRIC);
    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, fieldSpecMap, TimeUnit.HOURS);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addMetric("m1", DataType.INT).addMetric("m2", DataType.INT)
        .addDateTime("hoursSinceEpoch", DataType.LONG, "EPOCH|HOURS", "1:HOURS").build();
    assertEquals(inferredPinotSchema, expectedSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithComplexType()
      throws IOException {
    // do not unnest collect
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_NESTED_SCHEMA));
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        Map.of("d1", FieldType.DIMENSION, "hoursSinceEpoch", FieldType.DATE_TIME, "m1", FieldType.METRIC);
    Schema inferredPinotSchema =
        AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            new ArrayList<>(), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    Schema expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("entries", DataType.STRING)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING)
            .addMultiValueDimension("d2", DataType.INT)
            .addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addDateTime("hoursSinceEpoch", DataType.LONG, "EPOCH|HOURS", "1:HOURS").build();
    assertEquals(inferredPinotSchema, expectedSchema);

    // unnest collection entries
    inferredPinotSchema =
        AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList("entries"), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("entries.id", DataType.LONG)
            .addSingleValueDimension("entries.description", DataType.STRING)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING).addMultiValueDimension("d2", DataType.INT)
            .addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addDateTime("hoursSinceEpoch", DataType.LONG, "EPOCH|HOURS", "1:HOURS").build();
    assertEquals(inferredPinotSchema, expectedSchema);

    // change delimiter
    inferredPinotSchema =
        AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList(), "_", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("entries", DataType.STRING)
            .addSingleValueDimension("tuple_streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple_city", DataType.STRING)
            .addMultiValueDimension("d2", DataType.INT)
            .addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addDateTime("hoursSinceEpoch", DataType.LONG, "EPOCH|HOURS", "1:HOURS").build();
    assertEquals(inferredPinotSchema, expectedSchema);

    // change the handling of collection-to-json option, d2 will become string
    inferredPinotSchema =
        AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList("entries"), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.ALL);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("entries.id", DataType.LONG)
            .addSingleValueDimension("entries.description", DataType.STRING)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING)
            .addSingleValueDimension("d2", DataType.STRING)
            .addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addDateTime("hoursSinceEpoch", DataType.LONG, "EPOCH|HOURS", "1:HOURS").build();
    assertEquals(inferredPinotSchema, expectedSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithUuidLogicalType() {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    org.apache.avro.Schema uuidSchema =
        LogicalTypes.uuid().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    avroSchema.setFields(Lists.newArrayList(
        new org.apache.avro.Schema.Field("uuidCol", uuidSchema, null, null)
    ));

    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, null, null);

    Schema expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("uuidCol", DataType.UUID).build();
    assertEquals(inferredPinotSchema, expectedSchema);
  }

  @Test
  public void testGetAvroSchemaFromPinotSchemaWithUuidLogicalType() {
    Schema pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("uuidCol", DataType.UUID).build();

    org.apache.avro.Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema);
    org.apache.avro.Schema fieldSchema = avroSchema.getField("uuidCol").schema();

    assertEquals(fieldSchema.getType(), org.apache.avro.Schema.Type.STRING);
    assertEquals(fieldSchema.getLogicalType().getName(), "uuid");
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaRejectsUuidArray() {
    org.apache.avro.Schema uuidSchema =
        LogicalTypes.uuid().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("uuidArrayRecord").fields()
        .name("uuidArray").type().array().items(uuidSchema).noDefault().endRecord();

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, null, null,
            new ArrayList<>(), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE));
    assertTrue(exception.getMessage().contains("ARRAY<uuid>"));
  }

  @Test
  public void testGetAvroSchemaFromPinotSchemaRejectsMvUuid() {
    Schema pinotSchema = new Schema();
    pinotSchema.addField(new DimensionFieldSpec("uuidMv", DataType.UUID, false));

    IllegalStateException exception =
        Assert.expectThrows(IllegalStateException.class, () -> AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema));
    assertTrue(exception.getMessage().contains("single-value"));
  }
}
