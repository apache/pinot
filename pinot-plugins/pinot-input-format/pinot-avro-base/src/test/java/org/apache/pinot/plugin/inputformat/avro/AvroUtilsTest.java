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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public class AvroUtilsTest {

  String AVRO_SCHEMA = "fake_avro_schema.avsc";
  String AVRO_NESTED_SCHEMA = "fake_avro_nested_schema.avsc";

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
    Assert.assertEquals(expectedSchema, inferredPinotSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithFieldTypeMap()
      throws IOException {
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        new ImmutableMap.Builder<String, FieldSpec.FieldType>().put("d1", FieldType.DIMENSION)
            .put("d2", FieldType.DIMENSION).put("d3", FieldType.DIMENSION).put("hoursSinceEpoch", FieldType.TIME)
            .put("m1", FieldType.METRIC).put("m2", FieldType.METRIC).build();
    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, fieldSpecMap, TimeUnit.HOURS);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addMetric("m1", DataType.INT).addMetric("m2", DataType.INT)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hoursSinceEpoch"), null).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);

    fieldSpecMap = new ImmutableMap.Builder<String, FieldSpec.FieldType>().put("d1", FieldType.DIMENSION)
        .put("d2", FieldType.DIMENSION).put("d3", FieldType.DIMENSION).put("hoursSinceEpoch", FieldType.DATE_TIME)
        .put("m1", FieldType.METRIC).put("m2", FieldType.METRIC).build();
    inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, fieldSpecMap, TimeUnit.HOURS);
    expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addMetric("m1", DataType.INT).addMetric("m2", DataType.INT)
        .addDateTime("hoursSinceEpoch", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithComplexType()
      throws IOException {
    // do not unnest collect
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_NESTED_SCHEMA));
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        new ImmutableMap.Builder<String, FieldSpec.FieldType>().put("d1", FieldType.DIMENSION)
            .put("hoursSinceEpoch", FieldType.TIME).put("m1", FieldType.METRIC).build();
    Schema inferredPinotSchema = AvroUtils
        .getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            new ArrayList<>(), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    Schema expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING).addSingleValueDimension("entries", DataType.STRING)
            .addMultiValueDimension("d2", DataType.INT)
            .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hoursSinceEpoch"), null).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);

    // unnest collection entries
    inferredPinotSchema = AvroUtils
        .getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList("entries"), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING).addSingleValueDimension("entries.id", DataType.LONG)
            .addSingleValueDimension("entries.description", DataType.STRING).addMultiValueDimension("d2", DataType.INT)
            .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hoursSinceEpoch"), null).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);

    // change delimiter
    inferredPinotSchema = AvroUtils
        .getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList(), "_", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addSingleValueDimension("tuple_streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple_city", DataType.STRING).addSingleValueDimension("entries", DataType.STRING)
            .addMultiValueDimension("d2", DataType.INT)
            .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hoursSinceEpoch"), null).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);

    // change the handling of collection-to-json option, d2 will become string
    inferredPinotSchema = AvroUtils
        .getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, fieldSpecMap, TimeUnit.HOURS,
            Lists.newArrayList("entries"), ".", ComplexTypeConfig.CollectionNotUnnestedToJson.ALL);
    expectedSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING).addMetric("m1", DataType.INT)
            .addSingleValueDimension("tuple.streetaddress", DataType.STRING)
            .addSingleValueDimension("tuple.city", DataType.STRING).addSingleValueDimension("entries.id", DataType.LONG)
            .addSingleValueDimension("entries.description", DataType.STRING)
            .addSingleValueDimension("d2", DataType.STRING)
            .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hoursSinceEpoch"), null).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);
  }
}
