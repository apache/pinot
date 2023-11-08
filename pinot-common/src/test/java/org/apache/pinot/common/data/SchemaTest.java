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
package org.apache.pinot.common.data;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.NullHandling;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.TimeGranularitySpec.TimeFormat;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


public class SchemaTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);

  @Test
  public void testValidation()
      throws Exception {
    Schema schemaToValidate;

    schemaToValidate = new Schema();
    schemaToValidate.addField(new DimensionFieldSpec("d", FieldSpec.DataType.LONG, true));
    schemaToValidate.addField(new MetricFieldSpec("m", FieldSpec.DataType.LONG));
    schemaToValidate.validate();

    schemaToValidate = new Schema();
    schemaToValidate.addField(new DimensionFieldSpec("d", FieldSpec.DataType.STRING, true));
    schemaToValidate.validate();

    schemaToValidate = new Schema();
    schemaToValidate.addField(new MetricFieldSpec("m", FieldSpec.DataType.STRING, "null"));
    try {
      schemaToValidate.validate();
      Assert.fail("Should have failed validation for invalid schema.");
    } catch (IllegalStateException e) {
      // expected
    }

    schemaToValidate = new Schema();
    schemaToValidate.addField(new DimensionFieldSpec("d", FieldSpec.DataType.BOOLEAN, true));
    schemaToValidate.validate();

    schemaToValidate = new Schema();
    schemaToValidate.addField(new MetricFieldSpec("m", FieldSpec.DataType.BOOLEAN, false));
    try {
      schemaToValidate.validate();
      Assert.fail("Should have failed validation for invalid schema.");
    } catch (IllegalStateException e) {
      // expected
    }

    schemaToValidate = new Schema();
    schemaToValidate.addField(new DimensionFieldSpec("d", FieldSpec.DataType.TIMESTAMP, true));
    schemaToValidate.validate();

    schemaToValidate = new Schema();
    schemaToValidate.addField(new MetricFieldSpec("m", FieldSpec.DataType.TIMESTAMP, new Timestamp(0)));
    try {
      schemaToValidate.validate();
      Assert.fail("Should have failed validation for invalid schema.");
    } catch (IllegalStateException e) {
      // expected
    }

    schemaToValidate = new Schema();
    schemaToValidate.addField(new MetricFieldSpec("d", FieldSpec.DataType.BIG_DECIMAL));
    schemaToValidate.validate();

    schemaToValidate = new Schema();
    schemaToValidate.addField(new MetricFieldSpec("m", FieldSpec.DataType.BIG_DECIMAL, BigDecimal.ZERO));
    schemaToValidate.validate();
  }

  @Test
  public void testSchemaBuilder() {
    String defaultString = "default";
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addMetric("svBigDecimalMetricWithDefault", FieldSpec.DataType.BIG_DECIMAL, BigDecimal.TEN)
        .addSingleValueDimension("svDimensionWithMaxLength", FieldSpec.DataType.STRING, 20000, null)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, defaultString)
        .addMultiValueDimension("mvDimensionWithMaxLength", FieldSpec.DataType.STRING, 20000, null)
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Lists.newArrayList("svDimension")).build();

    DimensionFieldSpec dimensionFieldSpec = schema.getDimensionSpec("svDimension");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "svDimension");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), Integer.MIN_VALUE);

    dimensionFieldSpec = schema.getDimensionSpec("svDimensionWithDefault");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "svDimensionWithDefault");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), 10);

    MetricFieldSpec metricFieldSpec = schema.getMetricSpec("svBigDecimalMetricWithDefault");
    Assert.assertNotNull(metricFieldSpec);
    Assert.assertEquals(metricFieldSpec.getFieldType(), FieldSpec.FieldType.METRIC);
    Assert.assertEquals(metricFieldSpec.getName(), "svBigDecimalMetricWithDefault");
    Assert.assertEquals(metricFieldSpec.getDataType(), FieldSpec.DataType.BIG_DECIMAL);
    Assert.assertTrue(metricFieldSpec.isSingleValueField());
    Assert.assertEquals(metricFieldSpec.getDefaultNullValue(), BigDecimal.TEN);

    dimensionFieldSpec = schema.getDimensionSpec("svDimensionWithMaxLength");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "svDimensionWithMaxLength");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertTrue(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getMaxLength(), 20000);
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), "null");

    dimensionFieldSpec = schema.getDimensionSpec("mvDimension");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "mvDimension");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertFalse(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), "null");

    dimensionFieldSpec = schema.getDimensionSpec("mvDimensionWithDefault");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "mvDimensionWithDefault");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertFalse(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), defaultString);

    dimensionFieldSpec = schema.getDimensionSpec("mvDimensionWithMaxLength");
    Assert.assertNotNull(dimensionFieldSpec);
    Assert.assertEquals(dimensionFieldSpec.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(dimensionFieldSpec.getName(), "mvDimensionWithMaxLength");
    Assert.assertEquals(dimensionFieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertFalse(dimensionFieldSpec.isSingleValueField());
    Assert.assertEquals(dimensionFieldSpec.getMaxLength(), 20000);
    Assert.assertEquals(dimensionFieldSpec.getDefaultNullValue(), "null");

    metricFieldSpec = schema.getMetricSpec("metric");
    Assert.assertNotNull(metricFieldSpec);
    Assert.assertEquals(metricFieldSpec.getFieldType(), FieldSpec.FieldType.METRIC);
    Assert.assertEquals(metricFieldSpec.getName(), "metric");
    Assert.assertEquals(metricFieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(metricFieldSpec.isSingleValueField());
    Assert.assertEquals(metricFieldSpec.getDefaultNullValue(), 0);

    metricFieldSpec = schema.getMetricSpec("metricWithDefault");
    Assert.assertNotNull(metricFieldSpec);
    Assert.assertEquals(metricFieldSpec.getFieldType(), FieldSpec.FieldType.METRIC);
    Assert.assertEquals(metricFieldSpec.getName(), "metricWithDefault");
    Assert.assertEquals(metricFieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(metricFieldSpec.isSingleValueField());
    Assert.assertEquals(metricFieldSpec.getDefaultNullValue(), 5);

    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    Assert.assertNotNull(timeFieldSpec);
    Assert.assertEquals(timeFieldSpec.getFieldType(), FieldSpec.FieldType.TIME);
    Assert.assertEquals(timeFieldSpec.getName(), "time");
    Assert.assertEquals(timeFieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertTrue(timeFieldSpec.isSingleValueField());
    Assert.assertEquals(timeFieldSpec.getDefaultNullValue(), Long.MIN_VALUE);

    DateTimeFieldSpec dateTimeFieldSpec = schema.getDateTimeSpec("dateTime");
    Assert.assertNotNull(dateTimeFieldSpec);
    Assert.assertEquals(dateTimeFieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
    Assert.assertEquals(dateTimeFieldSpec.getName(), "dateTime");
    Assert.assertEquals(dateTimeFieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertTrue(dateTimeFieldSpec.isSingleValueField());
    Assert.assertEquals(dateTimeFieldSpec.getDefaultNullValue(), Long.MIN_VALUE);
    Assert.assertEquals(dateTimeFieldSpec.getFormat(), "1:HOURS:EPOCH");
    Assert.assertEquals(dateTimeFieldSpec.getGranularity(), "1:HOURS");

    Assert.assertEquals(schema.getPrimaryKeyColumns(), Lists.newArrayList("svDimension"));
  }

  @Test
  public void testFetchFieldSpecForTime() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addMetric("metric", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime0", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .addDateTime("dateTime1", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("dateTime2", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    // Test method which fetches the DateTimeFieldSpec given the timeColumnName
    // Test is on TIME
    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn("time");
    Assert.assertNotNull(dateTimeFieldSpec);
    Assert.assertEquals(dateTimeFieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
    Assert.assertEquals(dateTimeFieldSpec.getName(), "time");
    Assert.assertEquals(dateTimeFieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertTrue(dateTimeFieldSpec.isSingleValueField());
    Assert.assertEquals(dateTimeFieldSpec.getDefaultNullValue(), Long.MIN_VALUE);
    Assert.assertEquals(dateTimeFieldSpec.getFormat(), "1:DAYS:EPOCH");
    Assert.assertEquals(dateTimeFieldSpec.getGranularity(), "1:DAYS");

    // Test it on DATE_TIME
    dateTimeFieldSpec = schema.getSpecForTimeColumn("dateTime0");
    Assert.assertNotNull(dateTimeFieldSpec);
    Assert.assertEquals(dateTimeFieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
    Assert.assertEquals(dateTimeFieldSpec.getName(), "dateTime0");
    Assert.assertEquals(dateTimeFieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertTrue(dateTimeFieldSpec.isSingleValueField());
    Assert.assertEquals(dateTimeFieldSpec.getDefaultNullValue(), Long.MIN_VALUE);
    Assert.assertEquals(dateTimeFieldSpec.getFormat(), "1:HOURS:EPOCH");
    Assert.assertEquals(dateTimeFieldSpec.getGranularity(), "1:HOURS");

    dateTimeFieldSpec = schema.getSpecForTimeColumn("dateTime1");
    Assert.assertNotNull(dateTimeFieldSpec);
    Assert.assertEquals(dateTimeFieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
    Assert.assertEquals(dateTimeFieldSpec.getName(), "dateTime1");
    Assert.assertEquals(dateTimeFieldSpec.getDataType(), FieldSpec.DataType.TIMESTAMP);
    Assert.assertTrue(dateTimeFieldSpec.isSingleValueField());
    Assert.assertEquals(dateTimeFieldSpec.getDefaultNullValue(), 0L);
    Assert.assertEquals(dateTimeFieldSpec.getFormat(), "1:MILLISECONDS:EPOCH");
    Assert.assertEquals(dateTimeFieldSpec.getGranularity(), "1:MILLISECONDS");

    dateTimeFieldSpec = schema.getSpecForTimeColumn("dateTime2");
    Assert.assertNotNull(dateTimeFieldSpec);
    Assert.assertEquals(dateTimeFieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
    Assert.assertEquals(dateTimeFieldSpec.getName(), "dateTime2");
    Assert.assertEquals(dateTimeFieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertTrue(dateTimeFieldSpec.isSingleValueField());
    Assert.assertEquals(dateTimeFieldSpec.getDefaultNullValue(), Integer.MIN_VALUE);
    Assert.assertEquals(dateTimeFieldSpec.getFormat(), "1:DAYS:EPOCH");
    Assert.assertEquals(dateTimeFieldSpec.getGranularity(), "1:DAYS");
  }

  @Test
  public void testSchemaBuilderAddTime() {
    String incomingName = "incoming";
    FieldSpec.DataType incomingDataType = FieldSpec.DataType.LONG;
    TimeUnit incomingTimeUnit = TimeUnit.HOURS;
    int incomingTimeUnitSize = 1;
    TimeGranularitySpec incomingTimeGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    String outgoingName = "outgoing";
    FieldSpec.DataType outgoingDataType = FieldSpec.DataType.INT;
    TimeUnit outgoingTimeUnit = TimeUnit.DAYS;
    int outgoingTimeUnitSize = 1;
    TimeGranularitySpec outgoingTimeGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);

    Schema schema11 =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addTime(incomingTimeGranularitySpec, null).build();
    Schema schema12 = new Schema.SchemaBuilder().setSchemaName("testSchema").build();
    schema12.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec));
    Assert.assertNotNull(schema11.getTimeFieldSpec());
    Assert.assertNotNull(schema12.getTimeFieldSpec());

    Assert.assertNotEquals(schema12, schema11);

    schema11 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, outgoingTimeGranularitySpec).build();
    Assert.assertEquals(schema11, schema12);
  }

  @Test
  public void testSerializeDeserialize()
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("schemaTest.schema");
    Assert.assertNotNull(resourceUrl);
    Schema schema = Schema.fromFile(new File(resourceUrl.getFile()));

    Schema schemaToCompare = Schema.fromString(schema.toPrettyJsonString());
    Assert.assertEquals(schemaToCompare, schema);
    Assert.assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    schemaToCompare = Schema.fromString(schema.toSingleLineJsonString());
    Assert.assertEquals(schemaToCompare, schema);
    Assert.assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    schemaToCompare = SchemaUtils.fromZNRecord(SchemaUtils.toZNRecord(schema));
    Assert.assertEquals(schemaToCompare, schema);
    Assert.assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    // When setting new fields, schema string should be updated
    String jsonSchema = schemaToCompare.toSingleLineJsonString();
    schemaToCompare.setSchemaName("newSchema");
    String jsonSchemaToCompare = schemaToCompare.toSingleLineJsonString();
    Assert.assertNotEquals(jsonSchemaToCompare, jsonSchema);
  }

  @Test
  public void testSerializeDeserializeOptions()
      throws IOException {
    String json = "{\n"
        + "  \"options\" : {\n"
        + "    \"nullHandling\" : {\n"
        + "      \"mode\" : \"column\",\n"
        + "      \"default\" : true\n"
        + "    }\n"
        + "  },\n"
        + "  \"primaryKeyColumns\" : null,\n"
        + "  \"timeFieldSpec\" : null,\n"
        + "  \"schemaName\" : null,\n"
        + "  \"dimensionFieldSpecs\" : [ ],\n"
        + "  \"metricFieldSpecs\" : [ ],\n"
        + "  \"dateTimeFieldSpecs\" : [ ]\n"
        + "}";
    JsonNode expectedNode = JsonUtils.stringToJsonNode(json);

    Schema schema = JsonUtils.jsonNodeToObject(expectedNode, Schema.class);
    Assert.assertEquals(schema.getOptions().getNullHandling(), new NullHandling.ColumnBased(true));
    JsonNode actualNode = JsonUtils.objectToJsonNode(schema);

    Assert.assertEquals(actualNode, expectedNode);
  }

  @Test
  public void testSimpleDateFormat()
      throws Exception {
    TimeGranularitySpec incomingTimeGranularitySpec =
        new TimeGranularitySpec(FieldSpec.DataType.STRING, 1, TimeUnit.DAYS,
            TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd", "Date");
    TimeGranularitySpec outgoingTimeGranularitySpec =
        new TimeGranularitySpec(FieldSpec.DataType.STRING, 1, TimeUnit.DAYS,
            TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd", "Date");
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, outgoingTimeGranularitySpec).build();
    String jsonSchema = schema.toSingleLineJsonString();
    Schema schemaFromJson = Schema.fromString(jsonSchema);
    Assert.assertEquals(schemaFromJson, schema);
    Assert.assertEquals(schemaFromJson.hashCode(), schema.hashCode());
  }

  @Test
  public void testByteType()
      throws Exception {
    Schema expectedSchema = new Schema();
    byte[] expectedEmptyDefault = new byte[0];
    byte[] expectedNonEmptyDefault = BytesUtils.toBytes("abcd1234");

    expectedSchema.setSchemaName("test");
    expectedSchema.addField(new MetricFieldSpec("noDefault", FieldSpec.DataType.BYTES));
    expectedSchema.addField(new MetricFieldSpec("emptyDefault", FieldSpec.DataType.BYTES, expectedEmptyDefault));
    expectedSchema.addField(new MetricFieldSpec("nonEmptyDefault", FieldSpec.DataType.BYTES, expectedNonEmptyDefault));

    // Ensure that schema can be serialized and de-serialized (ie byte[] converted to String and back).
    String jsonSchema = expectedSchema.toSingleLineJsonString();
    Schema actualSchema = Schema.fromString(jsonSchema);

    Assert.assertEquals(actualSchema.getFieldSpecFor("noDefault").getDefaultNullValue(), expectedEmptyDefault);
    Assert.assertEquals(actualSchema.getFieldSpecFor("emptyDefault").getDefaultNullValue(), expectedEmptyDefault);
    Assert.assertEquals(actualSchema.getFieldSpecFor("nonEmptyDefault").getDefaultNullValue(), expectedNonEmptyDefault);

    Assert.assertEquals(actualSchema, expectedSchema);
    Assert.assertEquals(actualSchema.hashCode(), expectedSchema.hashCode());
  }

  @Test
  public void testSchemaBackwardCompatibility() {
    Schema oldSchema = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();

    Assert.assertThrows(NullPointerException.class, () -> oldSchema.isBackwardCompatibleWith(null));

    // remove column
    Schema schema1 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        // Remove column svDimensionWithDefault
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertFalse(schema1.isBackwardCompatibleWith(oldSchema));

    // change column type
    Schema schema2 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.LONG, 10)  // INT -> LONG
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertFalse(schema2.isBackwardCompatibleWith(oldSchema));

    // change time column
    Schema schema3 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertTrue(schema3.isBackwardCompatibleWith(oldSchema));

    // change datetime column
    Schema schema4 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "2:HOURS:EPOCH", "1:HOURS").build();  // timeUnit 1 -> 2
    Assert.assertTrue(schema4.isBackwardCompatibleWith(oldSchema));

    // change default value
    Schema schema5 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 100) // default value 10 -> 100
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertTrue(schema5.isBackwardCompatibleWith(oldSchema));

    // add a new column
    Schema schema6 = new Schema.SchemaBuilder().addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addSingleValueDimension("svDimensionWithDefault1", FieldSpec.DataType.INT, 10)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT).addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "time"), null)
        .addDateTime("dateTime", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").build();
    Assert.assertTrue(schema6.isBackwardCompatibleWith(oldSchema));
  }

  @Test
  public void testStringToBooleanSchemaBackwardCompatibility() {
    Schema oldSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.STRING, "false").build();

    // INT to BOOLEAN - incompatible
    Schema newSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.STRING, "false").build();
    newSchema.updateBooleanFieldsIfNeeded(oldSchema);
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    // STRING to BOOLEAN - compatible
    newSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svString", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.STRING, "false").build();
    newSchema.updateBooleanFieldsIfNeeded(oldSchema);
    Assert.assertTrue(newSchema.isBackwardCompatibleWith(oldSchema));
    Assert.assertEquals(newSchema, oldSchema);

    // STRING with default to BOOLEAN with default - compatible
    newSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.BOOLEAN, "false").build();
    newSchema.updateBooleanFieldsIfNeeded(oldSchema);
    Assert.assertTrue(newSchema.isBackwardCompatibleWith(oldSchema));
    Assert.assertEquals(newSchema, oldSchema);

    // STRING with default to BOOLEAN without default - backward compatible change
    newSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.BOOLEAN).build();
    newSchema.updateBooleanFieldsIfNeeded(oldSchema);
    Assert.assertTrue(newSchema.isBackwardCompatibleWith(oldSchema));

    // New added BOOLEAN - compatible
    newSchema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", FieldSpec.DataType.INT)
        .addSingleValueDimension("svString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("svStringWithDefault", FieldSpec.DataType.STRING, "false")
        .addSingleValueDimension("svBoolean", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("svBooleanWithDefault", FieldSpec.DataType.BOOLEAN, true).build();
    newSchema.updateBooleanFieldsIfNeeded(oldSchema);
    Assert.assertTrue(newSchema.isBackwardCompatibleWith(oldSchema));
    Assert.assertEquals(newSchema.getFieldSpecFor("svBoolean").getDataType(), FieldSpec.DataType.BOOLEAN);
    Assert.assertEquals(newSchema.getFieldSpecFor("svBooleanWithDefault").getDataType(), FieldSpec.DataType.BOOLEAN);
  }
}
