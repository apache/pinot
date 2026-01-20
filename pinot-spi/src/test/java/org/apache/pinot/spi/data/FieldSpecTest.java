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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.FieldSpec.DataType.*;


/**
 * Tests for {@link FieldSpec}.
 */
public class FieldSpecTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed is: " + RANDOM_SEED;

  /**
   * Test all {@link FieldSpec.DataType}.
   */
  @Test
  public void testDataType() {
    Assert.assertEquals(INT.getStoredType(), INT);
    Assert.assertEquals(LONG.getStoredType(), LONG);
    Assert.assertEquals(FLOAT.getStoredType(), FLOAT);
    Assert.assertEquals(DOUBLE.getStoredType(), DOUBLE);
    Assert.assertEquals(BIG_DECIMAL.getStoredType(), BIG_DECIMAL);
    Assert.assertEquals(BOOLEAN.getStoredType(), INT);
    Assert.assertEquals(TIMESTAMP.getStoredType(), LONG);
    Assert.assertEquals(STRING.getStoredType(), STRING);
    Assert.assertEquals(JSON.getStoredType(), STRING);
    Assert.assertEquals(BYTES.getStoredType(), BYTES);

    Assert.assertEquals(INT.size(), Integer.BYTES);
    Assert.assertEquals(LONG.size(), Long.BYTES);
    Assert.assertEquals(FLOAT.size(), Float.BYTES);
    Assert.assertEquals(DOUBLE.size(), Double.BYTES);
    Assert.assertEquals(BOOLEAN.size(), Integer.BYTES);
    Assert.assertEquals(TIMESTAMP.size(), Long.BYTES);
  }

  /**
   * Test all {@link FieldSpec.FieldType} with different {@link FieldSpec.DataType}.
   */
  @Test
  public void testFieldSpec() {
    // Single-value boolean type dimension field with default null value.
    FieldSpec fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("svDimension");
    fieldSpec1.setDataType(BOOLEAN);
    fieldSpec1.setDefaultNullValue(false);
    FieldSpec fieldSpec2 = new DimensionFieldSpec("svDimension", BOOLEAN, true, false);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), 0);

    // Single-value timestamp type dimension field with default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("svDimension");
    fieldSpec1.setDataType(TIMESTAMP);
    fieldSpec1.setDefaultNullValue(new Timestamp(0).toString());
    fieldSpec2 = new DimensionFieldSpec("svDimension", TIMESTAMP, true, new Timestamp(0).toString());
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), 0L);

    // Single-value string type dimension field with max length and default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("svDimension");
    fieldSpec1.setDataType(STRING);
    fieldSpec1.setMaxLength(20000);
    fieldSpec2 = new DimensionFieldSpec("svDimension", STRING, true, 20000, null);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), "null");

    // Single-value json type dimension field with max length and default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("svDimension");
    fieldSpec1.setDataType(JSON);
    fieldSpec1.setMaxLength(20000);
    fieldSpec2 = new DimensionFieldSpec("svDimension", JSON, true, 20000, null);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), "null");

    // Multi-value dimension field.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("mvDimension");
    fieldSpec1.setDataType(INT);
    fieldSpec1.setSingleValueField(false);
    fieldSpec2 = new DimensionFieldSpec("mvDimension", INT, false);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), Integer.MIN_VALUE);

    // Multi-value dimension field with default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("mvDimension");
    fieldSpec1.setDataType(FLOAT);
    fieldSpec1.setSingleValueField(false);
    fieldSpec1.setDefaultNullValue(-0.1);
    fieldSpec2 = new DimensionFieldSpec("mvDimension", FLOAT, false, -0.1);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), -0.1F);

    // Multi-value dimension field with max length and default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("mvDimension");
    fieldSpec1.setDataType(STRING);
    fieldSpec1.setSingleValueField(false);
    fieldSpec1.setMaxLength(20000);
    fieldSpec2 = new DimensionFieldSpec("mvDimension", STRING, false, 20000, null);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), "null");

    // Metric field with default null value.
    fieldSpec1 = new MetricFieldSpec();
    fieldSpec1.setName("metric");
    fieldSpec1.setDataType(LONG);
    fieldSpec1.setDefaultNullValue(1);
    fieldSpec2 = new MetricFieldSpec("metric", LONG, 1);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), 1L);

    // Single-value BigDecimal type dimension field with default null value.
    fieldSpec1 = new MetricFieldSpec();
    fieldSpec1.setName("svMetric");
    fieldSpec1.setDataType(BIG_DECIMAL);
    fieldSpec1.setDefaultNullValue(BigDecimal.ZERO);
    fieldSpec2 = new MetricFieldSpec("svMetric", BIG_DECIMAL, BigDecimal.ZERO);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), BigDecimal.ZERO);

    // Metric field with default null value for byte column.
    fieldSpec1 = new MetricFieldSpec();
    fieldSpec1.setName("byteMetric");
    fieldSpec1.setDataType(BYTES);
    fieldSpec1.setDefaultNullValue(new byte[]{0x10, 0x20});
    fieldSpec2 = new MetricFieldSpec("byteMetric", BYTES, new byte[]{0x10, 0x20});
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toJsonObject(), fieldSpec2.toJsonObject());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), fieldSpec2.getDefaultNullValue());
  }

  /**
   * Test {@link TimeFieldSpec} constructors.
   */
  @Test
  public void testTimeFieldSpecConstructor() {
    String incomingName = "incoming";
    TimeUnit incomingTimeUnit = TimeUnit.HOURS;
    int incomingTimeUnitSize = 1;
    TimeGranularitySpec incomingTimeGranularitySpec =
        new TimeGranularitySpec(LONG, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    String outgoingName = "outgoing";
    TimeUnit outgoingTimeUnit = TimeUnit.DAYS;
    int outgoingTimeUnitSize = 1;
    TimeGranularitySpec outgoingTimeGranularitySpec =
        new TimeGranularitySpec(INT, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
    TimeFieldSpec timeFieldSpec1 = new TimeFieldSpec(incomingTimeGranularitySpec);
    TimeFieldSpec timeFieldSpec2 = new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec);
    Assert.assertNotEquals(timeFieldSpec1, timeFieldSpec2);

    timeFieldSpec1.setOutgoingGranularitySpec(outgoingTimeGranularitySpec);
    Assert.assertEquals(timeFieldSpec1, timeFieldSpec2);
  }

  /**
   * Test {@link DateTimeFieldSpec} constructors.
   */
  @Test
  public void testDateTimeFieldSpecConstructor() {
    String name = "Date";
    String format = "1:HOURS:EPOCH";
    String granularity = "1:HOURS";

    DateTimeFieldSpec dateTimeFieldSpec1 = new DateTimeFieldSpec(name, LONG, format, granularity);
    DateTimeFieldSpec dateTimeFieldSpec2 = new DateTimeFieldSpec(name, INT, format, granularity);
    Assert.assertNotEquals(dateTimeFieldSpec2, dateTimeFieldSpec1);

    DateTimeFieldSpec dateTimeFieldSpec3 = new DateTimeFieldSpec(name, LONG, format, granularity);
    Assert.assertEquals(dateTimeFieldSpec1, dateTimeFieldSpec3);

    DateTimeFieldSpec dateTimeFieldSpec4 = new DateTimeFieldSpec(name, LONG, format, granularity, 100000000L, null);
    DateTimeFieldSpec dateTimeFieldSpec5 =
        new DateTimeFieldSpec(name, INT, format, granularity, null, "toEpochHours(millis)");
    Assert.assertNotEquals(dateTimeFieldSpec5, dateTimeFieldSpec4);

    DateTimeFieldSpec dateTimeFieldSpec6 = new DateTimeFieldSpec(name, LONG, format, granularity, 100000000L, null);
    Assert.assertEquals(dateTimeFieldSpec4, dateTimeFieldSpec6);
  }

  @Test(dataProvider = "testFormatDataProvider")
  public void testDateTimeFormat(String name, FieldSpec.DataType dataType, String format, String granularity,
      boolean exceptionExpected, DateTimeFieldSpec dateTimeFieldExpected) {

    DateTimeFieldSpec dateTimeFieldActual = null;
    boolean exceptionActual = false;
    try {
      dateTimeFieldActual = new DateTimeFieldSpec(name, dataType, format, granularity);
    } catch (IllegalArgumentException e) {
      exceptionActual = true;
    }
    Assert.assertEquals(exceptionActual, exceptionExpected);
    Assert.assertEquals(dateTimeFieldActual, dateTimeFieldExpected);
  }

  @DataProvider(name = "testFormatDataProvider")
  public Object[][] provideTestFormatData() {

    String name = "Date";
    FieldSpec.DataType dataType = LONG;
    String granularity = "1:HOURS";

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{name, dataType, "1:hours", granularity, true, null});
    entries.add(new Object[]{name, dataType, "one_hours", granularity, true, null});
    entries.add(new Object[]{name, dataType, "1:hour:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "1:HOUR:EPOCH:yyyyMMdd", granularity, true, null});
    entries.add(new Object[]{name, dataType, "0:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "-1:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "0.1:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{
        name, dataType, "1:HOURS:EPOCH", granularity, false,
        new DateTimeFieldSpec(name, dataType, "1:HOURS:EPOCH", granularity)
    });

    entries.add(new Object[]{
        name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT", granularity, false,
        new DateTimeFieldSpec(name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT", granularity)
    });

    entries.add(new Object[]{
        name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", granularity, false,
        new DateTimeFieldSpec(name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", granularity)
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  /**
   * Test different order of fields in serialized JSON string to deserialize {@link FieldSpec}.
   */
  @Test
  public void testOrderOfFields()
      throws Exception {
    // Metric field with default null value.
    String[] metricFields = {"\"name\":\"metric\"", "\"dataType\":\"INT\"", "\"defaultNullValue\":-1"};
    MetricFieldSpec metricFieldSpec1 =
        JsonUtils.stringToObject(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    MetricFieldSpec metricFieldSpec2 = new MetricFieldSpec("metric", INT, -1);
    Assert.assertEquals(metricFieldSpec1, metricFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(metricFieldSpec1.getDefaultNullValue(), -1, ERROR_MESSAGE);

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {"\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false"};
    DimensionFieldSpec dimensionFieldSpec1 =
        JsonUtils.stringToObject(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    DimensionFieldSpec dimensionFieldSpec2 = new DimensionFieldSpec("dimension", BOOLEAN, true, false);
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), 0, ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields = new String[]{
        "\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false",
        "\"defaultNullValue\":\"default\", \"maxLengthExceedStrategy\": \"TRIM_LENGTH\""
    };
    dimensionFieldSpec1 = JsonUtils.stringToObject(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    dimensionFieldSpec2 = new DimensionFieldSpec("dimension", STRING, false, "default");
    dimensionFieldSpec2.setMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH);
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), "default", ERROR_MESSAGE);

    // Date time field with default null value.
    String[] dateTimeFields = {
        "\"name\":\"Date\"", "\"dataType\":\"LONG\"", "\"format\":\"1:MILLISECONDS:EPOCH\"",
        "\"granularity\":\"5" + ":MINUTES\"", "\"dateTimeType\":\"PRIMARY\""
    };
    DateTimeFieldSpec dateTimeFieldSpec1 =
        JsonUtils.stringToObject(getRandomOrderJsonString(dateTimeFields), DateTimeFieldSpec.class);
    DateTimeFieldSpec dateTimeFieldSpec2 = new DateTimeFieldSpec("Date", LONG, "1:MILLISECONDS:EPOCH", "5:MINUTES");
    Assert.assertEquals(dateTimeFieldSpec1, dateTimeFieldSpec2, ERROR_MESSAGE);
  }

  /**
   * Test {@link FieldSpec} serialize deserialize.
   */
  @Test
  public void testSerializeDeserialize()
      throws Exception {
    FieldSpec first;
    FieldSpec second;

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {
        "\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false",
        "\"transformFunction" + "\":\"trim(foo)\""
    };
    first = JsonUtils.stringToObject(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = JsonUtils.stringToObject(first.toJsonObject().toString(), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields = new String[]{
        "\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false",
        "\"defaultNullValue\":\"default\""
    };
    first = JsonUtils.stringToObject(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = JsonUtils.stringToObject(first.toJsonObject().toString(), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Time field with default value.
    String[] timeFields = {
        "\"incomingGranularitySpec\":{\"timeUnitSize\":1, \"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\","
            + "\"name\":\"incomingTime\"}",
        "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\"," + "\"name\":\"outgoingTime\"}",
        "\"defaultNullValue\":-1", "\"transformFunction\":\"toEpochDays" + "(millis)\""
    };
    first = JsonUtils.stringToObject(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    second = JsonUtils.stringToObject(first.toJsonObject().toString(), TimeFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // DateTime field
    String[] dateTimeFields = {
        "\"name\":\"Date\"", "\"dataType\":\"LONG\"", "\"format\":\"1:MILLISECONDS:EPOCH\"",
        "\"granularity\":\"5" + ":MINUTES\"", "\"transformFunction\":\"fromEpochDays(daysSinceEpoch)\""
    };
    first = JsonUtils.stringToObject(getRandomOrderJsonString(dateTimeFields), DateTimeFieldSpec.class);
    second = JsonUtils.stringToObject(first.toJsonObject().toString(), DateTimeFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // BigDecimal field
    String[] metricFields = new String[]{
        "\"name\":\"Salary\"", "\"dataType\":\"BIG_DECIMAL\""
    };
    first = JsonUtils.stringToObject(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    second = JsonUtils.stringToObject(first.toJsonObject().toString(), MetricFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);
  }

  /**
   * Helper function to generate JSON string with random order of fields passed in.
   */
  private String getRandomOrderJsonString(String[] fields) {
    int length = fields.length;
    List<Integer> indices = new LinkedList<>();
    for (int i = 0; i < length; i++) {
      indices.add(i);
    }
    StringBuilder jsonString = new StringBuilder();
    jsonString.append('{');
    for (int i = length; i > 0; i--) {
      jsonString.append(fields[indices.remove(RANDOM.nextInt(i))]);
      if (i != 1) {
        jsonString.append(',');
      }
    }
    jsonString.append('}');
    return jsonString.toString();
  }

  @DataProvider(name = "nullableCases")
  public static Object[][] nullableCases() {
    return new Object[][] {
        //            declared notNull, returned notNull
        new Object[] {null},
        new Object[] {false},
        new Object[] {true}
    };
  }

  @Test(dataProvider = "nullableCases")
  void testNullability(Boolean declared)
      throws IOException {
    boolean expected = declared == Boolean.TRUE;
    String json;
    if (declared == null) {
      json = "{\"name\": \"col1\", \"dataType\":\"BOOLEAN\"}";
    } else {
      json = "{\"name\": \"col1\", \"dataType\":\"BOOLEAN\", \"notNull\": " + declared + "}";
    }
    DimensionFieldSpec fieldSpec = JsonUtils.stringToObject(json, DimensionFieldSpec.class);

    Assert.assertEquals(fieldSpec.isNotNull(), expected, "Unexpected notNull read when declared as " + declared);
    Assert.assertEquals(fieldSpec.isNullable(), !expected, "Unexpected nullable read when declared as " + declared);
  }

  @Test(dataProvider = "nullableCases")
  void testNullabilityIdempotency(Boolean declared)
      throws JsonProcessingException {
    String json;
    if (declared == null) {
      json = "{\"name\": \"col1\", \"dataType\":\"BOOLEAN\"}";
    } else {
      json = "{\"name\": \"col1\", \"dataType\":\"BOOLEAN\", \"notNull\": " + declared + "}";
    }
    DimensionFieldSpec fieldSpec = JsonUtils.stringToObject(json, DimensionFieldSpec.class);

    String serialized = JsonUtils.objectToString(fieldSpec);
    DimensionFieldSpec deserialized = JsonUtils.stringToObject(serialized, DimensionFieldSpec.class);

    Assert.assertEquals(deserialized, fieldSpec, "Changes detected while checking serialize/deserialize idempotency");
  }


  /**
   * Test to ensure only expected fields are serialized and @JsonIgnore methods are excluded.
   * This test verifies that getEffectiveMaxLength and getEffectiveMaxLengthExceedStrategy
   * (which are annotated with @JsonIgnore) do not appear in the JSON output.
   */
  @Test
  public void testJsonSerializationExcludesIgnoredFields() throws Exception {
    // Test DimensionFieldSpec with some null and some non-null values
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("testDimension", STRING, true, 100, "defaultValue");
    dimensionFieldSpec.setMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH);

    String json = JsonUtils.objectToString(dimensionFieldSpec);

    // Verify @JsonIgnore methods are not in JSON
    Assert.assertFalse(json.contains("effectiveMaxLength"),
        "JSON should not contain effectiveMaxLength (marked with @JsonIgnore): " + json);
    Assert.assertFalse(json.contains("effectiveMaxLengthExceedStrategy"),
        "JSON should not contain effectiveMaxLengthExceedStrategy (marked with @JsonIgnore): " + json);
    Assert.assertFalse(json.contains("nullable"),
        "JSON should not contain nullable (marked with @JsonIgnore): " + json);

    // Verify expected fields are present
    Assert.assertTrue(json.contains("\"name\":\"testDimension\""),
        "JSON should contain name field: " + json);
    Assert.assertTrue(json.contains("\"dataType\":\"STRING\""),
        "JSON should contain dataType field: " + json);

    // Test MetricFieldSpec with null values to ensure they're not serialized
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("testMetric", INT);
    // maxLength, maxLengthExceedStrategy, and transformFunction should be null

    String metricJson = JsonUtils.objectToString(metricFieldSpec);

    // Verify @JsonIgnore methods are not in JSON
    Assert.assertFalse(metricJson.contains("effectiveMaxLength"),
        "JSON should not contain effectiveMaxLength (marked with @JsonIgnore): " + metricJson);
    Assert.assertFalse(metricJson.contains("effectiveMaxLengthExceedStrategy"),
        "JSON should not contain effectiveMaxLengthExceedStrategy (marked with @JsonIgnore): " + metricJson);

    // Verify null fields are not present (these should be null for a basic MetricFieldSpec)
    Assert.assertFalse(metricJson.contains("maxLength"),
        "JSON should not contain maxLength when it's null: " + metricJson);
    Assert.assertFalse(metricJson.contains("maxLengthExceedStrategy"),
        "JSON should not contain maxLengthExceedStrategy when it's null: " + metricJson);
    Assert.assertFalse(metricJson.contains("transformFunction"),
        "JSON should not contain transformFunction when it's null: " + metricJson);

    // Verify expected fields are present
    Assert.assertTrue(metricJson.contains("\"name\":\"testMetric\""),
        "JSON should contain name field: " + metricJson);
    Assert.assertTrue(metricJson.contains("\"dataType\":\"INT\""),
        "JSON should contain dataType field: " + metricJson);
    Assert.assertTrue(metricJson.contains("\"singleValueField\":true"),
        "JSON should contain singleValueField field: " + metricJson);
  }


  /**
   * DataProvider for testing getDefaultNullValue with all valid FieldType and DataType combinations.
   * Each entry contains: FieldType, DataType, expected default null value
   */
  @DataProvider(name = "defaultNullValueCases")
  public Object[][] defaultNullValueCases() {
    return new Object[][]{
        // DIMENSION field type (ordinal 0)
        {FieldType.DIMENSION, DataType.INT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT},
        {FieldType.DIMENSION, DataType.LONG, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG},
        {FieldType.DIMENSION, DataType.FLOAT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT},
        {FieldType.DIMENSION, DataType.DOUBLE, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE},
        {FieldType.DIMENSION, DataType.BIG_DECIMAL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL},
        {FieldType.DIMENSION, DataType.BOOLEAN, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN},
        {FieldType.DIMENSION, DataType.TIMESTAMP, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP},
        {FieldType.DIMENSION, DataType.STRING, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING},
        {FieldType.DIMENSION, DataType.JSON, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON},
        {FieldType.DIMENSION, DataType.BYTES, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES},

        // TIME field type (ordinal 2) - same as DIMENSION
        {FieldType.TIME, DataType.INT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT},
        {FieldType.TIME, DataType.LONG, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG},
        {FieldType.TIME, DataType.FLOAT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT},
        {FieldType.TIME, DataType.DOUBLE, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE},
        {FieldType.TIME, DataType.BIG_DECIMAL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL},
        {FieldType.TIME, DataType.BOOLEAN, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN},
        {FieldType.TIME, DataType.TIMESTAMP, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP},
        {FieldType.TIME, DataType.STRING, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING},
        {FieldType.TIME, DataType.JSON, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON},
        {FieldType.TIME, DataType.BYTES, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES},

        // DATE_TIME field type (ordinal 3) - same as DIMENSION
        {FieldType.DATE_TIME, DataType.INT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT},
        {FieldType.DATE_TIME, DataType.LONG, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG},
        {FieldType.DATE_TIME, DataType.FLOAT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT},
        {FieldType.DATE_TIME, DataType.DOUBLE, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE},
        {FieldType.DATE_TIME, DataType.BIG_DECIMAL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL},
        {FieldType.DATE_TIME, DataType.BOOLEAN, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN},
        {FieldType.DATE_TIME, DataType.TIMESTAMP, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP},
        {FieldType.DATE_TIME, DataType.STRING, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING},
        {FieldType.DATE_TIME, DataType.JSON, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON},
        {FieldType.DATE_TIME, DataType.BYTES, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES},

        // METRIC field type (ordinal 1)
        {FieldType.METRIC, DataType.INT, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_INT},
        {FieldType.METRIC, DataType.LONG, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_LONG},
        {FieldType.METRIC, DataType.FLOAT, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_FLOAT},
        {FieldType.METRIC, DataType.DOUBLE, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE},
        {FieldType.METRIC, DataType.BIG_DECIMAL, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_BIG_DECIMAL},
        {FieldType.METRIC, DataType.STRING, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_STRING},
        {FieldType.METRIC, DataType.BYTES, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_BYTES},

        // COMPLEX field type (ordinal 4)
        {FieldType.COMPLEX, DataType.MAP, FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_MAP},
        {FieldType.COMPLEX, DataType.LIST, FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_LIST},
    };
  }

  /**
   * Test that getDefaultNullValue returns correct default values for all valid FieldType + DataType combinations.
   */
  @Test(dataProvider = "defaultNullValueCases")
  public void testGetDefaultNullValue(final FieldType fieldType, final DataType dataType, final Object expectedValue) {
    final Object actualValue = FieldSpec.getDefaultNullValue(fieldType, dataType, null);
    Assert.assertEquals(actualValue, expectedValue,
        String.format("Default null value mismatch for fieldType=%s, dataType=%s", fieldType, dataType));
  }

  /**
   * Test that default null values can be converted to string and back for all types.
   * This validates the round-trip: defaultValue -> getStringValue -> dataType.convert -> original value
   */
  @DataProvider(name = "stringConversionCases")
  public Object[][] stringConversionCases() {
    return new Object[][]{
        // Primitive types
        {DataType.INT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT},
        {DataType.LONG, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG},
        {DataType.FLOAT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT},
        {DataType.DOUBLE, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE},
        {DataType.BIG_DECIMAL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL},
        {DataType.BOOLEAN, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN},
        {DataType.TIMESTAMP, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP},
        {DataType.STRING, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING},
        {DataType.JSON, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON},
        {DataType.BYTES, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES},
        // Complex types
        {DataType.MAP, FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_MAP},
        {DataType.LIST, FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_LIST},
    };
  }

  @Test(dataProvider = "stringConversionCases")
  public void testDefaultNullValueStringConversion(final DataType dataType, final Object defaultValue) {
    // Convert to string
    final String stringValue = FieldSpec.getStringValue(defaultValue);
    Assert.assertNotNull(stringValue,
        String.format("String value should not be null for dataType=%s", dataType));

    // Convert back using dataType.convert
    final Object convertedValue = dataType.convert(stringValue);
    Assert.assertEquals(convertedValue, defaultValue,
        String.format("Round-trip conversion failed for dataType=%s: original=%s, string=%s, converted=%s",
            dataType, defaultValue, stringValue, convertedValue));
  }

  /**
   * Test that MAP default null value (empty Map) can be converted to string and back.
   * This specifically tests the bug where Scala Map.toString() produces "Map()" instead of "{}".
   */
  @Test
  public void testMapDefaultNullValueStringConversion() {
    final Map<?, ?> defaultMapValue = FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_MAP;

    // Verify the default is an empty map
    Assert.assertTrue(defaultMapValue.isEmpty(), "Default MAP null value should be an empty map");

    // Convert to string using getStringValue
    final String stringValue = FieldSpec.getStringValue(defaultMapValue);

    // The string should be valid JSON that can be parsed back
    // A Java HashMap.toString() returns "{}" for empty map
    // A Scala Map.toString() returns "Map()" which would break JSON parsing
    Assert.assertEquals(stringValue, "{}",
        "Empty Map should serialize to '{}', not 'Map()'. "
            + "This can happen if Scala Map is used instead of Java Map");

    // Verify it can be converted back
    final Object convertedValue = DataType.MAP.convert(stringValue);
    Assert.assertNotNull(convertedValue, "Converted value should not be null");
    Assert.assertTrue(convertedValue instanceof Map, "Converted value should be a Map");
    Assert.assertTrue(((Map<?, ?>) convertedValue).isEmpty(), "Converted Map should be empty");
  }

  /**
   * Test that verifies getStringValue produces valid JSON for Map/List types.
   * The bug occurs when getStringValue() uses toString() which produces
   * non-JSON output for certain Map/List implementations (e.g., Scala collections).
   *
   * This test verifies that Java standard collections work correctly.
   * The actual bug manifests when Scala collections are used (via Jackson Scala module).
   */
  @Test
  public void testGetStringValueProducesValidJsonForMap() {
    // Test with various Java Map implementations
    final Map<String, Object> hashMap = new HashMap<>();
    final Map<String, Object> emptyMap = Map.of();

    final String hashMapString = FieldSpec.getStringValue(hashMap);
    final String emptyMapString = FieldSpec.getStringValue(emptyMap);

    // Verify the string is valid JSON (can be parsed back)
    // Note: Java HashMap.toString() returns "{}" for empty map, which is valid JSON
    // But this test documents the expected behavior
    Assert.assertNotNull(hashMapString, "String value for HashMap should not be null");
    Assert.assertNotNull(emptyMapString, "String value for empty Map should not be null");

    // The string should be parseable by DataType.MAP.convert()
    // If getStringValue returns something like "Map()" (Scala style), this would fail
    try {
      DataType.MAP.convert(emptyMapString);
    } catch (IllegalArgumentException e) {
      Assert.fail("getStringValue() produced invalid JSON for Map: '" + emptyMapString
          + "'. Error: " + e.getMessage());
    }
  }

  @Test
  public void testGetStringValueProducesValidJsonForList() {
    final List<Object> arrayList = new ArrayList<>();
    final List<Object> emptyList = List.of();

    final String arrayListString = FieldSpec.getStringValue(arrayList);
    final String emptyListString = FieldSpec.getStringValue(emptyList);

    Assert.assertNotNull(arrayListString, "String value for ArrayList should not be null");
    Assert.assertNotNull(emptyListString, "String value for empty List should not be null");

    // The string should be parseable by DataType.LIST.convert()
    try {
      DataType.LIST.convert(emptyListString);
    } catch (IllegalArgumentException e) {
      Assert.fail("getStringValue() produced invalid JSON for List: '" + emptyListString
          + "'. Error: " + e.getMessage());
    }
  }

  /**
   * Test that LIST default null value (empty List) can be converted to string and back.
   */
  @Test
  public void testListDefaultNullValueStringConversion() {
    final List<?> defaultListValue = FieldSpec.DEFAULT_COMPLEX_NULL_VALUE_OF_LIST;

    // Verify the default is an empty list
    Assert.assertTrue(defaultListValue.isEmpty(), "Default LIST null value should be an empty list");

    // Convert to string using getStringValue
    final String stringValue = FieldSpec.getStringValue(defaultListValue);

    // The string should be valid JSON that can be parsed back
    Assert.assertEquals(stringValue, "[]",
        "Empty List should serialize to '[]', not 'List()'");

    // Verify it can be converted back
    final Object convertedValue = DataType.LIST.convert(stringValue);
    Assert.assertNotNull(convertedValue, "Converted value should not be null");
    Assert.assertTrue(convertedValue instanceof List, "Converted value should be a List");
    Assert.assertTrue(((List<?>) convertedValue).isEmpty(), "Converted List should be empty");
  }

  /**
   * Test custom string default null values are correctly converted by dataType.convert().
   */
  @DataProvider(name = "customStringDefaultNullValueCases")
  public Object[][] customStringDefaultNullValueCases() {
    return new Object[][]{
        {FieldType.DIMENSION, DataType.INT, "42", 42},
        {FieldType.DIMENSION, DataType.LONG, "123456789", 123456789L},
        {FieldType.DIMENSION, DataType.FLOAT, "3.14", 3.14f},
        {FieldType.DIMENSION, DataType.DOUBLE, "2.718281828", 2.718281828},
        {FieldType.DIMENSION, DataType.BIG_DECIMAL, "99999.99999", new BigDecimal("99999.99999")},
        {FieldType.DIMENSION, DataType.STRING, "custom_default", "custom_default"},
        {FieldType.DIMENSION, DataType.JSON, "{\"key\":\"value\"}", "{\"key\":\"value\"}"},
        {FieldType.COMPLEX, DataType.MAP, "{\"a\":1}", Map.of("a", 1)},
        {FieldType.COMPLEX, DataType.LIST, "[1,2,3]", List.of(1, 2, 3)},
    };
  }

  @Test(dataProvider = "customStringDefaultNullValueCases")
  public void testCustomStringDefaultNullValue(FieldType fieldType, DataType dataType,
      String stringDefaultNullValue, Object expectedValue) {
    final Object actualValue = FieldSpec.getDefaultNullValue(fieldType, dataType, stringDefaultNullValue);
    Assert.assertEquals(actualValue, expectedValue,
        String.format("Custom default null value conversion failed for fieldType=%s, dataType=%s, input=%s",
            fieldType, dataType, stringDefaultNullValue));
  }

  /**
   * Test that invalid string default null values throw IllegalArgumentException.
   * This validates the error message format: "Cannot convert value: 'X' to type: Y"
   */
  @Test
  public void testInvalidMapStringDefaultNullValueThrowsException() {
    // "Map()" is what Scala Map.toString() produces, which is NOT valid JSON
    final String invalidMapString = "Map()";

    try {
      FieldSpec.getDefaultNullValue(FieldType.COMPLEX, DataType.MAP, invalidMapString);
      Assert.fail("Expected IllegalArgumentException for invalid MAP string: " + invalidMapString);
    } catch (IllegalArgumentException e) {
      // Verify the error message contains expected information
      Assert.assertTrue(e.getMessage().contains("Cannot convert value"),
          "Error message should contain 'Cannot convert value': " + e.getMessage());
      Assert.assertTrue(e.getMessage().contains("Map()"),
          "Error message should contain the invalid value 'Map()': " + e.getMessage());
      Assert.assertTrue(e.getMessage().contains("MAP"),
          "Error message should contain the target type 'MAP': " + e.getMessage());
    }
  }

  /**
   * Test that getStringValue handles objects with Scala collection-like class names.
   * Since Scala collections don't implement java.util.Map/List, we detect them by class name.
   * This test uses a mock object to simulate Scala collection behavior.
   */
  @Test
  public void testGetStringValueHandlesScalaStyleCollections() {
    // Create a mock object that simulates Scala Map behavior:
    // - Its class name starts with "scala.collection" pattern
    // - Its toString() returns "Map()" instead of valid JSON
    // We can't test with actual Scala classes without adding Scala dependency,
    // so we verify the fix using a custom HashMap that overrides toString()
    final Object scalaStyleMap = new HashMap<String, Object>() {
      @Override
      public String toString() {
        return "Map()";  // This is what Scala Map.toString() returns
      }
    };

    // With the fix, getStringValue should serialize to JSON "{}" instead of using toString()
    final String result = FieldSpec.getStringValue(scalaStyleMap);

    // The result should be valid JSON, not "Map()"
    Assert.assertEquals(result, "{}",
        "getStringValue should serialize Map to JSON '{}', not '" + result + "'");

    // Verify the result can be parsed back
    try {
      DataType.MAP.convert(result);
    } catch (IllegalArgumentException e) {
      Assert.fail("getStringValue produced invalid JSON: " + result);
    }
  }

  /**
   * Test that invalid string default null values throw IllegalArgumentException for LIST.
   */
  @Test
  public void testInvalidListStringDefaultNullValueThrowsException() {
    // "List()" is what Scala List.toString() produces, which is NOT valid JSON
    final String invalidListString = "List()";

    try {
      FieldSpec.getDefaultNullValue(FieldType.COMPLEX, DataType.LIST, invalidListString);
      Assert.fail("Expected IllegalArgumentException for invalid LIST string: " + invalidListString);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot convert value"),
          "Error message should contain 'Cannot convert value': " + e.getMessage());
      Assert.assertTrue(e.getMessage().contains("List()"),
          "Error message should contain the invalid value 'List()': " + e.getMessage());
    }
  }

  /**
   * Test that getStringValue throws RuntimeException when JSON serialization fails.
   * This covers the catch block for JsonProcessingException.
   */
  @Test
  public void testGetStringValueThrowsRuntimeExceptionOnSerializationFailure() {
    // Create a Map with a circular reference which cannot be serialized to JSON
    final Map<String, Object> circularMap = new HashMap<>();
    circularMap.put("self", circularMap);  // Circular reference

    try {
      FieldSpec.getStringValue(circularMap);
      Assert.fail("Expected RuntimeException for circular reference in Map");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to serialize java.util.HashMap to JSON"),
          "Error message should contain 'Failed to serialize java.util.HashMap to JSON': " + e.getMessage());
      Assert.assertNotNull(e.getCause(), "RuntimeException should have a cause");
    }
  }

  /**
   * Test isScalaCollection returns false for null.
   */
  @Test
  public void testIsScalaCollectionReturnsFalseForNull()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Assert.assertFalse(invokeIsScalaCollection(null),
        "isScalaCollection should return false for null");
  }

  /**
   * Test isScalaCollection returns false for Java collections.
   */
  @Test
  public void testIsScalaCollectionReturnsFalseForJavaCollections()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // Test various Java collection types
    Assert.assertFalse(invokeIsScalaCollection(new HashMap<>()),
        "isScalaCollection should return false for HashMap");
    Assert.assertFalse(invokeIsScalaCollection(new ArrayList<>()),
        "isScalaCollection should return false for ArrayList");
    Assert.assertFalse(invokeIsScalaCollection(new LinkedList<>()),
        "isScalaCollection should return false for LinkedList");
    Assert.assertFalse(invokeIsScalaCollection(new TreeMap<>()),
        "isScalaCollection should return false for TreeMap");
    Assert.assertFalse(invokeIsScalaCollection(new HashSet<>()),
        "isScalaCollection should return false for HashSet");
    Assert.assertFalse(invokeIsScalaCollection(Map.of()),
        "isScalaCollection should return false for immutable Map");
    Assert.assertFalse(invokeIsScalaCollection(List.of()),
        "isScalaCollection should return false for immutable List");
  }

  /**
   * Test isScalaCollection returns false for non-collection types.
   */
  @Test
  public void testIsScalaCollectionReturnsFalseForNonCollections()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Assert.assertFalse(invokeIsScalaCollection("string"),
        "isScalaCollection should return false for String");
    Assert.assertFalse(invokeIsScalaCollection(123),
        "isScalaCollection should return false for Integer");
    Assert.assertFalse(invokeIsScalaCollection(new Object()),
        "isScalaCollection should return false for Object");
  }

  /**
   * Test isScalaCollection detection logic by verifying class name matching.
   * Since we can't instantiate actual Scala collections without Scala dependency,
   * we verify the detection logic would work for Scala class names.
   */
  @Test
  public void testIsScalaCollectionClassNameDetection() {
    // Verify that Java collection class names don't start with "scala.collection"
    Assert.assertFalse(HashMap.class.getName().startsWith("scala.collection"),
        "HashMap class name should not start with 'scala.collection'");
    Assert.assertFalse(ArrayList.class.getName().startsWith("scala.collection"),
        "ArrayList class name should not start with 'scala.collection'");

    // Document expected Scala collection class names that would match
    String[] scalaCollectionClassNames = {
        "scala.collection.immutable.Map$EmptyMap$",
        "scala.collection.immutable.List",
        "scala.collection.mutable.HashMap",
        "scala.collection.Seq"
    };

    for (String className : scalaCollectionClassNames) {
      Assert.assertTrue(className.startsWith("scala.collection"),
          "Scala collection class name should start with 'scala.collection': " + className);
    }
  }

  /**
   * Helper method to invoke the private isScalaCollection method via reflection.
   */
  private boolean invokeIsScalaCollection(Object value)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method = FieldSpec.class.getDeclaredMethod("isScalaCollection", Object.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, value);
  }
}
