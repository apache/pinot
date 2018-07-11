/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.pinot.common.data.FieldSpec.DataType.*;


/**
 * Tests for {@link FieldSpec}.
 */
public class FieldSpecTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed is: " + RANDOM_SEED;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Test all {@link FieldSpec.DataType}.
   */
  @Test
  public void testDataType() {
    Assert.assertEquals(INT.getStoredType(), INT);
    Assert.assertEquals(LONG.getStoredType(), LONG);
    Assert.assertEquals(FLOAT.getStoredType(), FLOAT);
    Assert.assertEquals(DOUBLE.getStoredType(), DOUBLE);
    Assert.assertEquals(BOOLEAN.getStoredType(), STRING);
    Assert.assertEquals(STRING.getStoredType(), STRING);
    Assert.assertEquals(BYTES.getStoredType(), BYTES);

    Assert.assertEquals(INT.size(), Integer.BYTES);
    Assert.assertEquals(LONG.size(), Long.BYTES);
    Assert.assertEquals(FLOAT.size(), Float.BYTES);
    Assert.assertEquals(DOUBLE.size(), Double.BYTES);

    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.INT), INT);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.LONG), LONG);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.FLOAT), FLOAT);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.DOUBLE), DOUBLE);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.BOOLEAN), STRING);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.STRING), STRING);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.ENUM), STRING);
    Assert.assertEquals(FieldSpec.DataType.valueOf(Schema.Type.BYTES), BYTES);
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
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), "false");

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
  }

  /**
   * Test derived {@link MetricFieldSpec}.
   */
  @Test
  public void testDerivedMetricFieldSpec() throws Exception {
    MetricFieldSpec derivedMetricField =
        new MetricFieldSpec("derivedMetric", STRING, 10, MetricFieldSpec.DerivedMetricType.HLL);
    Assert.assertEquals(derivedMetricField.getFieldSize(), 10);
    Assert.assertTrue(derivedMetricField.isDerivedMetric());
    Assert.assertEquals(derivedMetricField.getDerivedMetricType(), MetricFieldSpec.DerivedMetricType.HLL);
    Assert.assertEquals(derivedMetricField.getDefaultNullValue(), "null");

    // Test serialize deserialize.
    MetricFieldSpec derivedMetricField2 =
        MAPPER.readValue(derivedMetricField.toJsonObject().toString(), MetricFieldSpec.class);
    Assert.assertEquals(derivedMetricField2, derivedMetricField);
  }

  /**
   * Test {@link TimeFieldSpec} constructors.
   */
  @Test
  public void testTimeFieldSpecConstructor() {
    String incomingName = "incoming";
    FieldSpec.DataType incomingDataType = LONG;
    TimeUnit incomingTimeUnit = TimeUnit.HOURS;
    int incomingTimeUnitSize = 1;
    TimeGranularitySpec incomingTimeGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    String outgoingName = "outgoing";
    FieldSpec.DataType outgoingDataType = INT;
    TimeUnit outgoingTimeUnit = TimeUnit.DAYS;
    int outgoingTimeUnitSize = 1;
    TimeGranularitySpec outgoingTimeGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
    int defaultNullValue = 17050;

    TimeFieldSpec timeFieldSpec1 = new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit);
    TimeFieldSpec timeFieldSpec2 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, defaultNullValue);
    TimeFieldSpec timeFieldSpec3 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
            outgoingTimeUnit);
    TimeFieldSpec timeFieldSpec4 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
            outgoingTimeUnit, defaultNullValue);
    TimeFieldSpec timeFieldSpec5 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit);
    TimeFieldSpec timeFieldSpec6 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, defaultNullValue);
    TimeFieldSpec timeFieldSpec7 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
            outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit);
    TimeFieldSpec timeFieldSpec8 =
        new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
            outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, defaultNullValue);
    TimeFieldSpec timeFieldSpec9 = new TimeFieldSpec(incomingTimeGranularitySpec);
    TimeFieldSpec timeFieldSpec10 = new TimeFieldSpec(incomingTimeGranularitySpec, defaultNullValue);
    TimeFieldSpec timeFieldSpec11 = new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec);
    TimeFieldSpec timeFieldSpec12 =
        new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec, defaultNullValue);

    Assert.assertEquals(timeFieldSpec1, timeFieldSpec5);
    Assert.assertEquals(timeFieldSpec1, timeFieldSpec9);
    Assert.assertEquals(timeFieldSpec2, timeFieldSpec6);
    Assert.assertEquals(timeFieldSpec2, timeFieldSpec10);
    Assert.assertEquals(timeFieldSpec3, timeFieldSpec7);
    Assert.assertEquals(timeFieldSpec3, timeFieldSpec11);
    Assert.assertEquals(timeFieldSpec4, timeFieldSpec8);
    Assert.assertEquals(timeFieldSpec4, timeFieldSpec12);

    // Before adding default null value.
    Assert.assertFalse(timeFieldSpec1.equals(timeFieldSpec2));
    Assert.assertFalse(timeFieldSpec3.equals(timeFieldSpec4));
    Assert.assertFalse(timeFieldSpec5.equals(timeFieldSpec6));
    Assert.assertFalse(timeFieldSpec7.equals(timeFieldSpec8));
    Assert.assertFalse(timeFieldSpec9.equals(timeFieldSpec10));
    Assert.assertFalse(timeFieldSpec11.equals(timeFieldSpec12));

    // After adding default null value.
    timeFieldSpec1.setDefaultNullValue(defaultNullValue);
    timeFieldSpec3.setDefaultNullValue(defaultNullValue);
    timeFieldSpec5.setDefaultNullValue(defaultNullValue);
    timeFieldSpec7.setDefaultNullValue(defaultNullValue);
    timeFieldSpec9.setDefaultNullValue(defaultNullValue);
    timeFieldSpec11.setDefaultNullValue(defaultNullValue);
    Assert.assertEquals(timeFieldSpec1, timeFieldSpec2);
    Assert.assertEquals(timeFieldSpec3, timeFieldSpec4);
    Assert.assertEquals(timeFieldSpec5, timeFieldSpec6);
    Assert.assertEquals(timeFieldSpec7, timeFieldSpec8);
    Assert.assertEquals(timeFieldSpec9, timeFieldSpec10);
    Assert.assertEquals(timeFieldSpec11, timeFieldSpec12);
  }

  /**
   * Test {@link TimeFieldSpec} constructors.
   */
  @Test
  public void testDateTimeFieldSpecConstructor() {
    String name = "Date";
    String format = "1:HOURS:EPOCH";
    String granularity = "1:HOURS";

    DateTimeFieldSpec dateTimeFieldSpec1 = new DateTimeFieldSpec(name, LONG, format, granularity);
    DateTimeFieldSpec dateTimeFieldSpec2 = new DateTimeFieldSpec(name, INT, format, granularity);
    Assert.assertFalse(dateTimeFieldSpec1.equals(dateTimeFieldSpec2));

    DateTimeFieldSpec dateTimeFieldSpec3 = new DateTimeFieldSpec(name, LONG, format, granularity);
    Assert.assertEquals(dateTimeFieldSpec1, dateTimeFieldSpec3);
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
    entries.add(new Object[]{name, dataType, "1:HOURS:SIMPLE_DATE_FORMAT", granularity, true, null});
    entries.add(new Object[]{name, dataType, "1:hour:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "1:HOUR:EPOCH:yyyyMMdd", granularity, true, null});
    entries.add(new Object[]{name, dataType, "0:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "-1:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "0.1:HOURS:EPOCH", granularity, true, null});
    entries.add(new Object[]{name, dataType, "1:HOURS:EPOCH", granularity, false, new DateTimeFieldSpec(name, dataType,
        "1:HOURS:EPOCH", granularity)});
    entries.add(
        new Object[]{name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", granularity, false, new DateTimeFieldSpec(
            name, dataType, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", granularity)});

    return entries.toArray(new Object[entries.size()][]);
  }

  /**
   * Test different order of fields in serialized JSON string to deserialize {@link FieldSpec}.
   */
  @Test
  public void testOrderOfFields() throws Exception {
    // Metric field with default null value.
    String[] metricFields = {"\"name\":\"metric\"", "\"dataType\":\"INT\"", "\"defaultNullValue\":-1"};
    MetricFieldSpec metricFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    MetricFieldSpec metricFieldSpec2 = new MetricFieldSpec("metric", INT, -1);
    Assert.assertEquals(metricFieldSpec1, metricFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(metricFieldSpec1.getDefaultNullValue(), -1, ERROR_MESSAGE);

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {"\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false"};
    DimensionFieldSpec dimensionFieldSpec1 =
        MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    DimensionFieldSpec dimensionFieldSpec2 = new DimensionFieldSpec("dimension", BOOLEAN, true, false);
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), "false", ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields =
        new String[]{"\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false", "\"defaultNullValue\":\"default\""};
    dimensionFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    dimensionFieldSpec2 = new DimensionFieldSpec("dimension", STRING, false, "default");
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), "default", ERROR_MESSAGE);

    // Time field with default null value.
    String[] timeFields =
        {"\"incomingGranularitySpec\":{\"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"incomingTime\"}", "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"outgoingTime\"}", "\"defaultNullValue\":-1"};
    TimeFieldSpec timeFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    TimeFieldSpec timeFieldSpec2 =
        new TimeFieldSpec("incomingTime", LONG, TimeUnit.MILLISECONDS, "outgoingTime", INT, TimeUnit.SECONDS, -1);
    Assert.assertEquals(timeFieldSpec1, timeFieldSpec2, ERROR_MESSAGE);
    Assert.assertEquals(timeFieldSpec1.getDefaultNullValue(), -1, ERROR_MESSAGE);

    // Date time field with default null value.
    String[] dateTimeFields =
        {"\"name\":\"Date\"", "\"dataType\":\"LONG\"", "\"format\":\"1:MILLISECONDS:EPOCH\"", "\"granularity\":\"5:MINUTES\"", "\"dateTimeType\":\"PRIMARY\""};
    DateTimeFieldSpec dateTimeFieldSpec1 =
        MAPPER.readValue(getRandomOrderJsonString(dateTimeFields), DateTimeFieldSpec.class);
    DateTimeFieldSpec dateTimeFieldSpec2 = new DateTimeFieldSpec("Date", LONG, "1:MILLISECONDS:EPOCH", "5:MINUTES");
    Assert.assertEquals(dateTimeFieldSpec1, dateTimeFieldSpec2, ERROR_MESSAGE);
  }

  /**
   * Test {@link FieldSpec} serialize deserialize.
   */
  @Test
  public void testSerializeDeserialize() throws Exception {
    FieldSpec first;
    FieldSpec second;

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {"\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false"};
    first = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = MAPPER.readValue(first.toJsonObject().toString(), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields =
        new String[]{"\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false", "\"defaultNullValue\":\"default\""};
    first = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = MAPPER.readValue(first.toJsonObject().toString(), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Time field with default value.
    String[] timeFields =
        {"\"incomingGranularitySpec\":{\"timeUnitSize\":1, \"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"incomingTime\"}", "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"outgoingTime\"}", "\"defaultNullValue\":-1"};
    first = MAPPER.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    second = MAPPER.readValue(first.toJsonObject().toString(), TimeFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // DateTime field
    String[] dateTimeFields =
        {"\"name\":\"Date\"", "\"dataType\":\"LONG\"", "\"format\":\"1:MILLISECONDS:EPOCH\"", "\"granularity\":\"5:MINUTES\"", "\"dateTimeType\":\"PRIMARY\""};
    first = MAPPER.readValue(getRandomOrderJsonString(dateTimeFields), DateTimeFieldSpec.class);
    second = MAPPER.readValue(first.toJsonObject().toString(), DateTimeFieldSpec.class);
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
}
