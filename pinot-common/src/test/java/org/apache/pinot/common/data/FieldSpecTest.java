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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
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
        "\"defaultNullValue\":\"default\""
    };
    dimensionFieldSpec1 = JsonUtils.stringToObject(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    dimensionFieldSpec2 = new DimensionFieldSpec("dimension", STRING, false, "default");
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
}
