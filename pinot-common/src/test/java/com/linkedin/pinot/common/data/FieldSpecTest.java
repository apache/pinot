/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link FieldSpec}.
 */
public class FieldSpecTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed is: " + RANDOM_SEED;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Test all {@link FieldSpec.FieldType} with different {@link DataType}.
   */
  @Test
  public void testFieldSpec() {
    // Single-value boolean type dimension field with default null value.
    FieldSpec fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("svDimension");
    fieldSpec1.setDataType(DataType.BOOLEAN);
    fieldSpec1.setDefaultNullValue(false);
    FieldSpec fieldSpec2 = new DimensionFieldSpec("svDimension", DataType.BOOLEAN, true);
    fieldSpec2.setDefaultNullValue(false);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertTrue(fieldSpec1.getDefaultNullValue() instanceof String);
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), "false");

    // Multi-value dimension field.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("mvDimension");
    fieldSpec1.setDataType(DataType.INT);
    fieldSpec1.setSingleValueField(false);
    fieldSpec2 = new DimensionFieldSpec("mvDimension", DataType.INT, false);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertTrue(fieldSpec1.getDefaultNullValue() instanceof Integer);
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), Integer.MIN_VALUE);

    // Multi-value dimension field with default null value.
    fieldSpec1 = new DimensionFieldSpec();
    fieldSpec1.setName("mvDimension");
    fieldSpec1.setDataType(DataType.FLOAT);
    fieldSpec1.setSingleValueField(false);
    fieldSpec1.setDefaultNullValue(-0.1);
    fieldSpec2 = new DimensionFieldSpec("mvDimension", DataType.FLOAT, false);
    fieldSpec2.setDefaultNullValue(-0.1);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertTrue(fieldSpec1.getDefaultNullValue() instanceof Float);
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), -0.1F);

    // Short type metric field.
    fieldSpec1 = new MetricFieldSpec();
    fieldSpec1.setName("metric");
    fieldSpec1.setDataType(DataType.SHORT);
    fieldSpec2 = new MetricFieldSpec("metric", DataType.SHORT);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertTrue(fieldSpec1.getDefaultNullValue() instanceof Integer);
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), 0);

    // Metric field with default null value.
    fieldSpec1 = new MetricFieldSpec();
    fieldSpec1.setName("metric");
    fieldSpec1.setDataType(DataType.LONG);
    fieldSpec1.setDefaultNullValue(1);
    fieldSpec2 = new MetricFieldSpec("metric", DataType.LONG);
    fieldSpec2.setDefaultNullValue(1);
    Assert.assertEquals(fieldSpec1, fieldSpec2);
    Assert.assertEquals(fieldSpec1.toString(), fieldSpec2.toString());
    Assert.assertEquals(fieldSpec1.hashCode(), fieldSpec2.hashCode());
    Assert.assertTrue(fieldSpec1.getDefaultNullValue() instanceof Long);
    Assert.assertEquals(fieldSpec1.getDefaultNullValue(), 1L);
  }

  /**
   * Test derived {@link MetricFieldSpec}.
   */
  @Test
  public void testDerivedMetricFieldSpec()
      throws IOException {
    MetricFieldSpec derivedMetricField =
        new MetricFieldSpec("derivedMetric", DataType.STRING, 10, MetricFieldSpec.DerivedMetricType.HLL);
    Assert.assertEquals(derivedMetricField.getFieldSize(), 10);
    Assert.assertTrue(derivedMetricField.isDerivedMetric());
    Assert.assertEquals(derivedMetricField.getDerivedMetricType(), MetricFieldSpec.DerivedMetricType.HLL);
    Assert.assertEquals(derivedMetricField.getDefaultNullValue(), "null");

    // Test serialize deserialize.
    MetricFieldSpec derivedMetricField2 =
        MAPPER.readValue(MAPPER.writeValueAsString(derivedMetricField), MetricFieldSpec.class);
    Assert.assertEquals(derivedMetricField2, derivedMetricField);
  }

  /**
   * Test different order of fields in serialized JSON string to deserialize {@link FieldSpec}.
   */
  @Test
  public void testOrderOfFields() throws Exception {
    // Metric field with default null value.
    String[] metricFields = {"\"name\":\"metric\"", "\"dataType\":\"INT\"", "\"defaultNullValue\":-1"};
    MetricFieldSpec metricFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    MetricFieldSpec metricFieldSpec2 = new MetricFieldSpec("metric", DataType.INT);
    metricFieldSpec2.setDefaultNullValue(-1);
    Assert.assertEquals(metricFieldSpec1, metricFieldSpec2, ERROR_MESSAGE);
    Assert.assertTrue(metricFieldSpec1.getDefaultNullValue() instanceof Integer, ERROR_MESSAGE);
    Assert.assertEquals(metricFieldSpec1.getDefaultNullValue(), -1, ERROR_MESSAGE);

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {"\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false"};
    DimensionFieldSpec dimensionFieldSpec1 =
        MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    DimensionFieldSpec dimensionFieldSpec2 = new DimensionFieldSpec("dimension", DataType.BOOLEAN, true);
    dimensionFieldSpec2.setDefaultNullValue(false);
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertTrue(dimensionFieldSpec1.getDefaultNullValue() instanceof String, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), "false", ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields = new String[]{
        "\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false",
        "\"defaultNullValue\":\"default\""};
    dimensionFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    dimensionFieldSpec2 = new DimensionFieldSpec("dimension", DataType.STRING, false);
    dimensionFieldSpec2.setDefaultNullValue("default");
    Assert.assertEquals(dimensionFieldSpec1, dimensionFieldSpec2, ERROR_MESSAGE);
    Assert.assertTrue(dimensionFieldSpec1.getDefaultNullValue() instanceof String, ERROR_MESSAGE);
    Assert.assertEquals(dimensionFieldSpec1.getDefaultNullValue(), "default", ERROR_MESSAGE);

    // Time field with default null value.
    String[] timeFields = {
        "\"incomingGranularitySpec\":{\"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"incomingTime\"}",
        "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"outgoingTime\"}",
        "\"defaultNullValue\":-1"};
    TimeFieldSpec timeFieldSpec1 = MAPPER.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    TimeFieldSpec timeFieldSpec2 =
        new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incomingTime"),
            new TimeGranularitySpec(DataType.INT, TimeUnit.SECONDS, "outgoingTime"));
    timeFieldSpec2.setDefaultNullValue(-1);
    Assert.assertEquals(timeFieldSpec1, timeFieldSpec2, ERROR_MESSAGE);
    Assert.assertTrue(timeFieldSpec1.getDefaultNullValue() instanceof Integer, ERROR_MESSAGE);
    Assert.assertEquals(timeFieldSpec1.getDefaultNullValue(), -1, ERROR_MESSAGE);
  }

  /**
   * Test {@link FieldSpec} serialize deserialize.
   */
  @Test
  public void testSerializeDeserialize() throws Exception {
    FieldSpec first;
    FieldSpec second;

    // Short type Metric field.
    String[] metricFields = {"\"name\":\"metric\"", "\"dataType\":\"SHORT\""};
    first = MAPPER.readValue(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    second = MAPPER.readValue(MAPPER.writeValueAsString(first), MetricFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Single-value boolean type dimension field with default null value.
    String[] dimensionFields = {"\"name\":\"dimension\"", "\"dataType\":\"BOOLEAN\"", "\"defaultNullValue\":false"};
    first = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = MAPPER.readValue(MAPPER.writeValueAsString(first), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Multi-value dimension field with default null value.
    dimensionFields = new String[]{
        "\"name\":\"dimension\"", "\"dataType\":\"STRING\"", "\"singleValueField\":false",
        "\"defaultNullValue\":\"default\""};
    first = MAPPER.readValue(getRandomOrderJsonString(dimensionFields), DimensionFieldSpec.class);
    second = MAPPER.readValue(MAPPER.writeValueAsString(first), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, ERROR_MESSAGE);

    // Time field with default value.
    String[] timeFields = {
        "\"incomingGranularitySpec\":{\"timeUnitSize\":1, \"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"incomingTime\"}",
        "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"outgoingTime\"}",
        "\"defaultNullValue\":-1"};
    first = MAPPER.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    second = MAPPER.readValue(MAPPER.writeValueAsString(first), TimeFieldSpec.class);
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
