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
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FieldSpecTest {
  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);
  private final String _errorMessage = "Random seed is: " + _randomSeed;
  private final ObjectMapper _mapper = new ObjectMapper();

  @Test
  public void testFieldSpec() {
    // Single value dimension field.
    FieldSpec fieldSpec = new DimensionFieldSpec();
    fieldSpec.setDataType(DataType.STRING);
    Assert.assertEquals(fieldSpec.toString(),
        "< data type: STRING , field type: DIMENSION, single value column, default null value: null >");
    Assert.assertTrue(fieldSpec.getDefaultNullValue() instanceof String);

    // Multi value dimension field.
    fieldSpec = new DimensionFieldSpec();
    fieldSpec.setDataType(DataType.INT);
    fieldSpec.setSingleValueField(false);
    fieldSpec.setDelimiter(";");
    Assert.assertEquals(fieldSpec.toString(),
        "< data type: INT , field type: DIMENSION, multi value column, delimiter: ';', default null value: -2147483648 >");
    Assert.assertTrue(fieldSpec.getDefaultNullValue() instanceof Integer);

    // Multi value dimension field with default null value.
    fieldSpec = new DimensionFieldSpec();
    fieldSpec.setDataType(DataType.FLOAT);
    fieldSpec.setSingleValueField(false);
    fieldSpec.setDefaultNullValue(-0.1);
    Assert.assertEquals(fieldSpec.toString(),
        "< data type: FLOAT , field type: DIMENSION, multi value column, delimiter: ',', default null value: -0.1 >");
    Assert.assertTrue(fieldSpec.getDefaultNullValue() instanceof Float);

    // Metric field.
    fieldSpec = new MetricFieldSpec();
    fieldSpec.setDataType(DataType.DOUBLE);
    Assert.assertEquals(fieldSpec.toString(),
        "< data type: DOUBLE , field type: METRIC, single value column, default null value: 0.0 >");
    Assert.assertTrue(fieldSpec.getDefaultNullValue() instanceof Double);

    // Metric field with default null value.
    fieldSpec = new MetricFieldSpec();
    fieldSpec.setDataType(DataType.LONG);
    fieldSpec.setDefaultNullValue(0L);
    Assert.assertEquals(fieldSpec.toString(),
        "< data type: LONG , field type: METRIC, single value column, default null value: 0 >");
    Assert.assertTrue(fieldSpec.getDefaultNullValue() instanceof Long);
  }

  @Test
  public void testSchemaBuilder() {
    Schema schema =
        new SchemaBuilder().addSingleValueDimension("svDimension", DataType.INT)
            .addMultiValueDimension("mvDimension", DataType.STRING, ",").addMetric("metric", DataType.INT)
            .addTime("incomingTime", TimeUnit.DAYS, DataType.LONG).build();

    Assert.assertEquals(schema.getFieldSpecFor("svDimension").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("svDimension").getDataType(), DataType.INT);

    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").isSingleValueField(), false);
    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").getDataType(), DataType.STRING);
    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").getDelimiter(), ",");

    Assert.assertEquals(schema.getFieldSpecFor("metric").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("metric").getDataType(), DataType.INT);

    Assert.assertEquals(schema.getFieldSpecFor("incomingTime").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("incomingTime").getDataType(), DataType.LONG);
  }

  @Test
  public void testOrderOfFields() throws Exception {
    // Metric field with default value.
    String[] metricFields = {"\"dataType\":\"INT\"", "\"name\":\"metric\"", "\"defaultNullValue\":-1"};
    MetricFieldSpec metricFieldSpec = _mapper.readValue(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    Assert.assertEquals(metricFieldSpec.toString(),
        "< data type: INT , field type: METRIC, single value column, default null value: -1 >", _errorMessage);
    Assert.assertTrue(metricFieldSpec.getDefaultNullValue() instanceof Integer, _errorMessage);

    // Single value dimension field.
    String[] dimensionFields1 = {"\"dataType\":\"DOUBLE\"","\"name\":\"dimension\""};
    DimensionFieldSpec dimensionFieldSpec1 =
        _mapper.readValue(getRandomOrderJsonString(dimensionFields1), DimensionFieldSpec.class);
    Assert.assertEquals(dimensionFieldSpec1.toString(),
        "< data type: DOUBLE , field type: DIMENSION, single value column, default null value: -Infinity >", _errorMessage);
    Assert.assertTrue(dimensionFieldSpec1.getDefaultNullValue() instanceof Double, _errorMessage);

    // Multi value dimension field with default value.
    String[] dimensionFields2 =
        {"\"dataType\":\"STRING\"", "\"name\":\"dimension\"", "\"singleValueField\":false",
            "\"defaultNullValue\":\"default\""};
    DimensionFieldSpec dimensionFieldSpec2 =
        _mapper.readValue(getRandomOrderJsonString(dimensionFields2), DimensionFieldSpec.class);
    Assert.assertEquals(dimensionFieldSpec2.toString(),
        "< data type: STRING , field type: DIMENSION, multi value column, delimiter: ',', default null value: default >");
    Assert.assertTrue(dimensionFieldSpec2.getDefaultNullValue() instanceof String, _errorMessage);

    // Time field with default value.
    String[] timeFields =
        {"\"incomingGranularitySpec\":{\"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"time\"}",
            "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"time\"}",
            "\"defaultNullValue\":-1"};
    TimeFieldSpec timeFieldSpec = _mapper.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    Assert.assertEquals(timeFieldSpec.toString(), "< data type: INT, field type : TIME, "
        + "incoming granularity spec: < data type: LONG, time type: MILLISECONDS, time unit size: 1, name: time >, "
        + "outgoing granularity spec: < data type: INT, time type: SECONDS, time unit size: 1, name: time >, "
        + "default null value: -1 >", _errorMessage);
    Assert.assertTrue(timeFieldSpec.getDefaultNullValue() instanceof Integer, _errorMessage);
  }

  @Test
  public void testSerializeDeserialize() throws Exception {
    FieldSpec first;
    FieldSpec second;

    // Metric field with default value.
    String[] metricFields = {"\"dataType\":\"INT\"", "\"name\":\"metric\"", "\"defaultNullValue\":-1"};
    first = _mapper.readValue(getRandomOrderJsonString(metricFields), MetricFieldSpec.class);
    second = _mapper.readValue(_mapper.writeValueAsString(first), MetricFieldSpec.class);
    Assert.assertEquals(first, second, _errorMessage);

    // Single value dimension field.
    String[] dimensionFields1 = {"\"dataType\":\"DOUBLE\"","\"name\":\"dimension\""};
    first = _mapper.readValue(getRandomOrderJsonString(dimensionFields1), DimensionFieldSpec.class);
    second = _mapper.readValue(_mapper.writeValueAsString(first), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, _errorMessage);

    // Multi value dimension field with default value.
    String[] dimensionFields2 =
        {"\"dataType\":\"STRING\"", "\"name\":\"dimension\"", "\"singleValueField\":false",
            "\"defaultNullValue\":\"default\""};
    first = _mapper.readValue(getRandomOrderJsonString(dimensionFields2), DimensionFieldSpec.class);
    second = _mapper.readValue(_mapper.writeValueAsString(first), DimensionFieldSpec.class);
    Assert.assertEquals(first, second, _errorMessage);

    // Time field with default value.
    String[] timeFields =
        {"\"incomingGranularitySpec\":{\"timeType\":\"MILLISECONDS\",\"dataType\":\"LONG\",\"name\":\"time\"}",
            "\"outgoingGranularitySpec\":{\"timeType\":\"SECONDS\",\"dataType\":\"INT\",\"name\":\"time\"}",
            "\"defaultNullValue\":-1"};
    first = _mapper.readValue(getRandomOrderJsonString(timeFields), TimeFieldSpec.class);
    second = _mapper.readValue(_mapper.writeValueAsString(first), TimeFieldSpec.class);
    Assert.assertEquals(first, second, _errorMessage);
  }

  /**
   * Helper function to generate json string with random order of fields passed in.
   *
   * @param fields string array of fields.
   * @return generated json string.
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
      jsonString.append(fields[indices.remove(_random.nextInt(i))]);
      if (i != 1) {
        jsonString.append(',');
      }
    }
    jsonString.append('}');
    return jsonString.toString();
  }
}
