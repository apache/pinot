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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class JSONMessageDecoderTest {
  private static final String PRECISE_DECIMAL = "12345678901234567890.12345678901234567890";

  @Test
  public void testJsonDecoderWithoutOutgoingTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_without_outgoing_time_spec.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderWithOutgoingTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_with_outgoing_time_spec.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    sourceFields.remove("secondsSinceEpoch");
    sourceFields.put("time_day", DataType.INT);
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderNoTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_no_time_field.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderPreservesBigDecimalPrecision()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(), Set.of("decimalMetric", "doubleMetric"), "testTopic");

    byte[] payload = ("{\"decimalMetric\":" + PRECISE_DECIMAL + ",\"doubleMetric\":1.25}").getBytes(
        StandardCharsets.UTF_8);
    GenericRow row = decoder.decode(payload, new GenericRow());

    assertEquals(row.getValue("decimalMetric"), new BigDecimal(PRECISE_DECIMAL));
    assertEquals(((BigDecimal) row.getValue("doubleMetric")).doubleValue(), 1.25d);
  }

  @Test
  public void testCustomRecordExtractorKeepsLegacyDoubleParsingByDefault()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(StreamMessageDecoder.RECORD_EXTRACTOR_CONFIG_KEY, DecimalTypeRecordExtractor.class.getName()),
        Set.of("decimalMetric"), "testTopic");

    GenericRow row = decoder.decode("{\"decimalMetric\":1.25}".getBytes(StandardCharsets.UTF_8), new GenericRow());

    assertEquals(row.getValue("decimalMetricType"), Double.class.getName());
    assertEquals(row.getValue("decimalMetric"), 1.25d);
  }

  @Test
  public void testCustomRecordExtractorCanOptIntoBigDecimalParsing()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(
            StreamMessageDecoder.RECORD_EXTRACTOR_CONFIG_KEY, DecimalTypeRecordExtractor.class.getName(),
            "preserveDecimalPrecision", "true"),
        Set.of("decimalMetric"), "testTopic");

    GenericRow row = decoder.decode("{\"decimalMetric\":1.25}".getBytes(StandardCharsets.UTF_8), new GenericRow());

    assertEquals(row.getValue("decimalMetricType"), BigDecimal.class.getName());
    assertEquals(row.getValue("decimalMetric"), new BigDecimal("1.25"));
  }

  public static class DecimalTypeRecordExtractor implements RecordExtractor<Map<String, Object>> {
    @Override
    public void init(Set<String> fields, RecordExtractorConfig config) {
    }

    @Override
    public GenericRow extract(Map<String, Object> from, GenericRow to) {
      Object value = from.get("decimalMetric");
      to.putValue("decimalMetric", value);
      to.putValue("decimalMetricType", value.getClass().getName());
      return to;
    }
  }

  private Schema loadSchema(String resourcePath)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(resourcePath);
    assertNotNull(resource);
    return Schema.fromFile(new File(resource.getFile()));
  }

  private void testJsonDecoder(Map<String, DataType> sourceFields)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource("data/test_sample_data.json");
    assertNotNull(resource);
    try (BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()))) {
      JSONMessageDecoder decoder = new JSONMessageDecoder();
      decoder.init(Map.of(), sourceFields.keySet(), "testTopic");
      GenericRow row = new GenericRow();
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode jsonNode = JsonUtils.DEFAULT_READER.readTree(line);
        decoder.decode(line.getBytes(), row);
        for (String field : sourceFields.keySet()) {
          Object actualValue = row.getValue(field);
          JsonNode expectedValue = jsonNode.get(field);
          switch (sourceFields.get(field)) {
            case STRING:
              assertEquals(actualValue, expectedValue.asText());
              break;
            case INT:
              assertEquals(actualValue, expectedValue.asInt());
              break;
            case LONG:
              assertEquals(actualValue, expectedValue.asLong());
              break;
            case FLOAT:
              assertEquals(((Number) actualValue).floatValue(), (float) expectedValue.asDouble());
              break;
            case DOUBLE:
              assertEquals(((Number) actualValue).doubleValue(), expectedValue.asDouble());
              break;
            default:
              fail("Shouldn't arrive here.");
              break;
          }
        }
      }
    }
  }
}
