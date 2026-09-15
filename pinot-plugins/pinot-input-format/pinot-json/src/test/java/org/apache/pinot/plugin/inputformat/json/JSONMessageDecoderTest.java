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
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


public class JSONMessageDecoderTest {
  private static final String PRECISE_DECIMAL = "12345678901234567890.12345678901234567890";

  @Test
  public void testDirectDecodePreservesValueConversion()
      throws Exception {
    byte[] payload = ("{\"id\":1,\"huge\":99999999999999999999999999,"
        + "\"nested\":{\"values\":[1,null,3]},\"tags\":[\"a\",\"b\"],"
        + "\"flag\":true,\"off\":false,\"gone\":null}").getBytes(StandardCharsets.UTF_8);
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(), null, "testTopic");
    GenericRow row = new GenericRow();
    // Without a field selection there is no pre-nulling pass, so an explicit JSON null must still overwrite
    // the previous message's value in a reused row.
    row.putValue("gone", "stale");

    decoder.decode(payload, row);

    assertEquals(row.getValue("id"), 1);
    assertEquals(row.getValue("huge"), new BigDecimal("99999999999999999999999999"));
    assertEquals((Object[]) ((Map<?, ?>) row.getValue("nested")).get("values"), new Object[]{1, null, 3});
    assertEquals((Object[]) row.getValue("tags"), new Object[]{"a", "b"});
    assertEquals(row.getValue("flag"), Boolean.TRUE);
    assertEquals(row.getValue("off"), Boolean.FALSE);
    assertEquals(row.getValue("gone"), null);
  }

  @Test
  public void testDirectDecodeSelectedFieldsOverwritesMissingValues()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(), Set.of("id", "missing"), "testTopic");
    GenericRow row = new GenericRow();
    row.putValue("missing", "stale");
    row.putValue("unselected", "preserved");

    decoder.decode("{\"id\":1,\"ignored\":[1,2,3]}".getBytes(StandardCharsets.UTF_8), row);

    assertEquals(row.getValue("id"), 1);
    assertEquals(row.getValue("missing"), null);
    assertEquals(row.getValue("unselected"), "preserved");
    assertEquals(row.getFieldToValueMap().keySet(), Set.of("id", "missing", "unselected"));
  }

  @Test
  public void testDirectDecodeHonorsOffsetAndLength()
      throws Exception {
    String prefix = "invalid-prefix";
    String record = "{\"id\":1,\"name\":\"alice\"}";
    byte[] payload = (prefix + record + "invalid-suffix").getBytes(StandardCharsets.UTF_8);
    int offset = prefix.getBytes(StandardCharsets.UTF_8).length;
    int length = record.getBytes(StandardCharsets.UTF_8).length;

    JSONMessageDecoder allFieldsDecoder = new JSONMessageDecoder();
    allFieldsDecoder.init(Map.of(), null, "testTopic");
    GenericRow allFieldsRow = allFieldsDecoder.decode(payload, offset, length, new GenericRow());
    assertEquals(allFieldsRow.getFieldToValueMap(), Map.of("id", 1, "name", "alice"));

    JSONMessageDecoder selectedFieldsDecoder = new JSONMessageDecoder();
    selectedFieldsDecoder.init(Map.of(), Set.of("id"), "testTopic");
    GenericRow selectedFieldsRow = selectedFieldsDecoder.decode(payload, offset, length, new GenericRow());
    assertEquals(selectedFieldsRow.getFieldToValueMap(), Map.of("id", 1));

    // Exclude the closing brace to prove the parser honors length instead of reading the remaining array.
    assertThrows(RuntimeException.class,
        () -> allFieldsDecoder.decode(payload, offset, length - 1, new GenericRow()));
  }

  @Test
  public void testCustomExtractorUsesMapFallback()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(StreamMessageDecoder.RECORD_EXTRACTOR_CONFIG_KEY, CustomJSONRecordExtractor.class.getName()),
        Set.of("id"), "testTopic");

    GenericRow row = decoder.decode("{\"id\":1}".getBytes(StandardCharsets.UTF_8), new GenericRow());

    assertEquals(row.getValue("id"), 1);
    assertEquals(row.getValue("custom"), true);
  }

  public static class CustomJSONRecordExtractor extends JSONRecordExtractor {
    @Override
    public GenericRow extract(Map<String, Object> from, GenericRow to) {
      to.putValue("custom", true);
      return super.extract(from, to);
    }
  }

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
    assertSchemaConversion(row.getValue("decimalMetric"), PRECISE_DECIMAL);
  }

  @Test
  public void testJsonDecoderPreservesScientificNotation()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(), Set.of("large", "tiny"), "testTopic");

    GenericRow row = decoder.decode("{\"large\":1.23e10,\"tiny\":1.23e-10}".getBytes(StandardCharsets.UTF_8),
        new GenericRow());

    assertEquals(((BigDecimal) row.getValue("large")).toPlainString(), "12300000000");
    assertEquals(((BigDecimal) row.getValue("tiny")).toPlainString(), "0.000000000123");
    assertEquals(PinotDataType.DOUBLE.convert(row.getValue("large"), PinotDataType.BIG_DECIMAL), 1.23e10);
    assertEquals(PinotDataType.STRING.convert(row.getValue("large"), PinotDataType.BIG_DECIMAL), "12300000000");
  }

  @Test
  public void testCustomRecordExtractorAlsoPreservesBigDecimal()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(StreamMessageDecoder.RECORD_EXTRACTOR_CONFIG_KEY, DecimalTypeRecordExtractor.class.getName()),
        Set.of("decimalMetric"), "testTopic");

    GenericRow row = decoder.decode("{\"decimalMetric\":1.25}".getBytes(StandardCharsets.UTF_8), new GenericRow());

    assertEquals(row.getValue("decimalMetricType"), BigDecimal.class.getName());
    assertEquals(row.getValue("decimalMetric"), new BigDecimal("1.25"));
  }

  @Test
  public void testAutoDetectedTextPreservesBigDecimalPrecision()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "AUTO"), Set.of("decimalMetric"), "testTopic");

    byte[] payload = ("{\"decimalMetric\":" + PRECISE_DECIMAL + "}").getBytes(StandardCharsets.UTF_8);
    GenericRow row = decoder.decode(payload, new GenericRow());

    assertEquals(row.getValue("decimalMetric"), new BigDecimal(PRECISE_DECIMAL));
  }

  @Test
  public void testPostgresJsonbPreservesBigDecimalPrecision()
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "POSTGRES_JSONB"), Set.of("decimalMetric"),
        "testTopic");

    byte[] body = ("{\"decimalMetric\":" + PRECISE_DECIMAL + "}").getBytes(StandardCharsets.UTF_8);
    byte[] payload = new byte[body.length + 1];
    payload[0] = 1;
    System.arraycopy(body, 0, payload, 1, body.length);
    GenericRow row = decoder.decode(payload, new GenericRow());

    assertEquals(row.getValue("decimalMetric"), new BigDecimal(PRECISE_DECIMAL));
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

  /// Schema conversion uses the decoded Java type, not the JSON token. Floating literals are BigDecimal
  /// until PinotDataType maps them onto the target column.
  private static void assertSchemaConversion(Object decoded, String expectedPlain) {
    assertEquals(PinotDataType.getSingleValueType(decoded), PinotDataType.BIG_DECIMAL);
    assertEquals(PinotDataType.BIG_DECIMAL.convert(decoded, PinotDataType.BIG_DECIMAL), new BigDecimal(expectedPlain));
    assertEquals(PinotDataType.DOUBLE.convert(decoded, PinotDataType.BIG_DECIMAL),
        new BigDecimal(expectedPlain).doubleValue());
    assertEquals(PinotDataType.FLOAT.convert(decoded, PinotDataType.BIG_DECIMAL),
        new BigDecimal(expectedPlain).floatValue());
    assertEquals(PinotDataType.STRING.convert(decoded, PinotDataType.BIG_DECIMAL), expectedPlain);
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
