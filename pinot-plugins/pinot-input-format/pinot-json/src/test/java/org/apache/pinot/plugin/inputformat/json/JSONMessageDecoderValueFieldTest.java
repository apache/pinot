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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.stream.StreamDataDecoderImpl.VALUE_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/// Tests for reading the whole message value through the reserved `__value` field.
public class JSONMessageDecoderValueFieldTest {
  private static final Set<String> FIELDS_WITH_VALUE = Set.of(VALUE_KEY, "state", "scope");

  @DataProvider
  public static Object[][] jsonValues() {
    return new Object[][]{
        {"{\"scope\":\"internal\",\"state\":\"CABIN_IDLE\",\"n\":{\"a\":[1,2]}}",
            "{\"scope\":\"internal\",\"state\":\"CABIN_IDLE\",\"n\":{\"a\":[1,2]}}"},
        {"{\n  \"state\": \"DOOR_OPEN\",\n  \"temp\": 19.50\n}", "{\"state\":\"DOOR_OPEN\",\"temp\":19.5}"},
        {"{\"big\":123456789012345678901234567890}", "{\"big\":123456789012345678901234567890}"},
        {"\"CABIN_IDLE\"", "\"CABIN_IDLE\""},
        {"42", "42"},
        {"true", "true"},
        {"[\"A\",\"B\"]", "[\"A\",\"B\"]"},
        {"null", "null"}
    };
  }

  @Test(dataProvider = "jsonValues")
  public void testWholeValueIsStoredAsOneField(String payload, String expected)
      throws Exception {
    GenericRow row = decode(Map.of(), FIELDS_WITH_VALUE, payload.getBytes(StandardCharsets.UTF_8));
    // Only the value field is emitted: top-level keys such as "scope" and "state" never become fields
    assertEquals(row.getFieldToValueMap().keySet(), Set.of(VALUE_KEY));
    assertEquals(row.getValue(VALUE_KEY), expected);
  }

  @Test
  public void testValueIsEmittedOnlyWhenRead()
      throws Exception {
    byte[] payload = "{\"scope\":\"internal\",\"state\":\"CABIN_IDLE\"}".getBytes(StandardCharsets.UTF_8);
    GenericRow row = decode(Map.of(), Set.of("state", "scope"), payload);
    assertNull(row.getValue(VALUE_KEY));
    assertEquals(row.getValue("scope"), "internal");
    assertEquals(row.getValue("state"), "CABIN_IDLE");
    assertThrows(RuntimeException.class, () -> decode(Map.of(), Set.of("state"), "\"CABIN_IDLE\"".getBytes()));
  }

  @DataProvider
  public static Object[][] invalidPayloads() {
    return new Object[][]{
        {"beacon-cron TEST E2E 1794318612"}, {"{\"a\":1} junk"}, {"42 abc"}, {""}, {"   "}, {"{\"a\":"}
    };
  }

  @Test(dataProvider = "invalidPayloads")
  public void testPayloadThatIsNotExactlyOneJsonValueIsRejected(String payload) {
    assertThrows(RuntimeException.class,
        () -> decode(Map.of(), FIELDS_WITH_VALUE, payload.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testBinaryAndAutoDetectedFormats()
      throws Exception {
    String json = "{\"state\":\"CABIN_IDLE\",\"temp\":21.5}";
    ObjectMapper text = new ObjectMapper();
    byte[] smile = new ObjectMapper(new SmileFactory()).writeValueAsBytes(text.readTree(json));
    byte[] cbor = new ObjectMapper(new CBORFactory()).writeValueAsBytes(text.readTree(json));
    byte[] scalarCbor = new ObjectMapper(new CBORFactory()).writeValueAsBytes("CABIN_IDLE");
    byte[] jsonb = ("\u0001" + json).getBytes(StandardCharsets.UTF_8);
    assertEquals(decode(Map.of("jsonFormat", "SMILE"), FIELDS_WITH_VALUE, smile).getValue(VALUE_KEY), json);
    assertEquals(decode(Map.of("jsonFormat", "CBOR"), FIELDS_WITH_VALUE, cbor).getValue(VALUE_KEY), json);
    assertEquals(decode(Map.of("jsonFormat", "CBOR"), FIELDS_WITH_VALUE, scalarCbor).getValue(VALUE_KEY),
        "\"CABIN_IDLE\"");
    assertEquals(decode(Map.of("jsonFormat", "POSTGRES_JSONB"), FIELDS_WITH_VALUE, jsonb).getValue(VALUE_KEY), json);
    assertEquals(decode(Map.of("jsonFormat", "AUTO"), FIELDS_WITH_VALUE, smile).getValue(VALUE_KEY), json);
    assertEquals(decode(Map.of("jsonFormat", "AUTO"), FIELDS_WITH_VALUE, json.getBytes(StandardCharsets.UTF_8))
        .getValue(VALUE_KEY), json);
  }

  @Test
  public void testUnsupportedConfigurationsAreRejectedAtInit() {
    assertThrows(IllegalArgumentException.class,
        () -> new JSONMessageDecoder().init(Map.of("jsonFormat", "SQLITE_JSONB"), FIELDS_WITH_VALUE, "topic"));
    assertThrows(IllegalArgumentException.class, () -> new JSONMessageDecoder().init(
        Map.of("recordExtractorClass", CustomRecordExtractor.class.getName()), FIELDS_WITH_VALUE, "topic"));
  }

  @Test
  public void testDecodeWithOffset()
      throws Exception {
    byte[] framed = "xx\"CABIN_IDLE\"yy".getBytes(StandardCharsets.UTF_8);
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(Map.of(), FIELDS_WITH_VALUE, "topic");
    assertEquals(decoder.decode(framed, 2, framed.length - 4, new GenericRow()).getValue(VALUE_KEY),
        JsonUtils.objectToString("CABIN_IDLE"));
  }

  private static GenericRow decode(Map<String, String> props, Set<String> fieldsToRead, byte[] payload)
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(props, fieldsToRead, "topic");
    return decoder.decode(payload, new GenericRow());
  }

  public static class CustomRecordExtractor extends JSONRecordExtractor {
  }
}
