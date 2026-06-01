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
package org.apache.pinot.spi.config.table;

import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class OpenStructIndexConfigTest {

  @Test
  public void testDefaultConfig() {
    OpenStructIndexConfig config = OpenStructIndexConfig.DEFAULT;
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxDenseKeys(), -1);
    assertEquals(config.getDenseKeyMinFillRate(), 0.5);
    assertTrue(config.getDenseKeys().isEmpty());
    assertNull(config.getValueFieldConfigs());
    assertNull(config.getDefaultValueFieldConfig());
    // Built-in defaults when neither per-key nor defaultValueFieldConfig is set.
    assertTrue(config.shouldEnableInvertedIndexForKey("any"));
    assertTrue(config.shouldUseDictionaryForKey("any"));
  }

  @Test
  public void testDisabledConfig() {
    OpenStructIndexConfig config = OpenStructIndexConfig.DISABLED;
    assertFalse(config.isEnabled());
  }

  @Test
  public void testNoDictionaryKeys() {
    FieldConfig rawKey =
        new FieldConfig("raw_payload", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, 1000, null, 0.5, List.of(rawKey));
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    // Unconfigured key falls back to built-in default (DICTIONARY).
    assertTrue(config.shouldUseDictionaryForKey("other_key"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyPerKey()
      throws Exception {
    FieldConfig country = JsonUtils.stringToObject(
        "{\"name\":\"country\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    FieldConfig clicks = JsonUtils.stringToObject(
        "{\"name\":\"clicks\",\"indexes\":{\"inverted\":{\"disabled\":true}}}", FieldConfig.class);
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, 1000, null, 0.5,
        List.of(country, clicks));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertFalse(config.shouldEnableInvertedIndexForKey("clicks"));
    // Unconfigured key falls back to built-in default of inverted-on.
    assertTrue(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testDefaultValueFieldConfigSuppressesInvertedDefault()
      throws Exception {
    // defaultValueFieldConfig with no indexes node turns off inverted for unconfigured keys.
    FieldConfig defaultFieldConfig = JsonUtils.stringToObject(
        "{\"name\":\"__default__\",\"encodingType\":\"DICTIONARY\"}", FieldConfig.class);
    OpenStructIndexConfig config =
        new OpenStructIndexConfig(false, defaultFieldConfig, 1000, null, 0.5, null);
    assertFalse(config.shouldEnableInvertedIndexForKey("any_key"));
    assertTrue(config.shouldUseDictionaryForKey("any_key"));
  }

  @Test
  public void testDefaultValueFieldConfigInvertedExplicit()
      throws Exception {
    FieldConfig defaultFieldConfig = JsonUtils.stringToObject(
        "{\"name\":\"__default__\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    OpenStructIndexConfig config =
        new OpenStructIndexConfig(false, defaultFieldConfig, 1000, null, 0.5, null);
    assertTrue(config.shouldEnableInvertedIndexForKey("any_key"));
  }

  @Test
  public void testPerKeyWinsOverDefaultValueFieldConfig()
      throws Exception {
    FieldConfig defaultFieldConfig = JsonUtils.stringToObject(
        "{\"name\":\"__default__\",\"encodingType\":\"DICTIONARY\"}", FieldConfig.class);
    FieldConfig country = JsonUtils.stringToObject(
        "{\"name\":\"country\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    OpenStructIndexConfig config =
        new OpenStructIndexConfig(false, defaultFieldConfig, 1000, null, 0.5, List.of(country));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    // Unconfigured key uses default (no inverted), not built-in.
    assertFalse(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testDefaultValueFieldConfigRawEncoding()
      throws Exception {
    FieldConfig defaultFieldConfig = JsonUtils.stringToObject(
        "{\"name\":\"__default__\",\"encodingType\":\"RAW\"}", FieldConfig.class);
    OpenStructIndexConfig config =
        new OpenStructIndexConfig(false, defaultFieldConfig, 1000, null, 0.5, null);
    assertFalse(config.shouldUseDictionaryForKey("any_key"));
  }

  @Test
  public void testShouldUseDictionaryForKeyHardOverride() {
    FieldConfig blob =
        new FieldConfig("blob", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    FieldConfig rawPayload =
        new FieldConfig("raw_payload", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, 1000, null, 0.5, List.of(blob, rawPayload));
    assertFalse(config.shouldUseDictionaryForKey("blob"));
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    // Unconfigured key falls back to built-in default (DICTIONARY).
    assertTrue(config.shouldUseDictionaryForKey("country"));
  }

  @Test
  public void testValueFieldConfigsRoundTrip()
      throws Exception {
    String json = "{\n"
        + "  \"maxDenseKeys\": 500,\n"
        + "  \"denseKeyMinFillRate\": 0.3,\n"
        + "  \"denseKeys\": [\"country\", \"clicks\"],\n"
        + "  \"defaultValueFieldConfig\": {\n"
        + "    \"name\": \"__default__\",\n"
        + "    \"encodingType\": \"DICTIONARY\",\n"
        + "    \"indexes\": {\"inverted\": {}}\n"
        + "  },\n"
        + "  \"valueFieldConfigs\": [\n"
        + "    {\n"
        + "      \"name\": \"country\",\n"
        + "      \"encodingType\": \"DICTIONARY\",\n"
        + "      \"indexes\": {\"inverted\": {}}\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"clicks\",\n"
        + "      \"encodingType\": \"RAW\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    OpenStructIndexConfig config = JsonUtils.stringToObject(json, OpenStructIndexConfig.class);

    assertEquals(config.getMaxDenseKeys(), 500);
    assertEquals(config.getDenseKeyMinFillRate(), 0.3);
    assertEquals(config.getDenseKeys(), Set.of("country", "clicks"));
    assertNotNull(config.getDefaultValueFieldConfig());

    List<FieldConfig> valueFieldConfigs = config.getValueFieldConfigs();
    assertNotNull(valueFieldConfigs);
    assertEquals(valueFieldConfigs.size(), 2);

    // country: dictionary + inverted index
    assertTrue(config.shouldUseDictionaryForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));

    // clicks: raw, no inverted (per-key wins, no indexes node)
    assertFalse(config.shouldUseDictionaryForKey("clicks"));
    assertFalse(config.shouldEnableInvertedIndexForKey("clicks"));

    // unconfigured key: defaultValueFieldConfig (dictionary + inverted)
    assertTrue(config.shouldUseDictionaryForKey("payload"));
    assertTrue(config.shouldEnableInvertedIndexForKey("payload"));

    // direct lookup
    assertNotNull(config.getValueFieldConfig("country"));
    assertEquals(config.getValueFieldConfig("country").getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertNull(config.getValueFieldConfig("missing"));

    // JSON serialization round-trip
    String reJson = JsonUtils.objectToString(config);
    OpenStructIndexConfig reDeserialized = JsonUtils.stringToObject(reJson, OpenStructIndexConfig.class);
    assertEquals(reDeserialized.getMaxDenseKeys(), 500);
    assertEquals(reDeserialized.getDenseKeys(), Set.of("country", "clicks"));
    assertNotNull(reDeserialized.getValueFieldConfigs());
    assertEquals(reDeserialized.getValueFieldConfigs().size(), 2);
    assertNotNull(reDeserialized.getDefaultValueFieldConfig());
    assertTrue(reDeserialized.shouldUseDictionaryForKey("country"));
    assertTrue(reDeserialized.shouldEnableInvertedIndexForKey("country"));
    assertFalse(reDeserialized.shouldUseDictionaryForKey("clicks"));
    assertTrue(reDeserialized.shouldEnableInvertedIndexForKey("payload"));
  }

  @Test
  public void testEmptyValueFieldConfigs()
      throws Exception {
    String json = "{\"valueFieldConfigs\": []}";
    OpenStructIndexConfig config = JsonUtils.stringToObject(json, OpenStructIndexConfig.class);
    assertNotNull(config.getValueFieldConfigs());
    assertTrue(config.getValueFieldConfigs().isEmpty());
    assertNull(config.getValueFieldConfig("any"));
    // Unconfigured key falls back to built-in defaults.
    assertTrue(config.shouldUseDictionaryForKey("any"));
    assertTrue(config.shouldEnableInvertedIndexForKey("any"));
  }

  @Test
  public void testDisabledViaJson()
      throws Exception {
    String json = "{\"disabled\": true}";
    OpenStructIndexConfig config = JsonUtils.stringToObject(json, OpenStructIndexConfig.class);
    assertFalse(config.isEnabled());
  }

  @Test
  public void testEmptyJsonDefaults()
      throws Exception {
    OpenStructIndexConfig config = JsonUtils.stringToObject("{}", OpenStructIndexConfig.class);
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxDenseKeys(), OpenStructIndexConfig.DEFAULT_MAX_DENSE_KEYS);
    assertEquals(config.getDenseKeyMinFillRate(), OpenStructIndexConfig.DEFAULT_DENSE_KEY_MIN_FILL_RATE);
    assertTrue(config.getDenseKeys().isEmpty());
    assertNull(config.getValueFieldConfigs());
    assertNull(config.getDefaultValueFieldConfig());
  }

  @Test
  public void testPartialJsonRetainsDefaultFillRate()
      throws Exception {
    OpenStructIndexConfig config =
        JsonUtils.stringToObject("{\"maxDenseKeys\": 500}", OpenStructIndexConfig.class);
    assertEquals(config.getMaxDenseKeys(), 500);
    assertEquals(config.getDenseKeyMinFillRate(), OpenStructIndexConfig.DEFAULT_DENSE_KEY_MIN_FILL_RATE);
  }
}
