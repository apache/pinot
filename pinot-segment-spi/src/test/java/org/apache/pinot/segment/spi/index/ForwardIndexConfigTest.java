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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ForwardIndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
    assertEquals(config.getEncodingType(), FieldConfig.EncodingType.DICTIONARY,
        "Missing encodingType should fall back to DICTIONARY for backward compatibility");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withNegativeTargetDocsPerChunk()
      throws JsonProcessingException {
    String confStr = "{\"targetDocsPerChunk\": \"-1\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertEquals(config.getTargetDocsPerChunk(), -1, "Unexpected defaultTargetDocsPerChunk");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"chunkCompressionType\": \"SNAPPY\",\n"
        + "        \"deriveNumDocsPerChunk\": true,\n"
        + "        \"rawIndexWriterVersion\": 10,\n"
        + "        \"targetMaxChunkSize\": \"512K\",\n"
        + "        \"targetDocsPerChunk\": \"2000\"\n"
        + "}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.SNAPPY, "Unexpected chunkCompressionType");
    assertTrue(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), 10, "Unexpected rawIndexWriterVersion");
    assertEquals(config.getTargetMaxChunkSizeBytes(), 512 * 1024, "Unexpected targetMaxChunkSizeBytes");
    assertEquals(config.getTargetDocsPerChunk(), 2000, "Unexpected defaultTargetDocsPerChunk");
  }

  @Test
  public void defaultConfigUsesDictionaryEncoding() {
    ForwardIndexConfig config = ForwardIndexConfig.getDefault(FieldConfig.EncodingType.DICTIONARY);
    assertEquals(config.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
  }

  @Test
  public void explicitEncodingIsSerialized() {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.DICTIONARY)
        .withRawIndexWriterVersion(6)
        .build();
    JsonNode json = config.toJsonNode();
    assertTrue(json.has("encodingType"), "Resolved encodingType should always be serialized");
    assertEquals(json.get("encodingType").asText(), FieldConfig.EncodingType.DICTIONARY.name());
  }

  @Test
  public void explicitDictionaryEncodingIsSerialized() {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.DICTIONARY)
        .build();
    JsonNode json = config.toJsonNode();
    assertTrue(json.has("encodingType"), "Explicit encodingType should be preserved even when it is the default value");
    assertEquals(json.get("encodingType").asText(), FieldConfig.EncodingType.DICTIONARY.name());
    assertEquals(config.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
  }

  @Test
  public void builderRetainsExplicitlySetEncoding() {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .build();
    assertEquals(config.getEncodingType(), FieldConfig.EncodingType.RAW);
  }

  @Test
  public void builderCopyConstructorPreservesEncoding() {
    ForwardIndexConfig original = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .build();
    ForwardIndexConfig copy = new ForwardIndexConfig.Builder(original).build();
    assertEquals(copy.getEncodingType(), FieldConfig.EncodingType.RAW,
        "Copy constructor must preserve encodingType");
  }

  @Test
  public void builderCopyConstructorPreservesDisabledConfig() {
    ForwardIndexConfig disabled = ForwardIndexConfig.getDisabled();
    ForwardIndexConfig copy = new ForwardIndexConfig.Builder(disabled).build();
    assertTrue(copy.isDisabled(), "Copy constructor must preserve disabled config");
    assertEquals(copy.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
  }

  @Test
  public void equalsAndHashCodeDistinguishEncoding() {
    ForwardIndexConfig dict = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.DICTIONARY)
        .build();
    ForwardIndexConfig raw = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .build();
    assertNotEquals(dict, raw, "Configs differing only in encoding must not be equal");
  }

  @Test
  public void encodingRoundTripsThroughJson()
      throws JsonProcessingException {
    ForwardIndexConfig original = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .build();
    String json = JsonUtils.objectToString(original);
    ForwardIndexConfig restored = JsonUtils.stringToObject(json, ForwardIndexConfig.class);
    assertEquals(restored.getEncodingType(), FieldConfig.EncodingType.RAW,
        "JSON serialize/deserialize must preserve encodingType");
    assertEquals(restored, original, "Round-tripped config must equal the original");
  }

  @Test
  public void encodingDeserializesFromExplicitJsonField()
      throws JsonProcessingException {
    String confStr = "{\"encodingType\": \"RAW\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);
    assertEquals(config.getEncodingType(), FieldConfig.EncodingType.RAW);
  }
}
