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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FieldConfigTest {
  @Test
  @SuppressWarnings("deprecation")
  public void missingDeprecatedEncodingTypeDefaultsToDictionaryButDoesNotSerialize()
      throws IOException {
    FieldConfig fieldConfig = JsonUtils.stringToObject("{\"name\":\"col\"}", FieldConfig.class);

    assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertFalse(fieldConfig.hasFieldLevelEncodingType());
    assertFalse(fieldConfig.hasFieldLevelCompressionCodec());
    assertFalse(fieldConfig.hasFieldLevelProperties());
    assertFalse(fieldConfig.toJsonNode().has("encodingType"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void explicitDeprecatedEncodingTypeIsPreservedForBackwardCompatibility()
      throws IOException {
    FieldConfig fieldConfig = JsonUtils.stringToObject("{\"name\":\"col\",\"encodingType\":\"RAW\"}",
        FieldConfig.class);
    JsonNode jsonNode = fieldConfig.toJsonNode();

    assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);
    assertTrue(fieldConfig.hasFieldLevelEncodingType());
    assertEquals(jsonNode.get("encodingType").asText(), FieldConfig.EncodingType.RAW.name());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void explicitDeprecatedCompressionCodecAndPropertiesArePreservedForBackwardCompatibility()
      throws IOException {
    FieldConfig fieldConfig = JsonUtils.stringToObject(
        "{\"name\":\"col\",\"compressionCodec\":\"SNAPPY\",\"properties\":{\"rawIndexWriterVersion\":\"4\"}}",
        FieldConfig.class);
    JsonNode jsonNode = fieldConfig.toJsonNode();

    assertEquals(fieldConfig.getCompressionCodec(), FieldConfig.CompressionCodec.SNAPPY);
    assertTrue(fieldConfig.hasFieldLevelCompressionCodec());
    assertEquals(fieldConfig.getProperties().get(FieldConfig.RAW_INDEX_WRITER_VERSION), "4");
    assertTrue(fieldConfig.hasFieldLevelProperties());
    assertEquals(jsonNode.get("compressionCodec").asText(), FieldConfig.CompressionCodec.SNAPPY.name());
    assertEquals(jsonNode.get("properties").get(FieldConfig.RAW_INDEX_WRITER_VERSION).asText(), "4");
  }
}
