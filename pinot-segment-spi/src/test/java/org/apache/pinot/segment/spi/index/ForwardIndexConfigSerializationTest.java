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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link ForwardIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each field only when it differs from its
 * class default.
 *
 * <p>Particular attention to the cluster-tunable static defaults
 * ({@code _defaultRawIndexWriterVersion}, {@code _defaultTargetMaxChunkSize},
 * {@code _defaultTargetDocsPerChunk}): the serializer must read these at call time so that the
 * serialized form tracks live cluster config.
 */
public class ForwardIndexConfigSerializationTest {

  // Snapshot the cluster-tunable statics so we can restore them after each test.
  private final int _savedRawIndexWriterVersion = ForwardIndexConfig.getDefaultRawWriterVersion();
  private final String _savedTargetMaxChunkSize = ForwardIndexConfig.getDefaultTargetMaxChunkSize();
  private final int _savedTargetDocsPerChunk = ForwardIndexConfig.getDefaultTargetDocsPerChunk();

  @AfterMethod
  public void restoreClusterDefaults() {
    ForwardIndexConfig.setDefaultRawIndexWriterVersion(_savedRawIndexWriterVersion);
    ForwardIndexConfig.setDefaultTargetMaxChunkSize(_savedTargetMaxChunkSize);
    ForwardIndexConfig.setDefaultTargetDocsPerChunk(_savedTargetDocsPerChunk);
  }

  @Test
  public void testDefaultPojoSerializesToEmptyJson()
      throws Exception {
    ForwardIndexConfig config = ForwardIndexConfig.getDefault();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    ForwardIndexConfig config = ForwardIndexConfig.getDisabled();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testCompressionCodecEmittedWhenSet()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withCompressionCodec(CompressionCodec.SNAPPY).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "compressionCodec");
    assertEquals(node.get("compressionCodec").asText(), "SNAPPY");
  }

  @Test
  public void testDeriveNumDocsPerChunkEmittedWhenTrue()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withDeriveNumDocsPerChunk(true).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "deriveNumDocsPerChunk");
    assertTrue(node.get("deriveNumDocsPerChunk").asBoolean());
  }

  @Test
  public void testNonDefaultRawIndexWriterVersionEmitted()
      throws Exception {
    int nonDefault = ForwardIndexConfig.getDefaultRawWriterVersion() + 1;
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withRawIndexWriterVersion(nonDefault).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "rawIndexWriterVersion");
    assertEquals(node.get("rawIndexWriterVersion").asInt(), nonDefault);
  }

  /**
   * The cluster-tunable static {@code _defaultRawIndexWriterVersion} is read at call time, so a
   * config matching the live cluster default must serialize as empty even after the static is
   * mutated.
   */
  @Test
  public void testClusterTunableDefaultReadAtCallTime()
      throws Exception {
    int original = ForwardIndexConfig.getDefaultRawWriterVersion();
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withRawIndexWriterVersion(original).build();

    // Sanity: equal to live default, so omitted.
    assertOnlyKeys(serializeToNode(config));

    // Mutate live default; the same instance now differs from the live default.
    int mutated = original + 1;
    ForwardIndexConfig.setDefaultRawIndexWriterVersion(mutated);
    JsonNode mutatedNode = serializeToNode(config);
    assertEquals(mutatedNode.get("rawIndexWriterVersion").asInt(), original);

    // Restore so a config built at the new default again serializes empty.
    ForwardIndexConfig matchingNewDefault = new ForwardIndexConfig.Builder()
        .withRawIndexWriterVersion(mutated).build();
    assertOnlyKeys(serializeToNode(matchingNewDefault));
  }

  @Test
  public void testDeprecatedKeysNotEmitted()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withCompressionCodec(CompressionCodec.SNAPPY).build();

    JsonNode node = serializeToNode(config);

    assertFalse(node.has("chunkCompressionType"), "Deprecated chunkCompressionType must not be emitted: " + node);
    assertFalse(node.has("dictIdCompressionType"), "Deprecated dictIdCompressionType must not be emitted: " + node);
    assertFalse(node.has("configs"), "configs (already @JsonIgnore on getter) must not be emitted: " + node);
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"disabled\":false,"
        + "\"compressionCodec\":null,"
        + "\"deriveNumDocsPerChunk\":false,"
        + "\"rawIndexWriterVersion\":" + ForwardIndexConfig.getDefaultRawWriterVersion() + ","
        + "\"targetMaxChunkSize\":\"" + ForwardIndexConfig.getDefaultTargetMaxChunkSize() + "\","
        + "\"targetDocsPerChunk\":" + ForwardIndexConfig.getDefaultTargetDocsPerChunk()
        + "}";

    ForwardIndexConfig config = JsonUtils.stringToObject(fat, ForwardIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withCompressionCodec(CompressionCodec.LZ4)
        .withDeriveNumDocsPerChunk(true)
        .build();

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder()
        .withCompressionCodec(CompressionCodec.SNAPPY).build();

    String json = new ObjectMapper().writeValueAsString(config);

    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  // ---- Helpers ----

  private static JsonNode serializeToNode(Object pojo)
      throws Exception {
    return JsonUtils.stringToJsonNode(JsonUtils.objectToString(pojo));
  }

  private static void assertOnlyKeys(JsonNode node, String... expectedKeys) {
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    assertEquals(actual, Set.of(expectedKeys), "Unexpected key set: " + node);
  }
}
