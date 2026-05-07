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
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link ForwardIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits non-cluster-tunable fields only when they
 * differ from their class default.
 *
 * <p>Particular attention to the cluster-tunable trio ({@code rawIndexWriterVersion},
 * {@code targetMaxChunkSize}, {@code targetDocsPerChunk}): these are <i>always</i> materialized
 * regardless of how they relate to the JVM-local static defaults, because
 * {@code ServiceStartableUtils.initForwardIndexConfig} mutates those statics per-instance from
 * instance config. Slim-omitting them would let the same ZK payload resolve to different
 * forward-index settings on different nodes during a rolling upgrade or mismatched-config window.
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

  // The cluster-tunable trio is always materialized by the slim serializer (see class Javadoc).
  // The keys are: rawIndexWriterVersion, targetMaxChunkSize, targetDocsPerChunk.
  private static final String[] ALWAYS_EMITTED = {
      "rawIndexWriterVersion", "targetMaxChunkSize", "targetDocsPerChunk"
  };

  @Test
  public void testDefaultPojoSerializesOnlyClusterTunableTrio()
      throws Exception {
    ForwardIndexConfig config = ForwardIndexConfig.getDefault(EncodingType.DICTIONARY);

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, ALWAYS_EMITTED);
    assertEquals(node.get("rawIndexWriterVersion").asInt(), ForwardIndexConfig.getDefaultRawWriterVersion());
    assertEquals(node.get("targetMaxChunkSize").asText(), ForwardIndexConfig.getDefaultTargetMaxChunkSize());
    assertEquals(node.get("targetDocsPerChunk").asInt(), ForwardIndexConfig.getDefaultTargetDocsPerChunk());
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    ForwardIndexConfig config = ForwardIndexConfig.getDisabled();

    JsonNode node = serializeToNode(config);

    // getDisabled() builds via the explicit ctor with all-null settings, so the cluster-tunable
    // trio is materialized to live defaults.
    assertOnlyKeys(node, "disabled", "rawIndexWriterVersion", "targetMaxChunkSize", "targetDocsPerChunk");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testCompressionCodecEmittedWhenSet()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withCompressionCodec(CompressionCodec.SNAPPY).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "compressionCodec", "rawIndexWriterVersion", "targetMaxChunkSize", "targetDocsPerChunk");
    assertEquals(node.get("compressionCodec").asText(), "SNAPPY");
  }

  @Test
  public void testDeriveNumDocsPerChunkEmittedWhenTrue()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withDeriveNumDocsPerChunk(true).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "deriveNumDocsPerChunk", "rawIndexWriterVersion", "targetMaxChunkSize", "targetDocsPerChunk");
    assertTrue(node.get("deriveNumDocsPerChunk").asBoolean());
  }

  @Test
  public void testNonDefaultRawIndexWriterVersionEmitted()
      throws Exception {
    int nonDefault = ForwardIndexConfig.getDefaultRawWriterVersion() + 1;
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withRawIndexWriterVersion(nonDefault).build();

    JsonNode node = serializeToNode(config);

    assertEquals(node.get("rawIndexWriterVersion").asInt(), nonDefault);
  }

  /**
   * The cluster-tunable trio must be persisted regardless of whether the live JVM-local default
   * matches the resolved value, so that a node with a different local default does not silently
   * read a different effective forward-index setting from the same ZK payload. This test pins the
   * "always materialize" contract: mutating the static after construction does not change what is
   * emitted — both the original and mutated runs emit the value the POJO is carrying.
   */
  @Test
  public void testClusterTunableTrioAlwaysMaterialized()
      throws Exception {
    int original = ForwardIndexConfig.getDefaultRawWriterVersion();
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withRawIndexWriterVersion(original).build();

    JsonNode firstNode = serializeToNode(config);
    assertTrue(firstNode.has("rawIndexWriterVersion"),
        "Cluster-tunable rawIndexWriterVersion must always be materialized: " + firstNode);
    assertEquals(firstNode.get("rawIndexWriterVersion").asInt(), original);

    // Mutating the live default must not affect what is emitted by the same instance.
    ForwardIndexConfig.setDefaultRawIndexWriterVersion(original + 1);
    JsonNode mutatedNode = serializeToNode(config);
    assertEquals(mutatedNode.get("rawIndexWriterVersion").asInt(), original,
        "Serialized value must be the POJO's stored value, not the (mutated) live default: " + mutatedNode);
  }

  @Test
  public void testDeprecatedKeysNotEmitted()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withCompressionCodec(CompressionCodec.SNAPPY).build();

    JsonNode node = serializeToNode(config);

    assertFalse(node.has("chunkCompressionType"), "Deprecated chunkCompressionType must not be emitted: " + node);
    assertFalse(node.has("dictIdCompressionType"), "Deprecated dictIdCompressionType must not be emitted: " + node);
    assertFalse(node.has("configs"), "configs (already @JsonIgnore on getter) must not be emitted: " + node);
  }

  @Test
  public void testFatJsonStillDeserializesAndReSerializesWithTrio()
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

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, ALWAYS_EMITTED);
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
        .withCompressionCodec(CompressionCodec.LZ4)
        .withDeriveNumDocsPerChunk(true)
        .build();

    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(EncodingType.DICTIONARY)
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
