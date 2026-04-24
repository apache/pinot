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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/**
 * Cross-cutting test that exercises the full slim-serialization wire path for every
 * supported {@code *IndexConfig}: a slim user-supplied JSON travels through a
 * {@link org.apache.pinot.spi.config.table.FieldConfig} declared on a
 * {@link org.apache.pinot.spi.config.table.TableConfig}, gets resolved into the
 * concrete {@code *IndexConfig} via {@link org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil},
 * and is then re-serialized. The re-serialized form must be byte-equivalent to the
 * original slim input — i.e. the {@code @JsonValue toJsonObject()} on each config
 * must round-trip through the {@code IndexType.getConfig(...)} resolution path.
 *
 * <p>This guards against any wiring regression between the per-class slim serializers
 * (added in this PR) and the broker/server-side index resolution code.
 */
public class IndexConfigSlimRoundTripTest extends AbstractSerdeIndexContract {

  @DataProvider(name = "slimConfigs")
  public static Object[][] slimConfigs() {
    return new Object[][] {
        // forward
        {"forward", "{\"compressionCodec\":\"SNAPPY\"}", "ForwardIndexConfig"},
        {"forward", "{\"deriveNumDocsPerChunk\":true}", "ForwardIndexConfig"},
        // dictionary
        {"dictionary", "{\"onHeap\":true}", "DictionaryIndexConfig"},
        {"dictionary", "{\"useVarLengthDictionary\":true}", "DictionaryIndexConfig"},
        // range
        {"range", "{\"version\":1}", "RangeIndexConfig"},
        // json
        {"json", "{\"excludeArray\":true}", "JsonIndexConfig"},
        {"json", "{\"maxLevels\":3}", "JsonIndexConfig"},
        // bloom
        {"bloom", "{\"fpp\":0.5}", "BloomFilterConfig"},
        {"bloom", "{\"maxSizeInBytes\":1024,\"loadOnHeap\":true}", "BloomFilterConfig"},
        // text
        {"text", "{\"caseSensitive\":true}", "TextIndexConfig"},
        // h3
        {"h3", "{\"resolution\":[5,6]}", "H3IndexConfig"},
        // disabled (universal)
        {"bloom", "{\"disabled\":true}", "BloomFilterConfig"},
        {"forward", "{\"disabled\":true}", "ForwardIndexConfig"},
    };
  }

  /**
   * For each slim JSON input, attach it as a {@code FieldConfig.indexes[<name>]} on the
   * shared table config, resolve via {@link #getActualConfig}, then re-serialize the
   * resolved POJO through Jackson and assert the slim output equals the original input.
   */
  @Test(dataProvider = "slimConfigs")
  public void slimRoundTripsThroughIndexResolution(String indexName, String slimJson, String label)
      throws IOException {
    addFieldIndexConfig("{"
        + "\"name\": \"dimInt\","
        + "\"indexes\": {"
        + "\"" + indexName + "\": " + slimJson
        + "}"
        + "}");

    IndexType<? extends IndexConfig, ?, ?> type = lookupIndexType(indexName);
    IndexConfig resolved = getActualConfig("dimInt", type);

    JsonNode resolvedSlim = JsonUtils.stringToJsonNode(JsonUtils.objectToString(resolved));
    JsonNode expectedSlim = JsonUtils.stringToJsonNode(slimJson);

    assertEquals(resolvedSlim, expectedSlim,
        label + " must round-trip slim through IndexType.getConfig(): "
            + "expected " + expectedSlim + " but got " + resolvedSlim);
  }

  /**
   * Sanity guard: ensure no resolved config emits a known "fattening" key when the slim
   * input omitted it. This is a strict subset of the round-trip assertion above but
   * fails with a clearer message when a serializer regresses.
   */
  @Test
  public void slimForwardConfigDoesNotEmitClusterTunableDefaults()
      throws IOException {
    addFieldIndexConfig("{"
        + "\"name\": \"dimInt\","
        + "\"indexes\": {\"forward\": {\"compressionCodec\":\"SNAPPY\"}}"
        + "}");

    IndexConfig resolved = getActualConfig("dimInt", StandardIndexes.forward());
    JsonNode slim = JsonUtils.stringToJsonNode(JsonUtils.objectToString(resolved));

    assertFalse(slim.has("rawIndexWriterVersion"),
        "Cluster-tunable default rawIndexWriterVersion must not leak into slim output: " + slim);
    assertFalse(slim.has("targetMaxChunkSize"),
        "Cluster-tunable default targetMaxChunkSize must not leak into slim output: " + slim);
    assertFalse(slim.has("targetDocsPerChunk"),
        "Cluster-tunable default targetDocsPerChunk must not leak into slim output: " + slim);
  }

  // ---- Helpers ----

  private static IndexType<? extends IndexConfig, ?, ?> lookupIndexType(String name) {
    switch (name) {
      case "forward":
        return StandardIndexes.forward();
      case "dictionary":
        return StandardIndexes.dictionary();
      case "range":
        return StandardIndexes.range();
      case "json":
        return StandardIndexes.json();
      case "bloom":
        return StandardIndexes.bloomFilter();
      case "text":
        return StandardIndexes.text();
      case "h3":
        return StandardIndexes.h3();
      case "fst":
        return StandardIndexes.fst();
      case "vector":
        return StandardIndexes.vector();
      default:
        throw new IllegalArgumentException("Unknown index name: " + name);
    }
  }
}
