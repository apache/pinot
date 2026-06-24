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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Slim-serialization contract for [TextIndexConfig].
public class TextIndexConfigSerializationTest {

  @Test
  public void testRoundTripAllNull()
      throws Exception {
    TextIndexConfig config = newBuilder().build();

    assertOnlyKeys(serializeToNode(config));
    assertRoundTripIdempotent(config);

    // Getters fall back to documented defaults.
    assertEquals(config.isEnableQueryCache(), false);
    assertEquals(config.isUseANDForMultiTermQueries(), false);
    assertEquals(config.isLuceneUseCompoundFile(), true);
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500);
    assertEquals(config.getLuceneAnalyzerClass(), FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS);
    assertEquals(config.getLuceneQueryParserClass(), FieldConfig.TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS);
    assertEquals(config.isEnablePrefixSuffixMatchingInPhraseQueries(), false);
    assertEquals(config.isReuseMutableIndex(), false);
    assertEquals(config.getLuceneNRTCachingDirectoryMaxBufferSizeMB(), 0);
    assertEquals(config.isUseLogByteSizeMergePolicy(), false);
    assertEquals(config.isCaseSensitive(), false);
    assertEquals(config.isStoreInSegmentFile(), false);
  }

  @Test
  public void testRoundTripPartial()
      throws Exception {
    TextIndexConfig config = newBuilder().withLuceneMaxBufferSizeMB(1024).withCaseSensitive(true).build();

    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "luceneMaxBufferSizeMB", "caseSensitive");
    assertEquals(node.get("luceneMaxBufferSizeMB").asInt(), 1024);
    assertTrue(node.get("caseSensitive").asBoolean());
    assertRoundTripIdempotent(config);

    assertEquals(config.getLuceneMaxBufferSizeMB(), 1024);
    assertTrue(config.isCaseSensitive());
    assertEquals(config.isLuceneUseCompoundFile(), true); // default applied
  }

  @Test
  public void testRoundTripAllSet()
      throws Exception {
    TextIndexConfig config = newBuilder()
        .withUseANDForMultiTermQueries(true)
        .withStopWordsInclude(List.of("the", "a"))
        .withStopWordsExclude(List.of("is"))
        .withLuceneUseCompoundFile(false)
        .withLuceneMaxBufferSizeMB(800)
        .withLuceneAnalyzerClass("org.example.MyAnalyzer")
        .withLuceneAnalyzerClassArgs(List.of("foo"))
        .withLuceneAnalyzerClassArgTypes(List.of("java.lang.String"))
        .withLuceneQueryParserClass("org.example.MyParser")
        .withEnablePrefixSuffixMatchingInPhraseQueries(true)
        .withReuseMutableIndex(true)
        .withLuceneNRTCachingDirectoryMaxBufferSizeMB(64)
        .withUseLogByteSizeMergePolicy(true)
        .withDocIdTranslatorMode("Default")
        .withCaseSensitive(true)
        .withStoreInSegmentFile(true)
        .build();
    // queryCache is not exposed via the Builder; flip it via copy-constructor pattern.

    JsonNode node = serializeToNode(config);
    assertTrue(node.has("useANDForMultiTermQueries"));
    assertTrue(node.has("stopWordsInclude"));
    assertTrue(node.has("stopWordsExclude"));
    assertTrue(node.has("luceneUseCompoundFile"));
    assertTrue(node.has("luceneMaxBufferSizeMB"));
    assertTrue(node.has("luceneAnalyzerClass"));
    assertTrue(node.has("luceneAnalyzerClassArgs"));
    assertTrue(node.has("luceneAnalyzerClassArgTypes"));
    assertTrue(node.has("luceneQueryParserClass"));
    assertTrue(node.has("enablePrefixSuffixMatchingInPhraseQueries"));
    assertTrue(node.has("reuseMutableIndex"));
    assertTrue(node.has("luceneNRTCachingDirectoryMaxBufferSizeMB"));
    assertTrue(node.has("useLogByteSizeMergePolicy"));
    assertTrue(node.has("docIdTranslatorMode"));
    assertTrue(node.has("caseSensitive"));
    assertTrue(node.has("storeInSegmentFile"));
    assertRoundTripIdempotent(config);
  }

  @Test
  public void testDisabledConstantOnlyEmitsDisabled()
      throws Exception {
    JsonNode node = serializeToNode(TextIndexConfig.DISABLED);
    assertOnlyKeys(node, "disabled");
    assertTrue(node.get("disabled").asBoolean());
  }

  @Test
  public void testStopWordsIncludeEmittedOnlyWhenNonEmpty()
      throws Exception {
    TextIndexConfig empty = newBuilder().withStopWordsInclude(List.of()).build();
    assertOnlyKeys(serializeToNode(empty));

    TextIndexConfig populated = newBuilder().withStopWordsInclude(List.of("the", "a")).build();
    JsonNode node = serializeToNode(populated);
    assertOnlyKeys(node, "stopWordsInclude");
    assertEquals(node.get("stopWordsInclude").size(), 2);
  }

  @Test
  public void testExplicitDefaultPreserved()
      throws Exception {
    // Build via the wrapper @JsonCreator directly (the Builder path uses primitive fields for binary
    // compat and therefore can't preserve explicit-default). User explicitly sets
    // luceneUseCompoundFile=true (today's default). Slim form keeps the key so a future default
    // change does not silently swallow user intent.
    TextIndexConfig config = new TextIndexConfig(Boolean.FALSE, null, null, null, null, null,
        Boolean.TRUE, null, null, (Object) null, (Object) null, null, null, null, null, null, null,
        null, null);
    JsonNode node = serializeToNode(config);
    assertOnlyKeys(node, "luceneUseCompoundFile");
    assertEquals(node.get("luceneUseCompoundFile").asBoolean(), true);

    TextIndexConfig nullConfig = new TextIndexConfig(Boolean.FALSE, null, null, null, null, null, null, null, null,
        (Object) null, (Object) null, null, null, null, null, null, null, null, null);
    assertNotEquals(config, nullConfig,
        "Explicit-default-value config must NOT equal the all-null ctor output");
  }

  @Test
  public void testJacksonSerializationMatchesToJsonObject()
      throws Exception {
    TextIndexConfig config = newBuilder()
        .withLuceneMaxBufferSizeMB(800)
        .withCaseSensitive(true)
        .build();
    assertEquals(serializeToNode(config), config.toJsonObject());
  }

  @Test
  public void testFreshObjectMapperUsesJsonValue()
      throws Exception {
    TextIndexConfig config = newBuilder().withCaseSensitive(true).build();
    String json = new ObjectMapper().writeValueAsString(config);
    assertEquals(JsonUtils.stringToJsonNode(json), config.toJsonObject());
  }

  // ---- Helpers ----

  /// Anonymous concrete builder for tests; this module has no production builder subclass.
  private static TextIndexConfig.AbstractBuilder newBuilder() {
    return new TextIndexConfig.AbstractBuilder() {
      @Override
      public TextIndexConfig.AbstractBuilder withProperties(@Nullable Map<String, String> textIndexProperties) {
        return this;
      }
    };
  }

  private static JsonNode serializeToNode(Object pojo)
      throws Exception {
    return JsonUtils.stringToJsonNode(JsonUtils.objectToString(pojo));
  }

  private static void assertOnlyKeys(JsonNode node, String... expectedKeys) {
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    assertEquals(actual, Set.of(expectedKeys), "Unexpected key set: " + node);
  }

  private static void assertRoundTripIdempotent(TextIndexConfig original)
      throws Exception {
    JsonNode firstNode = serializeToNode(original);
    TextIndexConfig restored =
        JsonUtils.stringToObject(JsonUtils.objectToString(original), TextIndexConfig.class);
    JsonNode secondNode = serializeToNode(restored);
    assertEquals(secondNode, firstNode, "Slim form must be byte-equivalent across a round-trip");
  }
}
