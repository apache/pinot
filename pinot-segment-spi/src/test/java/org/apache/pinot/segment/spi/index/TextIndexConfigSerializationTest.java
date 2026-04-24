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
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that {@link TextIndexConfig} Jackson serialization uses the curated
 * {@code @JsonValue toJsonObject()} method and emits each of the 18 lucene defaults only when
 * the field differs from its constant default. Empty / null lists are treated as defaults.
 *
 * <p>This module ({@code pinot-segment-spi}) does not contain a concrete builder, so the test
 * uses an in-line anonymous subclass of {@link TextIndexConfig.AbstractBuilder}.
 */
public class TextIndexConfigSerializationTest {

  @Test
  public void testDefaultBuiltConfigSerializesToEmptyJson()
      throws Exception {
    TextIndexConfig config = newBuilder().build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node);
  }

  @Test
  public void testDisabledTrueIsMinimal()
      throws Exception {
    // TextIndexConfig.DISABLED is constructed with luceneUseCompoundFile=false (vs. default true),
    // so the slim form correctly carries that one extra field beyond "disabled". This documents
    // an intentional quirk of the DISABLED constant rather than a serializer bug.
    JsonNode node = serializeToNode(TextIndexConfig.DISABLED);

    assertOnlyKeys(node, "disabled", "luceneUseCompoundFile");
    assertTrue(node.get("disabled").asBoolean());
    assertTrue(!node.get("luceneUseCompoundFile").asBoolean());
  }

  @Test
  public void testNonDefaultLuceneMaxBufferSizeMBEmitted()
      throws Exception {
    TextIndexConfig config = newBuilder().withLuceneMaxBufferSizeMB(1024).build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "luceneMaxBufferSizeMB");
    assertEquals(node.get("luceneMaxBufferSizeMB").asInt(), 1024);
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
  public void testCaseSensitiveAndStoreInSegmentFileEmittedWhenNonDefault()
      throws Exception {
    TextIndexConfig config = newBuilder()
        .withCaseSensitive(true)
        .withStoreInSegmentFile(true)
        .build();

    JsonNode node = serializeToNode(config);

    assertOnlyKeys(node, "caseSensitive", "storeInSegmentFile");
  }

  @Test
  public void testFatJsonStillDeserializes()
      throws Exception {
    String fat = "{"
        + "\"disabled\":false,"
        + "\"luceneUseCompoundFile\":true,"
        + "\"luceneMaxBufferSizeMB\":500,"
        + "\"reuseMutableIndex\":false,"
        + "\"caseSensitive\":false,"
        + "\"storeInSegmentFile\":false"
        + "}";

    TextIndexConfig config = JsonUtils.stringToObject(fat, TextIndexConfig.class);

    assertOnlyKeys(serializeToNode(config));
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

  /**
   * Builds a concrete subclass of the abstract builder that no-ops {@code withProperties()}.
   * Tests do not exercise the property-bag path; they target the explicit fluent setters.
   */
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
}
