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
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.*;


public class TextIndexConfigTest {
  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
    assertTrue(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500, "Unexpected luceneMaxBufferSize");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
    assertTrue(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500, "Unexpected luceneMaxBufferSize");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
    assertTrue(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500, "Unexpected luceneMaxBufferSize");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getFstType(), "Unexpected fst");
    assertNull(config.getRawValueForTextIndex(), "Unexpected rawValue");
    assertFalse(config.isEnableQueryCache(), "Unexpected queryCache");
    assertFalse(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertNull(config.getStopWordsInclude(), "Unexpected stopWordsInclude");
    assertNull(config.getStopWordsExclude(), "Unexpected stopWordsExclude");
    assertTrue(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500, "Unexpected luceneMaxBufferSize");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"fst\": \"NATIVE\",\n"
        + "        \"rawValue\": \"fakeValue\",\n"
        + "        \"queryCache\": true,\n"
        + "        \"useANDForMultiTermQueries\": true,\n"
        + "        \"stopWordsInclude\": [\"a\"],\n"
        + "        \"stopWordsExclude\": [\"b\"],\n"
        + "        \"luceneUseCompoundFile\": false,\n"
        + "        \"luceneMaxBufferSizeMB\": 1024\n"
        + "}";
    TextIndexConfig config = JsonUtils.stringToObject(confStr, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getFstType(), FSTType.NATIVE, "Unexpected fst");
    assertEquals(config.getRawValueForTextIndex(), "fakeValue", "Unexpected rawValue");
    assertTrue(config.isEnableQueryCache(), "Unexpected queryCache");
    assertTrue(config.isUseANDForMultiTermQueries(), "Unexpected useANDForMultiTermQueries");
    assertEquals(config.getStopWordsInclude(), Lists.newArrayList("a"), "Unexpected stopWordsInclude");
    assertEquals(config.getStopWordsExclude(), Lists.newArrayList("b"), "Unexpected stopWordsExclude");
    assertFalse(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 1024, "Unexpected luceneMaxBufferSize");
  }

  @Test
  public void testRoundTripSerializationWithArrayValues()
      throws JsonProcessingException {
    // This test verifies that TextIndexConfig can be round-tripped through JSON serialization.
    // When serialized, luceneAnalyzerClassArgs/ArgTypes are output as arrays (from List<String> getters).
    // When deserialized, the constructor must accept both String (CSV) and Array (List) formats.
    final String confStrWithArrays = "{\n"
        + "        \"disabled\": false,\n"
        + "        \"luceneUseCompoundFile\": true,\n"
        + "        \"luceneMaxBufferSizeMB\": 500,\n"
        + "        \"luceneAnalyzerClass\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\",\n"
        + "        \"luceneAnalyzerClassArgs\": [],\n"
        + "        \"luceneAnalyzerClassArgTypes\": []\n"
        + "}";
    final TextIndexConfig config = JsonUtils.stringToObject(confStrWithArrays, TextIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertTrue(config.isLuceneUseCompoundFile(), "Unexpected luceneUseCompoundFile");
    assertEquals(config.getLuceneMaxBufferSizeMB(), 500, "Unexpected luceneMaxBufferSizeMB");
    assertEquals(config.getLuceneAnalyzerClass(),
        "org.apache.lucene.analysis.standard.StandardAnalyzer", "Unexpected luceneAnalyzerClass");
    assertTrue(config.getLuceneAnalyzerClassArgs().isEmpty(), "Unexpected luceneAnalyzerClassArgs");
    assertTrue(config.getLuceneAnalyzerClassArgTypes().isEmpty(), "Unexpected luceneAnalyzerClassArgTypes");

    // Now test with non-empty arrays
    final String confStrWithNonEmptyArrays = "{\n"
        + "        \"luceneAnalyzerClassArgs\": [\"arg1\", \"arg2\"],\n"
        + "        \"luceneAnalyzerClassArgTypes\": [\"java.lang.String\", \"java.lang.Integer\"]\n"
        + "}";
    final TextIndexConfig configWithArgs = JsonUtils.stringToObject(confStrWithNonEmptyArrays, TextIndexConfig.class);

    assertEquals(configWithArgs.getLuceneAnalyzerClassArgs(), Lists.newArrayList("arg1", "arg2"),
        "Unexpected luceneAnalyzerClassArgs");
    assertEquals(configWithArgs.getLuceneAnalyzerClassArgTypes(),
        Lists.newArrayList("java.lang.String", "java.lang.Integer"),
        "Unexpected luceneAnalyzerClassArgTypes");
  }

  @Test
  public void testRoundTripSerializationWithStringValues()
      throws JsonProcessingException {
    // Test backward compatibility - original CSV string format should still work
    final String confStrWithStrings = "{\n"
        + "        \"luceneAnalyzerClassArgs\": \"arg1,arg2\",\n"
        + "        \"luceneAnalyzerClassArgTypes\": \"java.lang.String,java.lang.Integer\"\n"
        + "}";
    final TextIndexConfig config = JsonUtils.stringToObject(confStrWithStrings, TextIndexConfig.class);

    assertEquals(config.getLuceneAnalyzerClassArgs(), Lists.newArrayList("arg1", "arg2"),
        "Unexpected luceneAnalyzerClassArgs");
    assertEquals(config.getLuceneAnalyzerClassArgTypes(),
        Lists.newArrayList("java.lang.String", "java.lang.Integer"),
        "Unexpected luceneAnalyzerClassArgTypes");
  }

  @Test
  public void testFullRoundTrip()
      throws JsonProcessingException {
    // Create config, serialize to JSON, deserialize back - should work
    final String originalConfStr = "{\n"
        + "        \"luceneAnalyzerClassArgs\": \"arg1,arg2\",\n"
        + "        \"luceneAnalyzerClassArgTypes\": \"java.lang.String,java.lang.Integer\"\n"
        + "}";
    final TextIndexConfig originalConfig = JsonUtils.stringToObject(originalConfStr, TextIndexConfig.class);

    // Serialize to JSON (getters return List<String>, so it becomes JSON arrays)
    final String serialized = JsonUtils.objectToString(originalConfig);

    // Deserialize back - this was the failing case before the fix
    final TextIndexConfig deserializedConfig = JsonUtils.stringToObject(serialized, TextIndexConfig.class);

    assertEquals(deserializedConfig.getLuceneAnalyzerClassArgs(), originalConfig.getLuceneAnalyzerClassArgs(),
        "Round-trip failed for luceneAnalyzerClassArgs");
    assertEquals(deserializedConfig.getLuceneAnalyzerClassArgTypes(), originalConfig.getLuceneAnalyzerClassArgTypes(),
        "Round-trip failed for luceneAnalyzerClassArgTypes");
  }

  @Test
  public void testEmptyTextConfigRoundTrip()
      throws JsonProcessingException {
    // When an empty text config is created from "{}", it gets default values.
    // When serialized, luceneAnalyzerClassArgs becomes [] (empty array).
    // When deserialized, this was failing with:
    // "Cannot deserialize value of type `java.lang.String` from Array value"
    final String emptyConfig = "{}";
    final TextIndexConfig originalConfig = JsonUtils.stringToObject(emptyConfig, TextIndexConfig.class);

    // Verify defaults are applied
    assertFalse(originalConfig.isDisabled());
    assertEquals(originalConfig.getLuceneAnalyzerClass(),
        "org.apache.lucene.analysis.standard.StandardAnalyzer");
    assertTrue(originalConfig.getLuceneAnalyzerClassArgs().isEmpty());
    assertTrue(originalConfig.getLuceneAnalyzerClassArgTypes().isEmpty());

    // Serialize to JSON - this produces the problematic format with empty arrays
    final String serialized = JsonUtils.objectToString(originalConfig);

    // Verify serialized JSON contains the array format that was causing the bug
    assertTrue(serialized.contains("\"luceneAnalyzerClassArgs\":[]"),
        "Serialized JSON should contain luceneAnalyzerClassArgs as empty array");
    assertTrue(serialized.contains("\"luceneAnalyzerClassArgTypes\":[]"),
        "Serialized JSON should contain luceneAnalyzerClassArgTypes as empty array");

    // Deserialize back
    final TextIndexConfig deserializedConfig = JsonUtils.stringToObject(serialized, TextIndexConfig.class);

    // Verify round-trip preserves the config
    assertEquals(deserializedConfig.isDisabled(), originalConfig.isDisabled());
    assertEquals(deserializedConfig.getLuceneAnalyzerClass(), originalConfig.getLuceneAnalyzerClass());
    assertEquals(deserializedConfig.getLuceneAnalyzerClassArgs(), originalConfig.getLuceneAnalyzerClassArgs());
    assertEquals(deserializedConfig.getLuceneAnalyzerClassArgTypes(), originalConfig.getLuceneAnalyzerClassArgTypes());
    assertEquals(deserializedConfig.isLuceneUseCompoundFile(), originalConfig.isLuceneUseCompoundFile());
    assertEquals(deserializedConfig.getLuceneMaxBufferSizeMB(), originalConfig.getLuceneMaxBufferSizeMB());
  }
}
