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
package org.apache.pinot.segment.local.segment.index.dictionary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DictionaryIndexTypeTest {

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(DictionaryIndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimInt", StandardIndexes.dictionary()), expected);
    }

    @Test
    public void oldIndexingConfigNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void defaultCase()
        throws JsonProcessingException {
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void withNullFieldConfig() {
      _tableConfig.setFieldConfigList(null);
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void withEmptyFieldConfig()
        throws IOException {
      cleanFieldConfig();
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void noDictionaryCol()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      assertEquals(DictionaryIndexConfig.DISABLED);
    }

    @Test
    public void oldRawEncodingType()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryConfig(
          JsonUtils.stringToObject("{\"dimInt\": \"RAW\"}",
              new TypeReference<Map<String, String>>() {
              })
      );
      assertEquals(DictionaryIndexConfig.DISABLED);
    }

    @Test
    public void oldWithRawEncodingFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": \"RAW\"\n"
          + "}");
      assertEquals(DictionaryIndexConfig.DISABLED);
    }

    @Test
    public void oldWithDictionaryEncodingFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": \"DICTIONARY\"\n"
          + "}");
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void oldWithDictionaryEncodingUndeclaredFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\"\n"
          + "}");
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void oldWithDictionaryEncodingNullFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": null\n"
          + "}");
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void oldWithOnHeap()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setOnHeapDictionaryColumns(JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      assertEquals(new DictionaryIndexConfig(true, null));
    }

    @Test
    public void oldWithVarLength()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setVarLengthDictionaryColumns(JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      assertEquals(new DictionaryIndexConfig(false, true));
    }

    @Test
    public void newUndefined()
        throws IOException {
      _tableConfig.setFieldConfigList(JsonUtils.stringToObject("[]", _fieldConfigListTypeRef));
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void newDisabled()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": null\n"
          + "    }\n"
          + " }");
      assertEquals(DictionaryIndexConfig.DISABLED);
    }

    @Test
    public void newOnHeapVarLength()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"onHeap\": true,\n"
          + "        \"useVarLengthDictionary\": true\n"
          + "      }"
          + "    }\n"
          + " }");
      assertEquals(new DictionaryIndexConfig(true, true));
    }

    @Test
    public void newOnHeap()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"onHeap\": true\n"
          + "      }"
          + "    }\n"
          + " }");
      assertEquals(new DictionaryIndexConfig(true, false));
    }

    @Test
    public void newDefault()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "      }"
          + "    }\n"
          + " }");
      assertEquals(new DictionaryIndexConfig(false, false));
    }

    @Test
    public void newVarLength()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"useVarLengthDictionary\": true\n"
          + "      }"
          + "    }\n"
          + " }");
      assertEquals(new DictionaryIndexConfig(false, true));
    }

    @Test
    public void oldToNewConfConversionWithOnHeap()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setOnHeapDictionaryColumns(JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      convertToUpdatedFormat();
      FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      DictionaryIndexConfig config = JsonUtils.jsonNodeToObject(
          fieldConfig.getIndexes().get(StandardIndexes.dictionary().getPrettyName()),
          DictionaryIndexConfig.class);
      assertNotNull(config);
      assertTrue(config.isOnHeap());
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithVarLength()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setVarLengthDictionaryColumns(JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      convertToUpdatedFormat();
      FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      DictionaryIndexConfig config = JsonUtils.jsonNodeToObject(
          fieldConfig.getIndexes().get(StandardIndexes.dictionary().getPrettyName()),
          DictionaryIndexConfig.class);
      assertNotNull(config);
      assertTrue(config.getUseVarLengthDictionary());
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithNoDictionaryColumns()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      convertToUpdatedFormat();
      FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      Assert.assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);
      postConversionAsserts();
    }

    @Test
    public void testIssue12254()
        throws IOException {
      withIndexingConfig("{\n"
          + "    \"autoGeneratedInvertedIndex\": false,\n"
          + "    \"enableDynamicStarTreeCreation\": false,\n"
          + "    \"columnMajorSegmentBuilderEnabled\": false,\n"
          + "    \"optimizeDictionaryForMetrics\": false,\n"
          + "    \"noDictionarySizeRatioThreshold\": 0.85,\n"
          + "    \"rangeIndexVersion\": 2,\n"
          + "    \"sortedColumn\": [\n"
          + "      \"dimInt\"\n"
          + "    ],\n"
          + "    \"loadMode\": \"HEAP\",\n"
          + "    \"enableDefaultStarTree\": false,\n"
          + "    \"aggregateMetrics\": false,\n"
          + "    \"nullHandlingEnabled\": false,\n"
          + "    \"optimizeDictionary\": false,\n"
          + "    \"createInvertedIndexDuringSegmentGeneration\": false\n"
          + "}"
      );
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": \"DICTIONARY\",\n"
          + "    \"indexTypes\": [],\n"
          + "    \"indexes\": {\n"
          + "      \"dictionary\": {\n"
          + "        \"disabled\": false,\n"
          + "        \"onHeap\": true,\n"
          + "        \"useVarLengthDictionary\": true\n"
          + "      }\n"
          + "    },\n"
          + "  \"tierOverwrites\": null\n"
          + "}"
      );
      assertEquals(new DictionaryIndexConfig(true, true));
    }

    private FieldConfig getFieldConfigByColumn(String column) {
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      return _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals(column))
          .collect(Collectors.toList()).get(0);
    }

    private void postConversionAsserts() {
      assertNull(_tableConfig.getIndexingConfig().getNoDictionaryColumns());
      assertNull(_tableConfig.getIndexingConfig().getOnHeapDictionaryColumns());
      assertNull(_tableConfig.getIndexingConfig().getVarLengthDictionaryColumns());
      assertNull(_tableConfig.getIndexingConfig().getNoDictionaryConfig());
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.dictionary(), StandardIndexes.dictionary(), "Standard index should use the same as "
        + "the DictionaryIndexType static instance");
  }
}
