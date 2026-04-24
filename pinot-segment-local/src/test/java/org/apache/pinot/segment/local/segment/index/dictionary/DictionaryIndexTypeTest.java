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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.Intern;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DictionaryIndexTypeTest {

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(DictionaryIndexConfig expected) {
      DictionaryIndexConfig actualConfig = getActualConfig("dimInt", StandardIndexes.dictionary());
      Assert.assertEquals(actualConfig, expected);
    }

    @Test
    public void defaultCase() {
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void withNullFieldConfig() {
      _tableConfig.setFieldConfigList(null);
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void withEmptyFieldConfig() {
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
      _tableConfig.getIndexingConfig()
          .setNoDictionaryConfig(JsonUtils.stringToObject("{\"dimInt\": \"RAW\"}", new TypeReference<>() {
          }));
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
      assertEquals(new DictionaryIndexConfig(true, null, null));
    }

    @Test
    public void oldWithVarLength()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setVarLengthDictionaryColumns(JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      assertEquals(new DictionaryIndexConfig(false, true, null));
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
      assertEquals(new DictionaryIndexConfig(true, true, null));
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
      assertEquals(new DictionaryIndexConfig(true, false, null));
    }

    @Test
    public void newOnHeapWithInternConfig()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"onHeap\": true,\n"
          + "        \"intern\": {\n"
          + "          \"capacity\":1000\n"
          + "        }"
          + "      }"
          + "    }\n"
          + " }");
      assertEquals(new DictionaryIndexConfig(true, false, new Intern(1000)));
    }

    @Test
    public void newDisabledOnHeapWithInternConfig()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"onHeap\": false,\n"
          + "        \"intern\": {\n"
          + "          \"capacity\":1000\n"
          + "        }"
          + "      }"
          + "    }\n"
          + " }");
      assertThrows(UncheckedIOException.class, () -> getActualConfig("dimInt", StandardIndexes.dictionary()));
    }

    @Test
    public void newOnHeapWithEmptyConfig()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"onHeap\": true,\n"
          + "        \"intern\": {\n"
          + "        }"
          + "      }"
          + "    }\n"
          + " }");
      assertThrows(UncheckedIOException.class, () -> getActualConfig("dimInt", StandardIndexes.dictionary()));
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
      assertEquals(new DictionaryIndexConfig(false, false, null));
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
      assertEquals(new DictionaryIndexConfig(false, true, null));
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
      assertTrue(config.isUseVarLengthDictionary());
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
    public void oldToNewConfConversionWithRawEncodingAndNoDictionaryColumns()
        throws IOException {
      addFieldIndexConfig("{"
          + "\"name\": \"dimInt\","
          + "\"encodingType\": \"RAW\","
          + "\"indexes\": {"
          + "  \"forward\": {"
          + "    \"compressionCodec\": \"ZSTANDARD\""
          + "  }"
          + "}"
          + "}");
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));

      convertToUpdatedFormat();

      final FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      Assert.assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);
      assertNotNull(fieldConfig.getIndexes());
      assertTrue(fieldConfig.getIndexes().isObject());
      assertNotNull(fieldConfig.getIndexes().get("forward"));

      final long dimIntCount = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .count();
      Assert.assertEquals(dimIntCount, 1L);
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithRawEncodingAndNoDictionaryColumnsMultipleCols()
        throws IOException {
      addFieldIndexConfig("{"
          + "\"name\": \"dimInt\","
          + "\"encodingType\": \"RAW\","
          + "\"indexes\": {"
          + "  \"forward\": {\"compressionCodec\": \"ZSTANDARD\"}"
          + "}"
          + "}");
      addFieldIndexConfig("{"
          + "\"name\": \"dimStr\","
          + "\"encodingType\": \"RAW\","
          + "\"indexes\": {"
          + "  \"forward\": {\"compressionCodec\": \"LZ4\"}"
          + "}"
          + "}");
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\", \"dimStr\"]", _stringListTypeRef));

      convertToUpdatedFormat();

      final FieldConfig dimInt = getFieldConfigByColumn("dimInt");
      final FieldConfig dimStr = getFieldConfigByColumn("dimStr");
      Assert.assertEquals(dimInt.getEncodingType(), FieldConfig.EncodingType.RAW);
      Assert.assertEquals(dimStr.getEncodingType(), FieldConfig.EncodingType.RAW);
      assertNotNull(dimInt.getIndexes().get("forward"));
      assertNotNull(dimStr.getIndexes().get("forward"));

      for (final String col : new String[]{"dimInt", "dimStr"}) {
        final long count = _tableConfig.getFieldConfigList().stream()
            .filter(fc -> fc.getName().equals(col))
            .count();
        Assert.assertEquals(count, 1L, "Duplicate FieldConfig for " + col);
      }
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithRawEncodingNoDictionaryColumnsPartialOverlap()
        throws IOException {
      addFieldIndexConfig("{"
          + "\"name\": \"dimInt\","
          + "\"encodingType\": \"RAW\","
          + "\"indexes\": {"
          + "  \"forward\": {\"compressionCodec\": \"ZSTANDARD\"}"
          + "}"
          + "}");
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\", \"dimStr\"]", _stringListTypeRef));

      convertToUpdatedFormat();

      final FieldConfig dimInt = getFieldConfigByColumn("dimInt");
      Assert.assertEquals(dimInt.getEncodingType(), FieldConfig.EncodingType.RAW);
      assertNotNull(dimInt.getIndexes().get("forward"));

      final FieldConfig dimStr = getFieldConfigByColumn("dimStr");
      Assert.assertEquals(dimStr.getEncodingType(), FieldConfig.EncodingType.RAW);

      for (final String col : new String[]{"dimInt", "dimStr"}) {
        final long count = _tableConfig.getFieldConfigList().stream()
            .filter(fc -> fc.getName().equals(col))
            .count();
        Assert.assertEquals(count, 1L, "Duplicate FieldConfig for " + col);
      }
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithRawEncodingNoDictionaryColumnsAndNoIndexes()
        throws IOException {
      addFieldIndexConfig("{"
          + "\"name\": \"dimInt\","
          + "\"encodingType\": \"RAW\""
          + "}");
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));

      convertToUpdatedFormat();

      final FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      Assert.assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);

      final long count = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .count();
      Assert.assertEquals(count, 1L);
      postConversionAsserts();
    }

    @Test
    public void oldToNewConfConversionWithNonRawEncodingAndNoDictionaryColumns()
        throws IOException {
      addFieldIndexConfig("{"
          + "\"name\": \"dimInt\","
          + "\"encodingType\": \"DICTIONARY\""
          + "}");
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));

      convertToUpdatedFormat();

      final FieldConfig fieldConfig = getFieldConfigByColumn("dimInt");
      Assert.assertEquals(fieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);

      final long count = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .count();
      Assert.assertEquals(count, 1L);
      postConversionAsserts();
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

  /**
   * Tests to verify various combinations of inputs to test dictionary override optimization.
   */
  @Test
  public void testDictionaryOverride() {
    MetricFieldSpec metric = new MetricFieldSpec("testCol", FieldSpec.DataType.DOUBLE);
    IndexType index1 = Mockito.mock(IndexType.class);
    Mockito.when(index1.getId()).thenReturn("index1");
    IndexConfig indexConf = new IndexConfig(true);
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder().add(index1, indexConf).build();
    // No need to disable dictionary
    boolean result =
        DictionaryIndexType.ignoreDictionaryOverride(false, true, 2, null, metric, fieldIndexConfigs, 5, 20);
    assertTrue(result);

    // Set a higher noDictionarySizeRatioThreshold
    result = DictionaryIndexType.ignoreDictionaryOverride(false, true, 5, null, metric, fieldIndexConfigs, 5, 20);
    assertFalse(result);

    // optimizeDictionary and optimizeDictionaryForMetrics both turned on
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, null, metric, fieldIndexConfigs, 5, 20);
    assertFalse(result);

    // noDictionarySizeRatioThreshold and noDictionaryCardinalityThreshold are provided
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, 0.10, metric, fieldIndexConfigs, 5, 100);
    assertTrue(result);

    // cardinality is much less than total docs, use dictionary
    metric.setDataType(FieldSpec.DataType.STRING);
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, 0.10, metric, fieldIndexConfigs, 5, 100);
    assertTrue(result);

    // cardinality is large % of total docs, do not use dictionary
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, 0.10, metric, fieldIndexConfigs, 5, 20);
    assertFalse(result);

    // Test Dimension col
    // Don't ignore for Json. We want to disable dictionary for json.
    DimensionFieldSpec dimension = new DimensionFieldSpec("testCol", FieldSpec.DataType.JSON, true);
    result = DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, null, dimension, fieldIndexConfigs, 5, 20);
    assertTrue(result);

    // cardinality is much less than total docs, use dictionary
    dimension.setDataType(FieldSpec.DataType.STRING);
    result = DictionaryIndexType.ignoreDictionaryOverride(true, false, 5, 0.10, dimension, fieldIndexConfigs, 5, 100);
    assertTrue(result);

    // cardinality is large % of total docs, do not use dictionary
    result = DictionaryIndexType.ignoreDictionaryOverride(true, false, 5, 0.10, dimension, fieldIndexConfigs, 5, 20);
    assertFalse(result);

    // Ignore for inverted index
    IndexConfig indexConfig = new IndexConfig(false);
    fieldIndexConfigs = new FieldIndexConfigs.Builder().add(StandardIndexes.inverted(), indexConfig).build();
    assertTrue(DictionaryIndexType.ignoreDictionaryOverride(true, true, 5, null, metric, fieldIndexConfigs, 5, 20));
  }
}
