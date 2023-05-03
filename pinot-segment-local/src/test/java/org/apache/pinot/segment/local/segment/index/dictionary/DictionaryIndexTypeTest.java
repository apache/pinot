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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

    protected <T extends Throwable> void assertThrows(Class<T> throwableClass, @Nullable Consumer<T> postCheck) {
      try {
        getActualConfig("dimInt", StandardIndexes.dictionary());
        Assert.fail("Throwable " + throwableClass + " was expected to be thrown but it wasn't");
      } catch (AssertionError error) {
        throw error;
      } catch (Throwable t) {
        if (throwableClass.isInstance(t)) {
          if (postCheck != null) {
            postCheck.accept((T) t);
          }
        } else {
          throw new AssertionError(
              "Unexpected exception found. Expected " + throwableClass + " but " + t.getClass() + " was thrown", t);
        }
      }
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
    public void testSortedColumn()
        throws IOException {
      _tableConfig.getIndexingConfig().setSortedColumn(Lists.newArrayList("dimInt"));
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void testSortedAndNoDictionary()
        throws IOException {
      _tableConfig.getIndexingConfig().setSortedColumn(Lists.newArrayList("dimInt"));
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      assertEquals(DictionaryIndexConfig.DEFAULT);
    }

    @Test
    public void testSortedAndNoDictionaryButConfigured()
        throws IOException {
      _tableConfig.getIndexingConfig().setSortedColumn(Lists.newArrayList("dimInt"));
      _tableConfig.getIndexingConfig().setVarLengthDictionaryColumns(Lists.newArrayList("dimInt"));
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      assertEquals(new DictionaryIndexConfig(false, true));
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
    public void newNull()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": null\n"
          + "    }\n"
          + " }");
      assertEquals(DictionaryIndexConfig.DEFAULT);
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
    public void newSortedAndDisabled()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"disabled\": true"
          + "      }\n"
          + "    }\n"
          + " }");
      _tableConfig.getIndexingConfig().setSortedColumn(Lists.newArrayList("dimInt"));

      assertThrows(IllegalStateException.class,
          (ex) -> Assert.assertEquals(ex.getMessage(), "Cannot disable dictionary in sorted column dimInt"));
    }

    @Test
    public void newSortedAndEnabled()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {\n"
          + "      \"dictionary\": {\n"
          + "        \"disabled\": false"
          + "      }\n"
          + "    }\n"
          + " }");
      _tableConfig.getIndexingConfig().setSortedColumn(Lists.newArrayList("dimInt"));
      assertEquals(DictionaryIndexConfig.DEFAULT);
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
