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
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DictionaryIndexTypeTest {

  public class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexDeclaration<DictionaryIndexConfig> expected) {
      Assert.assertEquals(getActualConfig("dimInt", DictionaryIndexType.INSTANCE), expected);
    }

    @Test
    public void oldIndexingConfigNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(IndexDeclaration.notDeclared(DictionaryIndexType.INSTANCE));
    }

    @Test
    public void defaultCase()
        throws JsonProcessingException {
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void withNullFieldConfig() {
      _tableConfig.setFieldConfigList(null);
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void withEmptyFieldConfig()
        throws IOException {
      cleanFieldConfig();
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void noDictionaryCol()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      assertEquals(IndexDeclaration.declaredDisabled());
    }

    public void oldRawEncondingType()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryConfig(
          JsonUtils.stringToObject("{\"dimInt\": \"RAW\"}",
              new TypeReference<Map<String, String>>() {
              })
      );
      assertEquals(IndexDeclaration.notDeclaredDisabled());
    }

    @Test
    public void oldWithRawEncodingFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": \"RAW\"\n"
          + "}");
      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test
    public void oldWithDictionaryEncodingFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": \"DICTIONARY\"\n"
          + "}");
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void oldWithDictionaryEncodingUndeclaredFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\"\n"
          + "}");
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void oldWithDictionaryEncodingNullFieldConfig()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"encodingType\": null\n"
          + "}");
      assertEquals(IndexDeclaration.notDeclared(new DictionaryIndexConfig(false, false)));
    }

    @Test
    public void oldWithOnHeap()
        throws IOException {
      _tableConfig.getIndexingConfig().setOnHeapDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(true, null)));
    }

    @Test
    public void oldWithVarLength()
        throws IOException {
      _tableConfig.getIndexingConfig().setVarLengthDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(false, true)));
    }

    @Test
    public void newUndefined()
        throws IOException {
      _tableConfig.setFieldConfigList(JsonUtils.stringToObject("[]", _fieldConfigListTypeRef));
      assertEquals(IndexDeclaration.notDeclared(DictionaryIndexType.INSTANCE));
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
      assertEquals(IndexDeclaration.declaredDisabled());
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
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(true, true)));
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
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(true, false)));
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
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(false, false)));
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
      assertEquals(IndexDeclaration.declared(new DictionaryIndexConfig(false, true)));
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.dictionary(), DictionaryIndexType.INSTANCE, "Standard index should use the same as "
        + "the DictionaryIndexType static instance");
  }
}
