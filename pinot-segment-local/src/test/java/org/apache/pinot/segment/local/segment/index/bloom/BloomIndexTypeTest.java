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
package org.apache.pinot.segment.local.segment.index.bloom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class BloomIndexTypeTest {

  @DataProvider(name = "allConfigs")
  public static Object[][] allConfigs() {
    return new String[][] {
        new String[] {"{}"},
        new String[] {"{\n"
            + "  \"fpp\": 0.5\n"
            + "}"},
        new String[] {"{\n"
            + "  \"fpp\": 0.5,\n"
            + "  \"maxSizeInBytes\": 1024,\n"
            + "  \"loadOnHeap\": true"
            + "}"}
    };
  }

  public class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexDeclaration<BloomFilterConfig> expected) {
      Assert.assertEquals(getActualConfig("dimInt", BloomIndexType.INSTANCE), expected);
    }

    @Test
    public void oldIndexingConfigNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(IndexDeclaration.notDeclared(BloomIndexType.INSTANCE));
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(null);
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(null);

      assertEquals(IndexDeclaration.notDeclared(BloomIndexType.INSTANCE));
    }

    @Test
    public void oldMapConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(
          JsonUtils.stringToObject("{}", new TypeReference<Map<String, BloomFilterConfig>>() {
          })
      );

      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test(dataProvider = "allConfigs", dataProviderClass = BloomIndexTypeTest.class)
    public void oldMapConfFound(String confStr)
        throws IOException {
      BloomFilterConfig config =
          JsonUtils.stringToObject(confStr, BloomFilterConfig.class);
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(Collections.singletonMap("dimInt", config));

      assertEquals(IndexDeclaration.declared(config));
    }

    @Test
    public void oldListConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[]", _stringListTypeRef)
      );

      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test
    public void oldListConfFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );

      assertEquals(IndexDeclaration.declared(BloomFilterConfig.createDefault()));
    }

    @Test
    public void oldConfPrioritizesMap()
        throws IOException {
      BloomFilterConfig config = JsonUtils.stringToObject(
          "{\n"
              + "  \"fpp\": 0.5,\n"
              + "  \"maxSizeInBytes\": 1024,\n"
              + "  \"loadOnHeap\": true"
              + "}", BloomFilterConfig.class
      );
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(Collections.singletonMap("dimInt", config));
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );

      assertEquals(IndexDeclaration.declared(config));
    }

    @Test
    public void newConfDisabled()
        throws IOException {
      addFieldIndexConfig("{"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"bloom\": null\n"
          + "    }"
          + "}");

      assertEquals(IndexDeclaration.declaredDisabled());
    }

    @Test(dataProvider = "allConfigs", dataProviderClass = BloomIndexTypeTest.class)
    public void newConfFound(String confStr)
        throws IOException {
      BloomFilterConfig config =
          JsonUtils.stringToObject(confStr, BloomFilterConfig.class);
      addFieldIndexConfig("{"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"bloom\": " + confStr + "\n"
          + "    }"
          + "}");

      assertEquals(IndexDeclaration.declared(config));
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.bloomFilter(), BloomIndexType.INSTANCE, "Standard index should use the same as "
        + "the BloomIndexType static instance");
  }
}
