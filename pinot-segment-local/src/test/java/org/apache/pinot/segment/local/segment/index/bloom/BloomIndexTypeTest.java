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
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


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

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(BloomFilterConfig expected) {
      checkConfigsMatch(StandardIndexes.bloomFilter(), "dimInt", expected);
    }

    @Test
    public void oldIndexingConfigNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(BloomFilterConfig.DISABLED);
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(null);
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(null);

      assertEquals(BloomFilterConfig.DISABLED);
    }

    @Test
    public void oldMapConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(
          JsonUtils.stringToObject("{}", new TypeReference<Map<String, BloomFilterConfig>>() {
          })
      );

      assertEquals(BloomFilterConfig.DISABLED);
    }

    @Test(dataProvider = "allConfigs", dataProviderClass = BloomIndexTypeTest.class)
    public void oldMapConfFound(String confStr)
        throws IOException {
      BloomFilterConfig config =
          JsonUtils.stringToObject(confStr, BloomFilterConfig.class);
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(Collections.singletonMap("dimInt", config));

      assertEquals(config);
    }

    @Test
    public void oldListConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[]", _stringListTypeRef)
      );

      assertEquals(BloomFilterConfig.DISABLED);
    }

    @Test
    public void oldListConfFound()
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );

      assertEquals(BloomFilterConfig.DEFAULT);
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

      assertEquals(config);
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

      assertEquals(BloomFilterConfig.DISABLED);
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

      assertEquals(config);
    }

    @Test(dataProvider = "allConfigs", dataProviderClass = BloomIndexTypeTest.class)
    public void oldToNewConfConversion(String confStr)
        throws IOException {
      _tableConfig.getIndexingConfig().setBloomFilterColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef)
      );
      BloomFilterConfig config =
          JsonUtils.stringToObject(confStr, BloomFilterConfig.class);
      _tableConfig.getIndexingConfig().setBloomFilterConfigs(Collections.singletonMap("dimInt", config));
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(BloomIndexType.INDEX_DISPLAY_NAME));
      assertNull(_tableConfig.getIndexingConfig().getBloomFilterColumns());
      assertNull(_tableConfig.getIndexingConfig().getBloomFilterConfigs());
    }
  }

  @Test
  public void testStandardIndex() {
    assertEquals(StandardIndexes.bloomFilter(), new BloomIndexPlugin().getIndexType(),
        "Standard index should be equal to the instance returned by the plugin");
  }
}
