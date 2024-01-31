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
package org.apache.pinot.segment.local.segment.index.inverted;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InvertedIndexTypeTest {


  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(IndexConfig expected) {
      checkConfigsMatch(StandardIndexes.inverted(), "dimInt", expected);
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(IndexConfig.DISABLED);
    }

    @Test
    public void oldConfNotFound()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[]")
      );

      assertEquals(IndexConfig.DISABLED);
    }

    @Test
    public void oldConfEnabled()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[\"dimInt\"]"));

      assertEquals(IndexConfig.ENABLED);
    }

    @Test
    public void newConfDisableByDefault()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "    }\n"
          + "}");

      assertEquals(IndexConfig.DISABLED);
    }

    @Test
    public void newConfDisabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"disabled\": true\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );

      assertEquals(IndexConfig.DISABLED);
    }

    @Test
    public void newConfEnabled()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"indexes\" : {\n"
          + "      \"inverted\": {\n"
          + "         \"disabled\": false\n"
          + "      }\n"
          + "    }\n"
          + "}"
      );
      assertEquals(IndexConfig.ENABLED);
    }

    @Test
    public void oldToNewConfConversion()
        throws IOException {
      _tableConfig.getIndexingConfig()
          .setInvertedIndexColumns(parseStringList("[\"dimInt\"]"));
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(InvertedIndexType.INDEX_DISPLAY_NAME));
      assertNull(_tableConfig.getIndexingConfig().getInvertedIndexColumns());
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.inverted(), StandardIndexes.inverted(), "Inverted index should use the same as "
        + "the InvertedIndexType static instance");
  }
}
