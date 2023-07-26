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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.text.TextIndexPlugin;
import org.apache.pinot.segment.local.segment.index.text.TextIndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


public class TextIndexTest {
  public static class ConfTest extends AbstractSerdeIndexContract {

    @Test
    public void oldToNewConfConversion()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"TEXT\"]\n"
          + " }");
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimStr")).collect(Collectors.toList()).get(0);
      JsonNode indexConfig = fieldConfig.getIndexes().get(TextIndexType.INDEX_DISPLAY_NAME);
      assertNotNull(indexConfig);
      assertFalse(indexConfig.get("disabled").asBoolean());
    }
  }

  @Test
  public void testStandardIndex() {
    assertEquals(StandardIndexes.text(), new TextIndexPlugin().getIndexType(),
        "Standard index should be equal to the instance returned by the plugin");
  }
}
