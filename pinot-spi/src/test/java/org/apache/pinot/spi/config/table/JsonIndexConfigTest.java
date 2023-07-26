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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.*;


public class JsonIndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    JsonIndexConfig config = JsonUtils.stringToObject(confStr, JsonIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isExcludeArray(), "Unexpected excludeArray");
    assertFalse(config.isDisableCrossArrayUnnest(), "Unexpected disableCrossArrayUnnest");
    assertNull(config.getIncludePaths(), "Unexpected includePaths");
    assertNull(config.getExcludePaths(), "Unexpected excludePaths");
    assertNull(config.getExcludeFields(), "Unexpected excludeFields");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    JsonIndexConfig config = JsonUtils.stringToObject(confStr, JsonIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isExcludeArray(), "Unexpected excludeArray");
    assertFalse(config.isDisableCrossArrayUnnest(), "Unexpected disableCrossArrayUnnest");
    assertNull(config.getIncludePaths(), "Unexpected includePaths");
    assertNull(config.getExcludePaths(), "Unexpected excludePaths");
    assertNull(config.getExcludeFields(), "Unexpected excludeFields");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    JsonIndexConfig config = JsonUtils.stringToObject(confStr, JsonIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isExcludeArray(), "Unexpected excludeArray");
    assertFalse(config.isDisableCrossArrayUnnest(), "Unexpected disableCrossArrayUnnest");
    assertNull(config.getIncludePaths(), "Unexpected includePaths");
    assertNull(config.getExcludePaths(), "Unexpected excludePaths");
    assertNull(config.getExcludeFields(), "Unexpected excludeFields");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    JsonIndexConfig config = JsonUtils.stringToObject(confStr, JsonIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isExcludeArray(), "Unexpected excludeArray");
    assertFalse(config.isDisableCrossArrayUnnest(), "Unexpected disableCrossArrayUnnest");
    assertNull(config.getIncludePaths(), "Unexpected includePaths");
    assertNull(config.getExcludePaths(), "Unexpected excludePaths");
    assertNull(config.getExcludeFields(), "Unexpected excludeFields");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"maxLevels\": 2,\n"
        + "        \"excludeArray\": true,\n"
        + "        \"disableCrossArrayUnnest\": true,\n"
        + "        \"includePaths\": [\"a\"],\n"
        + "        \"excludePaths\": [\"b\"],\n"
        + "        \"excludeFields\": [\"c\"]\n"
        + "}";
    JsonIndexConfig config = JsonUtils.stringToObject(confStr, JsonIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertTrue(config.isExcludeArray(), "Unexpected excludeArray");
    assertTrue(config.isDisableCrossArrayUnnest(), "Unexpected disableCrossArrayUnnest");
    assertEquals(config.getIncludePaths(), Lists.newArrayList("a"), "Unexpected includePaths");
    assertEquals(config.getExcludePaths(), Lists.newArrayList("b"), "Unexpected excludePaths");
    assertEquals(config.getExcludeFields(), Lists.newArrayList("c"), "Unexpected excludeFields");
  }
}
