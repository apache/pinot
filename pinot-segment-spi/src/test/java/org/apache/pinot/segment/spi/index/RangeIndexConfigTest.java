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
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class RangeIndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    RangeIndexConfig config = JsonUtils.stringToObject(confStr, RangeIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVersion(), RangeIndexConfig.DEFAULT.getVersion(), "Unexpected version");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    RangeIndexConfig config = JsonUtils.stringToObject(confStr, RangeIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVersion(), RangeIndexConfig.DEFAULT.getVersion(), "Unexpected version");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    RangeIndexConfig config = JsonUtils.stringToObject(confStr, RangeIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVersion(), RangeIndexConfig.DEFAULT.getVersion(), "Unexpected version");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    RangeIndexConfig config = JsonUtils.stringToObject(confStr, RangeIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVersion(), RangeIndexConfig.DEFAULT.getVersion(), "Unexpected version");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"version\": 42\n"
        + "}";
    RangeIndexConfig config = JsonUtils.stringToObject(confStr, RangeIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getVersion(), 42, "Unexpected version");
  }
}
