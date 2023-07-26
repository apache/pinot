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
import org.testng.Assert;
import org.testng.annotations.Test;


public class BloomFilterConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    BloomFilterConfig config = JsonUtils.stringToObject(confStr, BloomFilterConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    BloomFilterConfig config = JsonUtils.stringToObject(confStr, BloomFilterConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    BloomFilterConfig config = JsonUtils.stringToObject(confStr, BloomFilterConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    BloomFilterConfig config = JsonUtils.stringToObject(confStr, BloomFilterConfig.class);

    Assert.assertFalse(config.isEnabled(), "Config should be disabled");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "  \"fpp\": 0.5,\n"
        + "  \"maxSizeInBytes\": 1024,\n"
        + "  \"loadOnHeap\": true"
        + "}";
    BloomFilterConfig config = JsonUtils.stringToObject(confStr, BloomFilterConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
    Assert.assertEquals(config.getFpp(), 0.5d, "FPP is wrong");
    Assert.assertEquals(config.getMaxSizeInBytes(), 1024, "maxSizeInBytes is wrong");
    Assert.assertTrue(config.isLoadOnHeap(), "loadOnHeap is wrong");
  }
}
