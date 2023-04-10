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
package org.apache.pinot.segment.spi.index.creator;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.*;


public class H3IndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    H3IndexConfig config = JsonUtils.stringToObject(confStr, H3IndexConfig.class);

    assertTrue(config.isEnabled(), "Unexpected enabled");
    assertNull(config.getResolution(), "Unexpected resolution");
  }

  @Test
  public void withEnabledNull()
      throws JsonProcessingException {
    String confStr = "{\"enabled\": null}";
    H3IndexConfig config = JsonUtils.stringToObject(confStr, H3IndexConfig.class);

    assertTrue(config.isEnabled(), "Unexpected enabled");
    assertNull(config.getResolution(), "Unexpected resolution");
  }

  @Test
  public void withEnabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"enabled\": true}";
    H3IndexConfig config = JsonUtils.stringToObject(confStr, H3IndexConfig.class);

    assertTrue(config.isEnabled(), "Unexpected enabled");
    assertNull(config.getResolution(), "Unexpected resolution");
  }

  @Test
  public void withEnabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"enabled\": false}";
    H3IndexConfig config = JsonUtils.stringToObject(confStr, H3IndexConfig.class);

    assertFalse(config.isEnabled(), "Unexpected enabled");
    assertNull(config.getResolution(), "Unexpected resolution");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"resolution\": [13, 5, 6]\n"
        + "}";
    H3IndexConfig config = JsonUtils.stringToObject(confStr, H3IndexConfig.class);

    assertTrue(config.isEnabled(), "Unexpected enabled");
    H3IndexResolution resolution = config.getResolution();
    Assert.assertEquals(resolution.size(), 3);
    Assert.assertEquals(resolution.getLowestResolution(), 5);
    Assert.assertEquals(resolution.getResolutions(), Lists.newArrayList(5, 6, 13));
  }
}
