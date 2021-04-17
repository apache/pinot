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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class QuotaConfigTest {

  @Test
  public void testStorageQuota() throws IOException {
    {
      String quotaConfigStr = "{\"storage\" : \"100gb\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getStorage(), "100G");
      assertEquals(quotaConfig.getStorageInBytes(), 100L * 1024 * 1024 * 1024);
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertNull(quotaConfig.getStorage());
      assertEquals(quotaConfig.getStorageInBytes(), -1L);
    }
  }

  @Test
  public void testInvalidStorageQuota() {
    try {
      String quotaConfigStr = "{\"storage\" : \"124GB3GB\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"storage\":\"-1M\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testQPSQuota() throws IOException {
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"100\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getMaxQueriesPerSecond(), "100.0");
      assertEquals(quotaConfig.getMaxQPS(), 100.0);
    }
    {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"0.5\"}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertEquals(quotaConfig.getMaxQueriesPerSecond(), "0.5");
      assertEquals(quotaConfig.getMaxQPS(), 0.5);
    }
    {
      String quotaConfigStr = "{}";
      QuotaConfig quotaConfig = JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      assertNull(quotaConfig.getMaxQueriesPerSecond());
      assertEquals(quotaConfig.getMaxQPS(), -1.0);
    }
  }

  @Test
  public void testInvalidQPSQuota() {
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"InvalidQpsQuota\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"-1.0\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
    try {
      String quotaConfigStr = "{\"maxQueriesPerSecond\" : \"1.0Test\"}";
      JsonUtils.stringToObject(quotaConfigStr, QuotaConfig.class);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testSerDe() throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig("100G", "100.0");
    JsonNode quotaConfigJson = quotaConfig.toJsonNode();
    assertEquals(quotaConfigJson.get("storage").asText(), "100G");
    assertEquals(quotaConfigJson.get("maxQueriesPerSecond").asText(), "100.0");
    assertNull(quotaConfigJson.get("storageInBytes"));
    assertNull(quotaConfigJson.get("maxQPS"));

    assertEquals(JsonUtils.jsonNodeToObject(quotaConfigJson, QuotaConfig.class), quotaConfig);
    assertEquals(JsonUtils.stringToObject(quotaConfig.toJsonString(), QuotaConfig.class), quotaConfig);
  }
}
