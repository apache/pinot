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
package org.apache.pinot.integration.tests;

import java.nio.charset.StandardCharsets;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceResult;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TenantRebalanceIntegrationTest extends BaseHybridClusterIntegrationTest {

  private String getRebalanceUrl() {
    return StringUtil.join("/", getControllerRequestURLBuilder().getBaseUrl(), "tenants", getServerTenant(),
        "rebalance");
  }

  @Test
  public void testParallelWhitelistBlacklistCompatibility()
      throws Exception {
    // Prepare a TenantRebalanceConfig with parallelWhitelist and parallelBlacklist (old usage)
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    // Add a table to parallelWhitelist and another to parallelBlacklist
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    config.getParallelWhitelist().add(table1);
    config.getParallelBlacklist().add(table2);

    // Call the rebalance endpoint with the config
    TenantRebalanceResult result = JsonUtils.stringToObject(
        sendPostRequest(getRebalanceUrl(), JsonUtils.objectToString(config)),
        TenantRebalanceResult.class
    );

    // Assert that the result contains the expected tables and statuses
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertTrue(result.getRebalanceTableResults().containsKey(table2));
  }

  @Test
  public void testIncludeTablesQueryParamOverridesBody()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    // Set both tables in the body
    config.getIncludeTables().add(table1);
    config.getIncludeTables().add(table2);

    // Pass only table1 in the query param, should override the body
    String urlWithQuery = getRebalanceUrl() + "?includeTables=" + table1;
    TenantRebalanceResult result = JsonUtils.stringToObject(
        sendPostRequest(urlWithQuery, JsonUtils.objectToString(config)),
        TenantRebalanceResult.class
    );
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertFalse(result.getRebalanceTableResults().containsKey(table2), "Query param should override body");
  }

  @Test
  public void testIncludeTablesBodyUsedIfNoQueryParam()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    // Set only table2 in the body
    config.getIncludeTables().add(table2);

    // No query param, should use body
    TenantRebalanceResult result = JsonUtils.stringToObject(
        sendPostRequest(getRebalanceUrl(), JsonUtils.objectToString(config)),
        TenantRebalanceResult.class
    );
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table2));
    assertFalse(result.getRebalanceTableResults().containsKey(table1), "Should only use body if no query param");
  }

  @Test
  public void testIncludeExcludeTablesQueryParamMultipleTables()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";

    // Pass includeTables and excludeTables as comma separated list with spaces and quotes
    String urlWithQuery =
        getRebalanceUrl() + "?includeTables=" + java.net.URLEncoder.encode(" " + table1 + " , \"" + table2 + "\" , ",
            StandardCharsets.UTF_8) + "&excludeTables=" + java.net.URLEncoder.encode(" " + table2 + ", ",
            StandardCharsets.UTF_8);
    TenantRebalanceResult result = JsonUtils.stringToObject(
        sendPostRequest(urlWithQuery, JsonUtils.objectToString(config)),
        TenantRebalanceResult.class
    );
    assertNotNull(result);
    // Should only include table1, table2 are excluded
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertFalse(result.getRebalanceTableResults().containsKey(table2), "excludeTables should remove table2");
  }
}
