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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceResult;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TenantRebalanceIntegrationTest extends BaseHybridClusterIntegrationTest {

  private TenantRebalanceResult rebalanceTenant(TenantRebalanceConfig config, @Nullable Map<String, String> queryParams)
      throws Exception {
    String response = getOrCreateAdminClient().getTenantClient()
        .rebalanceTenantWithConfig(config.getTenantName(), JsonUtils.objectToString(config), queryParams);
    return JsonUtils.stringToObject(response, TenantRebalanceResult.class);
  }

  private TenantRebalanceResult rebalanceTenant(TenantRebalanceConfig config)
      throws Exception {
    return rebalanceTenant(config, null);
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
    TenantRebalanceResult result = rebalanceTenant(config);

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
    TenantRebalanceResult result =
        rebalanceTenant(config, Map.of("includeTables", table1));
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
    TenantRebalanceResult result = rebalanceTenant(config);
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
    Map<String, String> queryParams = Map.of("includeTables", " " + table1 + " , \"" + table2 + "\" , ",
        "excludeTables", " " + table2 + ", ");
    TenantRebalanceResult result = rebalanceTenant(config, queryParams);
    assertNotNull(result);
    // Should only include table1, table2 are excluded
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertFalse(result.getRebalanceTableResults().containsKey(table2), "excludeTables should remove table2");
  }
}
