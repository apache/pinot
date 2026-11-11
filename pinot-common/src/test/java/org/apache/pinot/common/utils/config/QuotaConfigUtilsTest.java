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
package org.apache.pinot.common.utils.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


public class QuotaConfigUtilsTest {
  private static final String TABLE_NAME = "testTable";
  private static final String SERVER_TENANT = "serverTenant";
  private static final String BROKER_TENANT = "brokerTenant";

  private static TableConfig table(@Nullable QuotaConfig quotaConfig) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setServerTenant(SERVER_TENANT).setBrokerTenant(BROKER_TENANT).build();
    tableConfig.setQuotaConfig(quotaConfig);
    return tableConfig;
  }

  @Test
  public void testTenantNames() {
    assertEquals(QuotaConfigUtils.getServerTenantName(table(null)), SERVER_TENANT);
    assertEquals(QuotaConfigUtils.getBrokerTenantName(table(null)), BROKER_TENANT);
    TableConfig noTenantTable = table(null);
    noTenantTable.setTenantConfig(new TenantConfig(null, null, null));
    assertEquals(QuotaConfigUtils.getServerTenantName(noTenantTable), TagNameUtils.DEFAULT_TENANT_NAME);
    assertEquals(QuotaConfigUtils.getBrokerTenantName(noTenantTable), TagNameUtils.DEFAULT_TENANT_NAME);
  }

  @Test
  public void testNoTenantQuotaReturnsTableQuota() {
    QuotaConfig tableQuota = new QuotaConfig("10G", "100");
    assertSame(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(tableQuota), null, null), tableQuota);
    assertNull(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(null), null, null));
  }

  @Test
  public void testTenantDefaultsApplyWhenTableQuotaUnset() {
    QuotaConfig serverTenantQuota = new QuotaConfig("10G", null, QuotaEnforcementMode.SHADOW, null);
    QuotaConfig brokerTenantQuota = new QuotaConfig(null, "50", null, QuotaEnforcementMode.SHADOW);
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(null), serverTenantQuota, brokerTenantQuota),
        new QuotaConfig("10G", "50", QuotaEnforcementMode.SHADOW, QuotaEnforcementMode.SHADOW));
  }

  @Test
  public void testEachQuotaIsOnlyInheritedFromTheTenantRoleThatEnforcesIt() {
    // Query fields of the server tenant and storage fields of the broker tenant are not inherited
    QuotaConfig serverTenantQuota = new QuotaConfig("10G", "50", null, QuotaEnforcementMode.SHADOW);
    QuotaConfig brokerTenantQuota = new QuotaConfig("20G", "100", QuotaEnforcementMode.SHADOW, null);
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(null), serverTenantQuota, brokerTenantQuota),
        new QuotaConfig("10G", "100"));
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(null), serverTenantQuota, null),
        new QuotaConfig("10G", null));
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(null), null, brokerTenantQuota),
        new QuotaConfig(null, "100"));
  }

  @Test
  public void testTableFieldsOverrideTenantDefaultsPerField() {
    QuotaConfig serverTenantQuota = new QuotaConfig("10G", null, QuotaEnforcementMode.SHADOW, null);
    QuotaConfig brokerTenantQuota = new QuotaConfig(null, "50", null, QuotaEnforcementMode.SHADOW);

    // Table sets the limits only: the limits are overridden, the enforcement modes are inherited
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(table(new QuotaConfig("20G", "100")),
            serverTenantQuota, brokerTenantQuota),
        new QuotaConfig("20G", "100", QuotaEnforcementMode.SHADOW, QuotaEnforcementMode.SHADOW));

    // Table sets the enforcement modes only: the limits are inherited, so a table can opt out with DISABLED
    assertEquals(QuotaConfigUtils.mergeWithTenantQuotaConfigs(
            table(new QuotaConfig(null, null, QuotaEnforcementMode.DISABLED, QuotaEnforcementMode.DISABLED)),
            serverTenantQuota, brokerTenantQuota),
        new QuotaConfig("10G", "50", QuotaEnforcementMode.DISABLED, QuotaEnforcementMode.DISABLED));
  }

  @Test
  public void testTenantQuotaLookup() {
    Map<String, QuotaConfig> tenantQuotas =
        Map.of(SERVER_TENANT, new QuotaConfig("10G", null), BROKER_TENANT, new QuotaConfig(null, "50"));
    assertEquals(QuotaConfigUtils.getEffectiveQuotaConfig(table(null), tenantQuotas::get),
        new QuotaConfig("10G", "50"));

    // A tenant used for both roles is looked up only once
    TableConfig sameTenantTable = table(null);
    sameTenantTable.setTenantConfig(new TenantConfig(SERVER_TENANT, SERVER_TENANT, null));
    List<String> lookups = new ArrayList<>();
    QuotaConfigUtils.getEffectiveQuotaConfig(sameTenantTable, tenantName -> {
      lookups.add(tenantName);
      return tenantQuotas.get(tenantName);
    });
    assertEquals(lookups, List.of(SERVER_TENANT));

    TableConfig otherTenantTable = table(null);
    otherTenantTable.setTenantConfig(new TenantConfig("otherBroker", "otherServer", null));
    assertNull(QuotaConfigUtils.getEffectiveQuotaConfig(otherTenantTable, tenantQuotas::get));
  }

  @Test
  public void testValidateTenantQuotaConfig() {
    QuotaConfig storageQuota = new QuotaConfig("10G", null);
    QuotaConfig queryQuota = new QuotaConfig(null, null, null, QuotaEnforcementMode.SHADOW);
    QuotaConfig bothQuotas = new QuotaConfig("10G", "100");

    QuotaConfigUtils.validateTenantQuotaConfig(storageQuota, true, false);
    QuotaConfigUtils.validateTenantQuotaConfig(queryQuota, false, true);
    QuotaConfigUtils.validateTenantQuotaConfig(bothQuotas, true, true);

    assertThrows(IllegalArgumentException.class,
        () -> QuotaConfigUtils.validateTenantQuotaConfig(storageQuota, false, true));
    assertThrows(IllegalArgumentException.class,
        () -> QuotaConfigUtils.validateTenantQuotaConfig(queryQuota, true, false));
    assertThrows(IllegalArgumentException.class,
        () -> QuotaConfigUtils.validateTenantQuotaConfig(bothQuotas, true, false));
    assertThrows(IllegalArgumentException.class,
        () -> QuotaConfigUtils.validateTenantQuotaConfig(new QuotaConfig(null, null), true, true));
  }
}
