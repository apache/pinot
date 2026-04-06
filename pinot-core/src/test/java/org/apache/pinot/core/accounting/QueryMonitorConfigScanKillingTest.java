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
package org.apache.pinot.core.accounting;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryMonitorConfigScanKillingTest {

  private static final long MAX_HEAP = 1_000_000_000L;

  @Test
  public void testDefaultValues() {
    PinotConfiguration config = new PinotConfiguration();
    QueryMonitorConfig qmc = new QueryMonitorConfig(config, MAX_HEAP);
    assertFalse(qmc.isScanBasedKillingEnabled());
    assertFalse(qmc.isScanBasedKillingLogOnly());
    assertEquals(qmc.getScanBasedKillingMaxEntriesScannedInFilter(), Long.MAX_VALUE);
    assertEquals(qmc.getScanBasedKillingMaxDocsScanned(), Long.MAX_VALUE);
    assertEquals(qmc.getScanBasedKillingMaxEntriesScannedPostFilter(), Long.MAX_VALUE);
  }

  @Test
  public void testExplicitValues() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.CONFIG_OF_SCAN_BASED_KILLING_ENABLED, "true");
    config.setProperty(Accounting.CONFIG_OF_SCAN_BASED_KILLING_LOG_ONLY, "true");
    config.setProperty(Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER, "500000000");
    config.setProperty(Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_DOCS_SCANNED, "50000000");
    config.setProperty(Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_POST_FILTER, "100000000");

    QueryMonitorConfig qmc = new QueryMonitorConfig(config, MAX_HEAP);
    assertTrue(qmc.isScanBasedKillingEnabled());
    assertTrue(qmc.isScanBasedKillingLogOnly());
    assertEquals(qmc.getScanBasedKillingMaxEntriesScannedInFilter(), 500_000_000L);
    assertEquals(qmc.getScanBasedKillingMaxDocsScanned(), 50_000_000L);
    assertEquals(qmc.getScanBasedKillingMaxEntriesScannedPostFilter(), 100_000_000L);
  }

  @Test
  public void testDynamicConfigUpdate() {
    PinotConfiguration config = new PinotConfiguration();
    QueryMonitorConfig oldConfig = new QueryMonitorConfig(config, MAX_HEAP);
    assertFalse(oldConfig.isScanBasedKillingEnabled());

    Set<String> changedConfigs = new HashSet<>();
    changedConfigs.add(Accounting.CONFIG_OF_SCAN_BASED_KILLING_ENABLED);
    changedConfigs.add(Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER);

    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(Accounting.CONFIG_OF_SCAN_BASED_KILLING_ENABLED, "true");
    clusterConfigs.put(Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER, "200000000");

    QueryMonitorConfig newConfig = new QueryMonitorConfig(oldConfig, changedConfigs, clusterConfigs);
    assertTrue(newConfig.isScanBasedKillingEnabled());
    assertEquals(newConfig.getScanBasedKillingMaxEntriesScannedInFilter(), 200_000_000L);
    assertFalse(newConfig.isScanBasedKillingLogOnly());
    assertEquals(newConfig.getScanBasedKillingMaxDocsScanned(), Long.MAX_VALUE);
  }
}
