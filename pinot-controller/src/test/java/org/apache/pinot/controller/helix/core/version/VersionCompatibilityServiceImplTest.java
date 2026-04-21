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
package org.apache.pinot.controller.helix.core.version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class VersionCompatibilityServiceImplTest {

  private static final String CLUSTER = "TestCluster";

  private static InstanceConfig makeConfig(String name, String version) {
    InstanceConfig cfg = new InstanceConfig(name);
    if (version != null) {
      cfg.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    }
    return cfg;
  }

  /** Testable subclass that short-circuits Helix access with fake data. */
  private static final class FakeService extends VersionCompatibilityServiceImpl {
    private List<InstanceConfig> _configs;
    private Set<String> _liveNames;
    private boolean _failNext;
    private final AtomicInteger _fetchCount = new AtomicInteger();

    FakeService(ControllerConf conf) {
      super(mock(PinotHelixResourceManager.class), conf);
    }

    void set(List<InstanceConfig> configs, Set<String> liveNames) {
      _configs = configs;
      _liveNames = liveNames;
    }

    void failNextFetch() {
      _failNext = true;
    }

    int fetchCount() {
      return _fetchCount.get();
    }

    @Override
    protected List<InstanceConfig> fetchAllInstanceConfigs() {
      _fetchCount.incrementAndGet();
      if (_failNext) {
        _failNext = false;
        throw new RuntimeException("simulated Helix failure");
      }
      return _configs;
    }

    @Override
    protected List<String> fetchLiveInstanceNames() {
      return new ArrayList<>(_liveNames);
    }

    @Override
    protected String fetchClusterName() {
      return CLUSTER;
    }
  }

  private static ControllerConf confWithTtlSeconds(long ttlSec) {
    ControllerConf cc = new ControllerConf();
    cc.setProperty(ControllerConf.ControllerPeriodicTasksConf.VERSION_COMPATIBILITY_CACHE_TTL_SECONDS,
        String.valueOf(ttlSec));
    return cc;
  }

  @Test
  public void testHappyPathSummaryAndMinVersion() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("Broker_h2_8000", "1.2.3"),
        makeConfig("Server_h3_7000", "1.2.0"),
        makeConfig("Server_h4_7000", "1.2.3"),
        makeConfig("Minion_h5_6000", "1.2.0"));
    svc.set(configs, new HashSet<>(Arrays.asList(
        "Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000", "Server_h4_7000", "Minion_h5_6000")));

    ClusterVersionSummary summary = svc.getClusterVersionSummary();
    assertTrue(summary.isDataAvailable());
    assertEquals(summary.getClusterName(), CLUSTER);

    ComponentVersionSummary controller = summary.getComponentSummaries().get("CONTROLLER");
    assertEquals(controller.getLiveInstanceCount(), 1);
    assertEquals(controller.getMinVersion(), "1.2.3");

    ComponentVersionSummary server = summary.getComponentSummaries().get("SERVER");
    assertEquals(server.getLiveInstanceCount(), 2);
    assertEquals(server.getMinVersion(), "1.2.0");
    assertEquals(server.getMaxVersion(), "1.2.3");

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertTrue(result.isOk(), "SERVER 1.2.0 <= BROKER 1.2.3, should pass");
    assertTrue(result.getWarnings().isEmpty());
  }

  @Test
  public void testRolloutOrderViolationServerAheadOfBroker() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.3.0"),
        makeConfig("Broker_h2_8000", "1.2.3"),
        makeConfig("Server_h3_7000", "1.3.0"));
    svc.set(configs, new HashSet<>(Arrays.asList(
        "Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000")));

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertFalse(result.isOk());
    assertEquals(result.getWarnings().size(), 1);
    assertTrue(result.getWarnings().get(0).contains("SERVER"));
    assertTrue(result.getWarnings().get(0).contains("BROKER"));
  }

  @Test
  public void testRolloutOrderViolationBrokerAheadOfController() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.0"),
        makeConfig("Broker_h2_8000", "1.3.0"),
        makeConfig("Server_h3_7000", "1.2.0"));
    svc.set(configs, new HashSet<>(Arrays.asList(
        "Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000")));

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertFalse(result.isOk());
    assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("BROKER") && w.contains("CONTROLLER")));
  }

  @Test
  public void testMissingBrokerFallsBackToControllerParent() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    // No brokers. SERVER must anchor on CONTROLLER instead.
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.0"),
        makeConfig("Server_h3_7000", "1.3.0"));
    svc.set(configs, new HashSet<>(Arrays.asList("Controller_h1_9000", "Server_h3_7000")));

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertFalse(result.isOk());
    assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("SERVER") && w.contains("CONTROLLER")));
  }

  @Test
  public void testOfflineInstancesExcludedFromMin() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    // Decommissioned host with stale 0.9.0 config must not drag the min version down.
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("Broker_h2_8000", "1.2.3"),
        makeConfig("Server_h3_7000", "1.2.3"),
        makeConfig("Server_old_9999", "0.9.0"));
    svc.set(configs, new HashSet<>(Arrays.asList(
        "Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000")));

    ClusterVersionSummary summary = svc.getClusterVersionSummary();
    ComponentVersionSummary server = summary.getComponentSummaries().get("SERVER");
    assertEquals(server.getLiveInstanceCount(), 1);
    assertEquals(server.getOfflineInstanceCount(), 1);
    assertEquals(server.getMinVersion(), "1.2.3", "offline instance must not drag min down");
  }

  @Test
  public void testUnknownVersionSurfacedAsWarningAndCount() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("Broker_h2_8000", "1.2.3"),
        makeConfig("Server_h3_7000", "1.2.3"),
        makeConfig("Server_h4_7000", PinotVersion.UNKNOWN),
        makeConfig("Server_h5_7000", null));
    svc.set(configs, new HashSet<>(Arrays.asList(
        "Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000", "Server_h4_7000", "Server_h5_7000")));

    ClusterVersionSummary summary = svc.getClusterVersionSummary();
    ComponentVersionSummary server = summary.getComponentSummaries().get("SERVER");
    assertEquals(server.getUnknownVersionCount(), 2);
    assertEquals(server.getMinVersion(), "1.2.3",
        "UNKNOWN instances must not drag the min version below the parseable floor");

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("SERVER") && w.contains("UNKNOWN")));
  }

  @Test
  public void testCachingWithinTtl() throws InterruptedException {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    svc.set(Collections.emptyList(), Collections.emptySet());
    svc.getClusterVersionSummary();
    svc.getClusterVersionSummary();
    svc.getClusterVersionSummary();
    assertEquals(svc.fetchCount(), 1, "within TTL, expect only one Helix fetch");
  }

  @Test
  public void testFailureReturnsUnavailableWhenNoCache() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    svc.set(Collections.emptyList(), Collections.emptySet());
    svc.failNextFetch();

    ClusterVersionSummary summary = svc.getClusterVersionSummary();
    assertFalse(summary.isDataAvailable());
    assertEquals(summary.getClusterName(), CLUSTER);

    CompatibilityCheckResult result = svc.checkRolloutOrderCompatibility();
    assertFalse(result.isOk());
    // Unavailable-cause must be visible to warnings-only clients.
    assertFalse(result.getWarnings().isEmpty());
    assertTrue(result.getWarnings().get(0).toLowerCase().contains("unavailable"));
  }

  @Test
  public void testFailureKeepsGoodCachedSnapshot() throws InterruptedException {
    // 1-second TTL so we can trigger natural expiry during the test.
    FakeService svc = new FakeService(confWithTtlSeconds(1));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("Broker_h2_8000", "1.2.3"));
    svc.set(configs, new HashSet<>(Arrays.asList("Controller_h1_9000", "Broker_h2_8000")));

    ClusterVersionSummary first = svc.getClusterVersionSummary();
    long originalSnapshotTime = first.getSnapshotTimeMs();
    assertTrue(first.isDataAvailable());

    // Wait for TTL expiry, then force a failing refresh. On TTL-driven refresh, the stale-but-good
    // contract kicks in: the previously-good snapshot must be preserved, not replaced with an
    // unavailable one.
    Thread.sleep(1100);
    svc.failNextFetch();
    ClusterVersionSummary second = svc.getClusterVersionSummary();
    assertTrue(second.isDataAvailable(), "stale-but-good snapshot must be served on refresh failure");
    assertEquals(second.getSnapshotTimeMs(), originalSnapshotTime,
        "snapshot time must reflect original data, not the failed attempt");
  }

  @Test
  public void testGetInstanceVersionInfo() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("Broker_h2_8000", "1.2.3"),
        makeConfig("Server_h3_7000", "1.2.0"));
    svc.set(configs, new HashSet<>(Arrays.asList("Controller_h1_9000", "Broker_h2_8000", "Server_h3_7000")));

    InstanceVersionInfo info = svc.getInstanceVersionInfo("Server_h3_7000");
    assertNotNull(info);
    assertEquals(info.getRawVersion(), "1.2.0");
    assertEquals(info.getComponentType(), "SERVER");
    assertTrue(info.isLive());

    assertNull(svc.getInstanceVersionInfo("Server_does_not_exist"));
  }

  @Test
  public void testUnrecognizedInstancePrefixIsSkipped() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    // "randomhost" has no known prefix. It should not end up in any component bucket.
    List<InstanceConfig> configs = Arrays.asList(
        makeConfig("Controller_h1_9000", "1.2.3"),
        makeConfig("randomhost_xyz", "1.2.3"));
    svc.set(configs, new HashSet<>(Arrays.asList("Controller_h1_9000", "randomhost_xyz")));

    ClusterVersionSummary summary = svc.getClusterVersionSummary();
    for (Map.Entry<String, ComponentVersionSummary> e : summary.getComponentSummaries().entrySet()) {
      for (InstanceVersionInfo info : e.getValue().getInstances()) {
        assertFalse(info.getInstanceName().startsWith("randomhost"),
            "unrecognized prefix must not be bucketed into any known component type");
      }
    }
  }

  @Test
  public void testOnChangeUpdatesTtl() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    svc.set(Collections.emptyList(), Collections.emptySet());
    svc.getClusterVersionSummary();
    int fetchesBefore = svc.fetchCount();

    // Change TTL to a different positive value; cache should be invalidated.
    svc.onChange(Collections.singleton(
            ControllerConf.ControllerPeriodicTasksConf.VERSION_COMPATIBILITY_CACHE_TTL_SECONDS),
        Collections.singletonMap(
            ControllerConf.ControllerPeriodicTasksConf.VERSION_COMPATIBILITY_CACHE_TTL_SECONDS, "5"));
    svc.getClusterVersionSummary();
    assertEquals(svc.fetchCount(), fetchesBefore + 1,
        "onChange with new TTL should invalidate cache and force refetch");
  }

  @Test
  public void testOnChangeIgnoresBadValue() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    svc.set(Collections.emptyList(), Collections.emptySet());
    svc.getClusterVersionSummary();
    int fetchesBefore = svc.fetchCount();

    svc.onChange(Collections.singleton(
            ControllerConf.ControllerPeriodicTasksConf.VERSION_COMPATIBILITY_CACHE_TTL_SECONDS),
        Collections.singletonMap(
            ControllerConf.ControllerPeriodicTasksConf.VERSION_COMPATIBILITY_CACHE_TTL_SECONDS, "not-a-number"));
    svc.getClusterVersionSummary();
    assertEquals(svc.fetchCount(), fetchesBefore,
        "onChange with malformed value should neither invalidate nor crash");
  }

  @Test
  public void testInvalidateCacheForcesRefetch() {
    FakeService svc = new FakeService(confWithTtlSeconds(30));
    svc.set(Collections.emptyList(), Collections.emptySet());
    svc.getClusterVersionSummary();
    int before = svc.fetchCount();

    svc.invalidateCache();
    svc.getClusterVersionSummary();
    assertEquals(svc.fetchCount(), before + 1);
  }
}
