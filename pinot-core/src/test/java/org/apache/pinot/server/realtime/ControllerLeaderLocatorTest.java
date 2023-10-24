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
package org.apache.pinot.server.realtime;

import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ControllerLeaderLocatorTest {
  private static final String TEST_TABLE = "testTable";

  /**
   * Tests the invalidate logic for cached controller leader
   * We set the value for lastCacheInvalidateMillis as we do not want to rely on operations being executed within or
   * after the time thresholds in the tests
   */
  @Test
  public void testInvalidateCachedControllerLeader() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(any(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    // Mocks the helix leader
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    LiveInstance liveInstance = mock(LiveInstance.class);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(liveInstance);
    when(liveInstance.getInstanceName()).thenReturn(leaderHost + "_" + leaderPort);

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    FakeControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    // check values at startup
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidationTimeMs(), 0);

    // very first invalidate
    long currentTimeMs = 31_000L;
    controllerLeaderLocator.setCurrentTimeMs(currentTimeMs);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    long lastCacheInvalidateMillis = controllerLeaderLocator.getLastCacheInvalidationTimeMs();
    Assert.assertTrue(lastCacheInvalidateMillis > 0);
    Assert.assertEquals(lastCacheInvalidateMillis, currentTimeMs);

    // invalidate within {@link ControllerLeaderLocator::getMinInvalidateIntervalMs()} millis
    // values should remain unchanged
    currentTimeMs = currentTimeMs + controllerLeaderLocator.getMinInvalidateIntervalMs() / 3;
    controllerLeaderLocator.setCurrentTimeMs(currentTimeMs);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidationTimeMs(), lastCacheInvalidateMillis);

    // getControllerLeader, which validates the cache
    controllerLeaderLocator.getControllerLeader(TEST_TABLE);
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidationTimeMs(), lastCacheInvalidateMillis);

    // invalidate within {@link ControllerLeaderLocator::getMinInvalidateIntervalMs()} millis
    // values should remain unchanged
    currentTimeMs = currentTimeMs + controllerLeaderLocator.getMinInvalidateIntervalMs() / 3;
    controllerLeaderLocator.setCurrentTimeMs(currentTimeMs);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidationTimeMs(), lastCacheInvalidateMillis);

    // invalidate after {@link ControllerLeaderLocator::getMinInvalidateIntervalMs()} millis have elapsed, by setting
    // lastCacheInvalidateMillis to well before the millisBetweenInvalidate
    // cache should be invalidated and last cache invalidation time should get updated
    controllerLeaderLocator.setCurrentTimeMs(
        controllerLeaderLocator.getCurrentTimeMs() + 2 * controllerLeaderLocator.getMinInvalidateIntervalMs());
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertTrue(controllerLeaderLocator.getLastCacheInvalidationTimeMs() > lastCacheInvalidateMillis);
  }

  @Test
  public void testNoControllerLeader() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);

    // Mock that there is no helix leader.
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(null);

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(any(), any())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Assert.assertNull(controllerLeaderLocator.getControllerLeader(TEST_TABLE));
  }

  @Test
  public void testControllerLeaderExists() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(any(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    // Mocks the helix leader
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    LiveInstance liveInstance = mock(LiveInstance.class);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(liveInstance);
    when(liveInstance.getInstanceName()).thenReturn(leaderHost + "_" + leaderPort);

    when(helixManager.getClusterName()).thenReturn("myCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Pair<String, Integer> expectedLeaderLocation = Pair.of(leaderHost, leaderPort);
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getLeft(),
        expectedLeaderLocation.getLeft());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getRight(),
        expectedLeaderLocation.getRight());
  }

  @Test
  public void testWhenLeadControllerResourceEnabled() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    LiveInstance liveInstance = mock(LiveInstance.class);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(liveInstance);
    when(liveInstance.getInstanceName()).thenReturn(leaderHost + "_" + leaderPort);

    when(helixManager.getClusterName()).thenReturn("myCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(any(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    FakeControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();
    Pair<String, Integer> expectedLeaderLocation = Pair.of(leaderHost, leaderPort);

    // Before enabling lead controller resource config, the helix leader should be used.
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getLeft(),
        expectedLeaderLocation.getLeft());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getRight(),
        expectedLeaderLocation.getRight());

    // Mock the behavior that 40 seconds have passed.
    controllerLeaderLocator.setCurrentTimeMs(controllerLeaderLocator.getCurrentTimeMs() + 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // After enabling lead controller resource config, the leader in lead controller resource should be used.
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("true");

    // External view is null, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(TEST_TABLE));

    ExternalView externalView = new ExternalView(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    PropertyKey externalViewPropertyKey = mock(PropertyKey.class);
    when(keyBuilder.externalView(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME))
        .thenReturn(externalViewPropertyKey);
    when(helixDataAccessor.getProperty(externalViewPropertyKey)).thenReturn(externalView);

    // External view is empty, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(TEST_TABLE));

    // Use custom instance id
    String participantInstanceId = "Controller_myInstance";
    InstanceConfig instanceConfig = new InstanceConfig(participantInstanceId);
    instanceConfig.setHostName(leaderHost);
    instanceConfig.setPort(Integer.toString(leaderPort));
    PropertyKey instanceConfigPropertyKey = mock(PropertyKey.class);
    when(keyBuilder.instanceConfig(participantInstanceId)).thenReturn(instanceConfigPropertyKey);
    when(helixDataAccessor.getProperty(instanceConfigPropertyKey)).thenReturn(instanceConfig);

    // Adding one host as master, should return the correct host-port pair.
    Map<String, String> instanceStateMap = new TreeMap<>();
    instanceStateMap.put(participantInstanceId, "MASTER");
    for (int i = 0; i < CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
      externalView.setStateMap(LeadControllerUtils.generatePartitionName(i), instanceStateMap);
    }

    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getLeft(),
        expectedLeaderLocation.getLeft());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(TEST_TABLE).getRight(),
        expectedLeaderLocation.getRight());

    // The participant host is in offline state, should return null.
    instanceStateMap.put(participantInstanceId, "OFFLINE");

    // The leader is still valid since the leader is just updated within 30 seconds.
    Assert.assertNotNull(controllerLeaderLocator.getControllerLeader(TEST_TABLE));

    // Mock the behavior that 40 seconds have passed.
    controllerLeaderLocator.setCurrentTimeMs(controllerLeaderLocator.getCurrentTimeMs() + 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // No controller in MASTER state, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(TEST_TABLE));
  }

  static class FakeControllerLeaderLocator extends ControllerLeaderLocator {
    private static FakeControllerLeaderLocator _instance = null;
    private long _currentTimeMs;

    FakeControllerLeaderLocator(HelixManager helixManager) {
      super(helixManager);
    }

    public static void create(HelixManager helixManager) {
      _instance = new FakeControllerLeaderLocator(helixManager);
    }

    public static FakeControllerLeaderLocator getInstance() {
      return _instance;
    }

    protected long getCurrentTimeMs() {
      return _currentTimeMs;
    }

    void setCurrentTimeMs(long currentTimeMs) {
      _currentTimeMs = currentTimeMs;
    }
  }
}
