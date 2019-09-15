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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.core.query.utils.Pair;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ControllerLeaderLocatorTest {
  private final String testTable = "testTable";

  /**
   * Tests the invalidate logic for cached controller leader
   * We set the value for lastCacheInvalidateMillis as we do not want to rely on operations being executed within or after the time thresholds in the tests
   */
  @Test
  public void testInvalidateCachedControllerLeader() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(anyString(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(znRecord.getId()).thenReturn(leaderHost + "_" + leaderPort);
    when(baseDataAccessor.get(anyString(), any(), anyInt())).thenReturn(znRecord);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    FakeControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    // check values at startup
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), 0);

    // very first invalidate
    long currentTimeMs = System.currentTimeMillis();
    controllerLeaderLocator.setCurrentTimeMs(currentTimeMs);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    long lastCacheInvalidateMillis = controllerLeaderLocator.getLastCacheInvalidateMillis();
    Assert.assertTrue(lastCacheInvalidateMillis > 0);
    Assert.assertEquals(lastCacheInvalidateMillis, currentTimeMs);

    // invalidate within {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis
    // values should remain unchanged
    lastCacheInvalidateMillis = System.currentTimeMillis();
    controllerLeaderLocator.setCurrentTimeMs(lastCacheInvalidateMillis);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), lastCacheInvalidateMillis);

    // getControllerLeader, which validates the cache
    controllerLeaderLocator.getControllerLeader(testTable);
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), lastCacheInvalidateMillis);

    // invalidate within {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis
    // values should remain unchanged
    currentTimeMs = System.currentTimeMillis();
    controllerLeaderLocator.setCurrentTimeMs(currentTimeMs);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertTrue(controllerLeaderLocator.getLastCacheInvalidateMillis() < currentTimeMs);

    // invalidate after {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis have elapsed, by setting lastCacheInvalidateMillis to well before the millisBetweenInvalidate
    // cache should be invalidated and last cache invalidation time should get updated
    controllerLeaderLocator.setCurrentTimeMs(
        controllerLeaderLocator.getCurrentTimeMS() + 2 * controllerLeaderLocator.getMillisBetweenInvalidate());
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderValid());
    Assert.assertTrue(controllerLeaderLocator.getLastCacheInvalidateMillis() > lastCacheInvalidateMillis);
  }

  @Test
  public void testNoControllerLeader() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(baseDataAccessor.get(anyString(), (Stat) any(), anyInt())).thenThrow(new RuntimeException());
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable), null);
  }

  @Test
  public void testControllerLeaderExists() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(anyString(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(znRecord.getId()).thenReturn(leaderHost + "_" + leaderPort);
    when(baseDataAccessor.get(anyString(), (Stat) any(), anyInt())).thenReturn(znRecord);
    when(helixManager.getClusterName()).thenReturn("myCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Pair<String, Integer> expectedLeaderLocation = new Pair<>(leaderHost, leaderPort);
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getFirst(),
        expectedLeaderLocation.getFirst());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getSecond(),
        expectedLeaderLocation.getSecond());
  }

  @Test
  public void testWhenLeadControllerResourceEnabled() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(znRecord.getId()).thenReturn(leaderHost + "_" + leaderPort);
    when(baseDataAccessor.get(anyString(), (Stat) any(), anyInt())).thenReturn(znRecord);
    when(helixManager.getClusterName()).thenReturn("myCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(null);

    // Lead controller resource disabled.
    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    ResourceConfig resourceConfig = mock(ResourceConfig.class);
    when(helixManager.getConfigAccessor()).thenReturn(configAccessor);
    when(configAccessor.getResourceConfig(anyString(), anyString())).thenReturn(resourceConfig);
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("false");

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();
    Pair<String, Integer> expectedLeaderLocation = new Pair<>(leaderHost, leaderPort);

    // Before enabling lead controller resource config, the helix leader should be used.
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getFirst(),
        expectedLeaderLocation.getFirst());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getSecond(),
        expectedLeaderLocation.getSecond());

    // Mock the behavior that 40 seconds have passed.
    ((FakeControllerLeaderLocator) controllerLeaderLocator)
        .setCurrentTimeMs(controllerLeaderLocator.getCurrentTimeMS() + 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // After enabling lead controller resource config, the leader in lead controller resource should be used.
    when(resourceConfig.getSimpleConfig(anyString())).thenReturn("true");


    // External view is null, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(testTable));

    ExternalView externalView = mock(ExternalView.class);
    when(helixAdmin.getResourceExternalView(anyString(), anyString())).thenReturn(externalView);
    Set<String> partitionSet = new HashSet<>();
    when(externalView.getPartitionSet()).thenReturn(partitionSet);
    Map<String, String> partitionStateMap = new HashMap<>();
    when(externalView.getStateMap(anyString())).thenReturn(partitionStateMap);

    // External view is empty, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(testTable));

    // Adding one host as master, should return the correct host-port pair.
    partitionSet.add(LeadControllerUtils.generatePartitionName(LeadControllerUtils.getPartitionIdForTable(testTable)));
    for (int i = 0; i < CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
      partitionSet.add(LeadControllerUtils.generatePartitionName(i));
    }
    partitionStateMap.put(LeadControllerUtils.generateParticipantInstanceId(leaderHost, leaderPort), "MASTER");

    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getFirst(),
        expectedLeaderLocation.getFirst());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(testTable).getSecond(),
        expectedLeaderLocation.getSecond());

    // The participant host is in offline state, should return null.
    partitionStateMap.put(LeadControllerUtils.generateParticipantInstanceId(leaderHost, leaderPort), "OFFLINE");

    // The leader is still valid since the leader is just updated within 30 seconds.
    Assert.assertNotNull(controllerLeaderLocator.getControllerLeader(testTable));

    // Mock the behavior that 40 seconds have passed.
    ((FakeControllerLeaderLocator) controllerLeaderLocator)
        .setCurrentTimeMs(controllerLeaderLocator.getCurrentTimeMS() + 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // No controller in MASTER state, should return null.
    Assert.assertNull(controllerLeaderLocator.getControllerLeader(testTable));
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

    protected long getCurrentTimeMS() {
      return _currentTimeMs;
    }

    public void setCurrentTimeMs(long currentTimeMs) {
      _currentTimeMs = currentTimeMs;
    }
  }
}
