/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.realtime;

import com.linkedin.pinot.core.query.utils.Pair;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ControllerLeaderLocatorTest {

  /**
   * Tests the invalidate logic for cached controller leader
   * We set the value for lastCacheInvalidateMillis as we do not want to rely on operations being executed within or after the time thresholds in the tests
   */
  @Test
  public void testInvalidateCachedControllerLeader()  {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(znRecord.getId()).thenReturn(leaderHost + "_" + leaderPort);
    when(baseDataAccessor.get(anyString(), any(), anyInt())).thenReturn(znRecord);
    when(helixManager.getClusterName()).thenReturn("testCluster");

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    // check values at startup
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), 0);

    // very first invalidate
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    long lastCacheInvalidateMillis = controllerLeaderLocator.getLastCacheInvalidateMillis();
    Assert.assertTrue(lastCacheInvalidateMillis > 0);

    // invalidate within {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis
    // values should remain unchanged
    lastCacheInvalidateMillis = System.currentTimeMillis();
    controllerLeaderLocator.setLastCacheInvalidateMillis(lastCacheInvalidateMillis);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), lastCacheInvalidateMillis);

    // getControllerLeader, which validates the cache
    controllerLeaderLocator.getControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), lastCacheInvalidateMillis);

    // invalidate within {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis
    // values should remain unchanged
    lastCacheInvalidateMillis = System.currentTimeMillis();
    controllerLeaderLocator.setLastCacheInvalidateMillis(lastCacheInvalidateMillis);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertFalse(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    Assert.assertEquals(controllerLeaderLocator.getLastCacheInvalidateMillis(), lastCacheInvalidateMillis);

    // invalidate after {@link ControllerLeaderLocator::getMillisBetweenInvalidate()} millis have elapsed, by setting lastCacheInvalidateMillis to well before the millisBetweenInvalidate
    // cache should be invalidated and last cache invalidation time should get updated
    lastCacheInvalidateMillis = System.currentTimeMillis() - 2 * controllerLeaderLocator.getMillisBetweenInvalidate();
    controllerLeaderLocator.setLastCacheInvalidateMillis(lastCacheInvalidateMillis);
    controllerLeaderLocator.invalidateCachedControllerLeader();
    Assert.assertTrue(controllerLeaderLocator.isCachedControllerLeaderInvalid());
    Assert.assertTrue(controllerLeaderLocator.getLastCacheInvalidateMillis() > lastCacheInvalidateMillis);
  }

  @Test
  public void testNoControllerLeader() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(baseDataAccessor.get(anyString(), (Stat) any(), anyInt())).thenThrow(new RuntimeException());

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Assert.assertEquals(controllerLeaderLocator.getControllerLeader(), null);
  }

  @Test
  public void testControllerLeaderExists() {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    ZNRecord znRecord = mock(ZNRecord.class);
    final String leaderHost = "host";
    final int leaderPort = 12345;

    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);
    when(znRecord.getId()).thenReturn(leaderHost + "_" + leaderPort);
    when(baseDataAccessor.get(anyString(), (Stat) any(), anyInt())).thenReturn(znRecord);
    when(helixManager.getClusterName()).thenReturn("myCluster");

    // Create Controller Leader Locator
    FakeControllerLeaderLocator.create(helixManager);
    ControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    Pair<String, Integer> expectedLeaderLocation = new Pair<>(leaderHost, leaderPort);
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader().getFirst(), expectedLeaderLocation.getFirst());
    Assert.assertEquals(controllerLeaderLocator.getControllerLeader().getSecond(), expectedLeaderLocation.getSecond());
  }

  static class FakeControllerLeaderLocator extends ControllerLeaderLocator {
    private static ControllerLeaderLocator _instance = null;

    FakeControllerLeaderLocator(HelixManager helixManager) {
      super(helixManager);
    }

    public static void create(HelixManager helixManager) {
      _instance = new ControllerLeaderLocator(helixManager);
    }

    public static ControllerLeaderLocator getInstance() {
      return _instance;
    }
  }
}
