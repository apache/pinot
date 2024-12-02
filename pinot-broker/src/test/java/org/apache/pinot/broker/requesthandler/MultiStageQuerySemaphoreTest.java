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
package org.apache.pinot.broker.requesthandler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


public class MultiStageQuerySemaphoreTest {

  private AutoCloseable _mocks;
  @Mock
  private HelixManager _helixManager;
  @Mock
  private HelixAdmin _helixAdmin;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_helixManager.getClusterManagmentTool()).thenReturn(_helixAdmin);
    when(_helixManager.getClusterName()).thenReturn("testCluster");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testBasicAcquireRelease()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 10);
    semaphore.tryAcquire(100, TimeUnit.MILLISECONDS);
    Assert.assertEquals(semaphore.availablePermits(), 4);
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 5);
  }

  @Test
  public void testAcquireTimeout()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testDisabledSemaphore()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(1, -1);

    // If maxConcurrentQueries is <= 0, semaphore should be "disabled" and any attempt to acquire should succeed
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testIncreaseNumBrokers()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.init(_helixManager);

    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    // Increase the number of brokers
    when(_helixAdmin.getInstancesInCluster(eq("testCluster"))).thenReturn(
        List.of("Broker_0", "Broker_1", "Broker_2", "Broker_3"));
    semaphore.processClusterChange(HelixConstants.ChangeType.EXTERNAL_VIEW);

    Assert.assertEquals(semaphore.availablePermits(), -1);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 1);
  }

  @Test
  public void testDecreaseNumBrokers()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.init(_helixManager);

    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    // Decrease the number of brokers
    when(_helixAdmin.getInstancesInCluster(eq("testCluster"))).thenReturn(List.of("Broker_0"));
    semaphore.processClusterChange(HelixConstants.ChangeType.EXTERNAL_VIEW);

    Assert.assertEquals(semaphore.availablePermits(), 2);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 1);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    semaphore.release();
    semaphore.release();
    semaphore.release();
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 4);
  }

  @Test
  public void testIncreaseMaxConcurrentQueries()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.init(_helixManager);

    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    // Increase the value of cluster config maxConcurrentQueries
    when(_helixAdmin.getConfig(any(),
        eq(Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES))))
        .thenReturn(Map.of(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES, "10"));
    semaphore.processClusterChange(HelixConstants.ChangeType.CLUSTER_CONFIG);

    Assert.assertEquals(semaphore.availablePermits(), 3);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 2);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 1);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    semaphore.release();
    semaphore.release();
    semaphore.release();
    semaphore.release();
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 5);
  }

  @Test
  public void testDecreaseMaxConcurrentQueries()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.init(_helixManager);

    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    semaphore.acquire();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));

    // Decrease the value of cluster config maxConcurrentQueries
    when(_helixAdmin.getConfig(any(),
        eq(Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES)))
    ).thenReturn(Map.of(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES, "2"));
    semaphore.processClusterChange(HelixConstants.ChangeType.CLUSTER_CONFIG);

    Assert.assertEquals(semaphore.availablePermits(), -1);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    semaphore.release();
    Assert.assertEquals(semaphore.availablePermits(), 1);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testEnabledToDisabledTransitionDisallowed()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(2, 4);
    semaphore.init(_helixManager);

    Assert.assertEquals(semaphore.availablePermits(), 2);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 1);

    // Disable the semaphore via cluster config change
    when(_helixAdmin.getConfig(any(),
        eq(Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES)))
    ).thenReturn(Map.of(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES, "-1"));
    semaphore.processClusterChange(HelixConstants.ChangeType.CLUSTER_CONFIG);

    // Should not be allowed to disable the semaphore if it is enabled during startup
    Assert.assertEquals(semaphore.availablePermits(), 1);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testDisabledToEnabledTransitionDisallowed()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(1, -1);
    semaphore.init(_helixManager);

    // If maxConcurrentQueries is <= 0, semaphore should be "disabled" and any attempt to acquire should succeed
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    }

    // Enable the semaphore via cluster config change
    when(_helixAdmin.getConfig(any(),
        eq(Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES)))
    ).thenReturn(Map.of(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES, "4"));
    semaphore.processClusterChange(HelixConstants.ChangeType.CLUSTER_CONFIG);

    // Should not be allowed to enable the semaphore if it is disabled during startup
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    }
    for (int i = 0; i < 100; i++) {
      semaphore.acquire();
    }
  }

  @Test
  public void testMaxConcurrentQueriesSmallerThanNumBrokers()
      throws Exception {
    MultiStageQuerySemaphore semaphore = new MultiStageQuerySemaphore(4, 2);
    semaphore.init(_helixManager);

    // The total permits should be capped at 1 even though maxConcurrentQueries / numBrokers is 0.
    Assert.assertEquals(semaphore.availablePermits(), 1);
    Assert.assertTrue(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
    Assert.assertEquals(semaphore.availablePermits(), 0);
    Assert.assertFalse(semaphore.tryAcquire(100, TimeUnit.MILLISECONDS));
  }
}
