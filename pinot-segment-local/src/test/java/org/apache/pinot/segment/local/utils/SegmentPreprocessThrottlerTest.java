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
package org.apache.pinot.segment.local.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class SegmentPreprocessThrottlerTest {
  private AutoCloseable _mocks;
  @Mock
  private HelixManager _helixManager;
  @Mock
  private HelixAdmin _helixAdmin;
  private SegmentPreprocessThrottler _segmentPreprocessThrottler;

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
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(4, 8, false);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 4);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), 4);

    _segmentPreprocessThrottler.acquire();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 3);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), 4);

    _segmentPreprocessThrottler.release();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 4);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), 4);
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(totalPermits, totalPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), totalPermits);

    for (int i = 0; i < totalPermits; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), totalPermits - i - 1);
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), totalPermits);
    }
    for (int i = 0; i < totalPermits; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), i + 1);
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), totalPermits);
    }
  }

  @Test
  public void testThrowExceptionOnSettingInvalidConfigValues()
      throws Exception {
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentPreprocessThrottler(-1, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentPreprocessThrottler(0, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentPreprocessThrottler(1, -4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentPreprocessThrottler(1, 0, false));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    int defaultPermits = Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    int defaultPermitsBeforeQuery =
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(defaultPermits, defaultPermitsBeforeQuery, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermits);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermits);
    for (int i = 0; i < 100; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermits);
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermits - i - 1);
    }
  }

  @Test
  public void testPositiveToNegativeThrottleChange()
      throws Exception {
    int initialPermits = 2;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);

    // Change the value of cluster config for max segment preprocess parallelism to be a negative value
    // If maxConcurrentQueries is <= 0, this is an invalid configuration change. Do nothing other than log a warning
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "-1");
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 2);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);
    for (int i = 0; i < (initialPermits * 2); i++) {
      _segmentPreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits * 2);
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits / 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits / 2);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), -(initialPermits / 2));
    for (int i = 0; i < 4; i++) {
      _segmentPreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits / 2);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits / 2);
  }

  @Test
  public void testRelaxThrottling() {
    int initialPermits = 4;
    int defaultPermitsBeforeQuery =
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, defaultPermitsBeforeQuery, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testRelaxThrottlingWithAcquireRelease()
      throws InterruptedException {
    int initialPermits = 4;
    int defaultPermitsBeforeQuery =
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, defaultPermitsBeforeQuery, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
    }

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits - defaultPermitsBeforeQuery);

    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
          (initialPermits - defaultPermitsBeforeQuery) + i + 1);
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testRelaxThrottlingWithAcquireReleaseWithConfigIncrease()
      throws InterruptedException {
    int initialPermits = 4;
    int defaultPermitsBeforeQuery =
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, defaultPermitsBeforeQuery, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
    }

    // Double the permits for before serving queries config
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(defaultPermitsBeforeQuery * 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery * 2);
    // We doubled permits but took all of the previous ones
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);

    // Take remaining permits
    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery * 2);
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
    }

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        initialPermits - (defaultPermitsBeforeQuery * 2));

    for (int i = 0; i < defaultPermitsBeforeQuery * 2; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
          (initialPermits - defaultPermitsBeforeQuery * 2) + i + 1);
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testRelaxThrottlingWithAcquireReleaseWithConfigDecrease()
      throws InterruptedException {
    int initialPermits = 4;
    int defaultPermitsBeforeQuery =
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, defaultPermitsBeforeQuery, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
    }

    // Half the permits for before serving queries config
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(defaultPermitsBeforeQuery / 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), defaultPermitsBeforeQuery / 2);
    // We doubled permits but took all of the previous ones
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), -(defaultPermitsBeforeQuery / 2));

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits - defaultPermitsBeforeQuery);

    for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
          (initialPermits - defaultPermitsBeforeQuery) + i + 1);
    }
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testThrowException()
      throws Exception {
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(1, 2, false);
    SegmentPreprocessThrottler spy = spy(_segmentPreprocessThrottler);
    spy.acquire();
    Assert.assertEquals(spy.availablePermits(), 0);
    doThrow(new InterruptedException("interrupt")).when(spy).acquire();

    Assert.assertThrows(InterruptedException.class, spy::acquire);
    Assert.assertEquals(spy.availablePermits(), 0);
    spy.release();
    Assert.assertEquals(spy.availablePermits(), 1);
  }

  @Test
  public void testChangeConfigsEmpty()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);

    // Add some random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigsOtherThanRelevant()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);

    // Add some random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigs()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, false);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits);

    // Add random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    // Since relaxThrottling = false, new total should match CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 2);
  }

  @Test
  public void testChangeConfigsWithRelaxThrottling()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, initialPermits * 2, true);
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 2);

    // Add random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    // Since relaxThrottling = false, new total should match CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
    Assert.assertEquals(_segmentPreprocessThrottler.totalPermits(), initialPermits * 4);
  }
}
