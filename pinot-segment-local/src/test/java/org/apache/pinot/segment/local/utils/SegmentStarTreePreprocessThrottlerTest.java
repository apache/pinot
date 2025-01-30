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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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


public class SegmentStarTreePreprocessThrottlerTest {
  private AutoCloseable _mocks;
  @Mock
  private HelixManager _helixManager;
  @Mock
  private HelixAdmin _helixAdmin;
  private SegmentStarTreePreprocessThrottler _segmentStarTreePreprocessThrottler;

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
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(4);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 4);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), 4);

    _segmentStarTreePreprocessThrottler.acquire();
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 3);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), 4);

    _segmentStarTreePreprocessThrottler.release();
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 4);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), 4);
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(totalPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), totalPermits);

    for (int i = 0; i < totalPermits; i++) {
      _segmentStarTreePreprocessThrottler.acquire();
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), totalPermits - i - 1);
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), totalPermits);
    }
    for (int i = 0; i < totalPermits; i++) {
      _segmentStarTreePreprocessThrottler.release();
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), i + 1);
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), totalPermits);
    }
  }

  @Test
  public void testThrowExceptionOnSettingInvalidConfigValues() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(-1));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(0));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    int defaultPermits = Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(defaultPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), defaultPermits);

    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), defaultPermits);
    for (int i = 0; i < 100; i++) {
      _segmentStarTreePreprocessThrottler.acquire();
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), defaultPermits);
      Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), defaultPermits - i - 1);
    }
  }

  @Test
  public void testPositiveToNegativeThrottleChange() {
    int initialPermits = 2;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), initialPermits);

    // Change the value of cluster config for max segment preprocess parallelism to be a negative value
    // If config is <= 0, this is an invalid configuration change. Do nothing other than log a warning
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "-1");
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      _segmentStarTreePreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment startree preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits * 2);

    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), initialPermits);
    for (int i = 0; i < initialPermits; i++) {
      _segmentStarTreePreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 0);
    for (int i = 0; i < (initialPermits * 2); i++) {
      _segmentStarTreePreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), initialPermits * 2);
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      _segmentStarTreePreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment startree preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits / 2));
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits / 2);

    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), -(initialPermits / 2));
    for (int i = 0; i < 4; i++) {
      _segmentStarTreePreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits / 2);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.availablePermits(), initialPermits / 2);
  }

  @Test
  public void testThrowException()
      throws Exception {
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(1);
    SegmentStarTreePreprocessThrottler spy = spy(_segmentStarTreePreprocessThrottler);
    spy.acquire();
    Assert.assertEquals(spy.availablePermits(), 0);
    doThrow(new InterruptedException("interrupt")).when(spy).acquire();

    Assert.assertThrows(InterruptedException.class, spy::acquire);
    Assert.assertEquals(spy.availablePermits(), 0);
    spy.release();
    Assert.assertEquals(spy.availablePermits(), 1);
  }

  @Test
  public void testChangeConfigsEmpty() {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    // Add some random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigDeletedConfigsEmpty() {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
    Set<String> keys = new HashSet<>();
    keys.add(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    _segmentStarTreePreprocessThrottler.onChange(keys, null);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(),
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM));
  }

  @Test
  public void testChangeConfigsOtherThanRelevant() {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    // Add some random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "10");
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigs() {
    int initialPermits = 4;
    _segmentStarTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(initialPermits);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits);

    // Add random and relevant configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 4));
    _segmentStarTreePreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(_segmentStarTreePreprocessThrottler.totalPermits(), initialPermits * 2);
  }
}
