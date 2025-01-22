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
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(4, false);

    _segmentPreprocessThrottler.acquire();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 3);
    _segmentPreprocessThrottler.release();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 4);
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(totalPermits, false);

    for (int i = 0; i < totalPermits; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), totalPermits - i - 1);
    }
    for (int i = 0; i < totalPermits; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), i + 1);
    }
  }

  @Test
  public void testDisabledThrottlingBySettingNegativeValue()
      throws Exception {
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(-1, false);

    // If maxConcurrentQueries is <= 0, the throttling mechanism should be set to a very large value, and any practical
    // attempts to acquire should succeed
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM));
    for (int i = 0; i < 100; i++) {
      _segmentPreprocessThrottler.acquire();
    }
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    int defaultPermits = Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(defaultPermits, false);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), defaultPermits);
    for (int i = 0; i < 100; i++) {
      _segmentPreprocessThrottler.acquire();
    }
  }

  @Test
  public void testNegativeToPositiveThrottleChange()
      throws Exception {
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(-1, false);

    // If maxConcurrentQueries is <= 0, the throttling mechanism should be set to a very large value, and any practical
    // attempts to acquire should succeed
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM));

    // Change the value of cluster config for max segment preprocess parallelism to be a positive value
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "2");
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 2);
  }

  @Test
  public void testPositiveToNegativeThrottleChange()
      throws Exception {
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(2, false);

    // If maxConcurrentQueries is <= 0, the throttling mechanism should be set to a very large value, and any practical
    // attempts to acquire should succeed
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 2);

    // Change the value of cluster config for max segment preprocess parallelism to be a positive value
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "-1");
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM));
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, false);

    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);
    for (int i = 0; i < (initialPermits * 2); i++) {
      _segmentPreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), (initialPermits * 2));
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, false);

    for (int i = 0; i < initialPermits; i++) {
      _segmentPreprocessThrottler.acquire();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits / 2));
    _segmentPreprocessThrottler.onChange(updatedClusterConfigs);

    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), -(initialPermits / 2));
    for (int i = 0; i < 4; i++) {
      _segmentPreprocessThrottler.release();
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), (initialPermits / 2));
  }

  @Test
  public void testRelaxThrottling() {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testRelaxThrottlingWithAcquireRelease()
      throws InterruptedException {
    int initialPermits = 4;
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(initialPermits, true);
    // We set relaxThrottling to true when the server is not yet ready to server queries. In this scenario ideally
    // preprocessing more segments is acceptable and cannot affect the query performance
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    for (int i = 0; i < CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES; i++) {
      _segmentPreprocessThrottler.acquire();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
          CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES - i - 1);
    }

    // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
    _segmentPreprocessThrottler.resetThrottling();
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
        (initialPermits - CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES));

    for (int i = 0; i < CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES; i++) {
      _segmentPreprocessThrottler.release();
      Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(),
          (initialPermits
              - CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES) + i + 1);
    }
    Assert.assertEquals(_segmentPreprocessThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testThrowException()
      throws Exception {
    _segmentPreprocessThrottler = new SegmentPreprocessThrottler(1, false);
    SegmentPreprocessThrottler spy = spy(_segmentPreprocessThrottler);
    spy.acquire();
    Assert.assertEquals(spy.availablePermits(), 0);
    doThrow(new InterruptedException("interrupt")).when(spy).acquire();

    Assert.assertThrows(InterruptedException.class, spy::acquire);
    Assert.assertEquals(spy.availablePermits(), 0);
    spy.release();
    Assert.assertEquals(spy.availablePermits(), 1);
  }
}
