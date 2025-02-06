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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class SegmentPreprocessThrottlerTest {

  @Test
  public void testBasicAcquireRelease()
      throws Exception {
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(4, 8, true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(4, 8, true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.availablePermits(), 4);
      Assert.assertEquals(preprocessThrottler.totalPermits(), 4);

      preprocessThrottler.acquire();
      Assert.assertEquals(preprocessThrottler.availablePermits(), 3);
      Assert.assertEquals(preprocessThrottler.totalPermits(), 4);

      preprocessThrottler.release();
      Assert.assertEquals(preprocessThrottler.availablePermits(), 4);
      Assert.assertEquals(preprocessThrottler.totalPermits(), 4);
    }
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(totalPermits, totalPermits * 2, true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(totalPermits, totalPermits * 2, true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), totalPermits);

      for (int i = 0; i < totalPermits; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.availablePermits(), totalPermits - i - 1);
        Assert.assertEquals(preprocessThrottler.totalPermits(), totalPermits);
      }
      for (int i = 0; i < totalPermits; i++) {
        preprocessThrottler.release();
        Assert.assertEquals(preprocessThrottler.availablePermits(), i + 1);
        Assert.assertEquals(preprocessThrottler.totalPermits(), totalPermits);
      }
    }
  }

  @Test
  public void testThrowExceptionOnSettingInvalidConfigValues() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(-1, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(0, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(1, -4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(1, 0, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(-1, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(0, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(1, -4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentAllIndexPreprocessThrottler(1, 0, false));

    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(-1, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(0, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(1, -4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(1, 0, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(-1, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(0, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(1, -4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentStarTreePreprocessThrottler(1, 0, false));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM), Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM), Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      int defaultPermits = preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM)
          : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermits);
      for (int i = 0; i < 100; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermits);
        Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermits - i - 1);
      }
    }
  }

  @Test
  public void testPositiveToNegativeThrottleChange() {
    int initialPermits = 2;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);

      // Change the value of cluster config for max segment preprocess parallelism to be a negative value
      // If config is <= 0, this is an invalid configuration change. Do nothing other than log a warning
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "-1");
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      for (int i = 0; i < initialPermits; i++) {
        preprocessThrottler.acquire();
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), 0);

      // Increase the value of cluster config for max segment preprocess parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          String.valueOf(initialPermits * 2));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);

      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
      for (int i = 0; i < initialPermits; i++) {
        preprocessThrottler.acquire();
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);
      Assert.assertEquals(preprocessThrottler.availablePermits(), 0);
      for (int i = 0; i < (initialPermits * 2); i++) {
        preprocessThrottler.release();
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits * 2);
    }
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      for (int i = 0; i < initialPermits; i++) {
        preprocessThrottler.acquire();
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), 0);

      // Increase the value of cluster config for max segment preprocess parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          String.valueOf(initialPermits / 2));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits / 2);

      Assert.assertEquals(preprocessThrottler.availablePermits(), -(initialPermits / 2));
      for (int i = 0; i < 4; i++) {
        preprocessThrottler.release();
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits / 2);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits / 2);
    }
  }

  @Test
  public void testServingQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      int defaultPermitsBeforeQuery = preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      preprocessThrottler.startServingQueries();
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireRelease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      int defaultPermitsBeforeQuery = preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
      }

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      preprocessThrottler.startServingQueries();
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(),
          initialPermits - defaultPermitsBeforeQuery);

      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.release();
        Assert.assertEquals(preprocessThrottler.availablePermits(),
            (initialPermits - defaultPermitsBeforeQuery) + i + 1);
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigIncrease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      int defaultPermitsBeforeQuery = preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
      }

      // Double the permits for before serving queries config
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(defaultPermitsBeforeQuery * 2));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery * 2);
      // We doubled permits but took all of the previous ones
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);

      // Take remaining permits
      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery * 2);
        Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
      }

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      preprocessThrottler.startServingQueries();
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(),
          initialPermits - (defaultPermitsBeforeQuery * 2));

      for (int i = 0; i < defaultPermitsBeforeQuery * 2; i++) {
        preprocessThrottler.release();
        Assert.assertEquals(preprocessThrottler.availablePermits(),
            (initialPermits - defaultPermitsBeforeQuery * 2) + i + 1);
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigDecrease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      int defaultPermitsBeforeQuery = preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery);
      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.acquire();
        Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(preprocessThrottler.availablePermits(), defaultPermitsBeforeQuery - i - 1);
      }

      // Half the permits for before serving queries config
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(defaultPermitsBeforeQuery / 2));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), defaultPermitsBeforeQuery / 2);
      // We doubled permits but took all of the previous ones
      Assert.assertEquals(preprocessThrottler.availablePermits(), -(defaultPermitsBeforeQuery / 2));

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      preprocessThrottler.startServingQueries();
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(),
          initialPermits - defaultPermitsBeforeQuery);

      for (int i = 0; i < defaultPermitsBeforeQuery; i++) {
        preprocessThrottler.release();
        Assert.assertEquals(preprocessThrottler.availablePermits(),
            (initialPermits - defaultPermitsBeforeQuery) + i + 1);
      }
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(preprocessThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testThrowException()
      throws Exception {
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(1, 2, true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(1, 2, true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      BaseSegmentPreprocessThrottler spy = spy(preprocessThrottler);
      spy.acquire();
      Assert.assertEquals(spy.availablePermits(), 0);
      doThrow(new InterruptedException("interrupt")).when(spy).acquire();

      Assert.assertThrows(InterruptedException.class, spy::acquire);
      Assert.assertEquals(spy.availablePermits(), 0);
      spy.release();
      Assert.assertEquals(spy.availablePermits(), 1);
    }
  }

  @Test
  public void testChangeConfigsEmpty() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      // Add some random configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmpty() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Set<String> keys = new HashSet<>();
      keys.add(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
      preprocessThrottler.onChange(keys, null);
      Assert.assertEquals(preprocessThrottler.totalPermits(),
          Integer.parseInt(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM));
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmptyQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Set<String> keys = new HashSet<>();
      keys.add(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
      keys.add(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
          : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
      preprocessThrottler.onChange(keys, null);
      Assert.assertEquals(preprocessThrottler.totalPermits(),
          Integer.parseInt(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES));
    }
  }

  @Test
  public void testChangeConfigsOtherThanRelevant() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      // Add some random configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
          : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, "42");
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);
    }
  }

  @Test
  public void testChangeConfigs() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(initialPermits * 4));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = false, new total should match CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);
    }
  }

  @Test
  public void testChangeConfigsWithServingQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentPreprocessThrottler> segmentPreprocessThrottlerList = new ArrayList<>();
    segmentPreprocessThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentPreprocessThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        false));

    for (BaseSegmentPreprocessThrottler preprocessThrottler : segmentPreprocessThrottlerList) {
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 2);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(preprocessThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(initialPermits * 4));
      preprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = false, new total should match higher threshold of before serving queries
      Assert.assertEquals(preprocessThrottler.totalPermits(), initialPermits * 4);
    }
  }

  @Test
  public void testChangeConfigsOnSegmentPreprocessThrottler() {
    int initialPermits = 4;
    SegmentAllIndexPreprocessThrottler allIndexPreprocessThrottler = new SegmentAllIndexPreprocessThrottler(
        initialPermits, initialPermits * 2, true);
    SegmentStarTreePreprocessThrottler starTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(
        initialPermits, initialPermits * 2, true);
    SegmentPreprocessThrottler segmentPreprocessThrottler = new SegmentPreprocessThrottler(allIndexPreprocessThrottler,
        starTreePreprocessThrottler);

    Assert.assertEquals(segmentPreprocessThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits);

    // Add random and relevant configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 2);
  }

  @Test
  public void testChangeConfigsOnSegmentPreprocessThrottlerQueriesDisabled() {
    int initialPermits = 4;
    SegmentAllIndexPreprocessThrottler allIndexPreprocessThrottler = new SegmentAllIndexPreprocessThrottler(
        initialPermits, initialPermits * 2, false);
    SegmentStarTreePreprocessThrottler starTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(
        initialPermits, initialPermits * 2, false);
    SegmentPreprocessThrottler segmentPreprocessThrottler = new SegmentPreprocessThrottler(allIndexPreprocessThrottler,
        starTreePreprocessThrottler);

    Assert.assertEquals(segmentPreprocessThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 2);

    // Add random and relevant configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    updatedClusterConfigs.put(
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    segmentPreprocessThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 4);
    Assert.assertEquals(segmentPreprocessThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 4);
  }
}
