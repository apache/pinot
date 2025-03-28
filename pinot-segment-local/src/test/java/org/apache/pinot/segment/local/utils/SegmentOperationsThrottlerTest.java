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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class SegmentOperationsThrottlerTest {

  private final ServerMetrics _serverMetrics = ServerMetrics.get();
  private final List<String> _thresholdGauges =
      Arrays.asList(ServerGauge.SEGMENT_ALL_PREPROCESS_THROTTLE_THRESHOLD.getGaugeName(),
          ServerGauge.SEGMENT_STARTREE_PREPROCESS_THROTTLE_THRESHOLD.getGaugeName(),
          ServerGauge.SEGMENT_DOWNLOAD_THROTTLE_THRESHOLD.getGaugeName());
  private final List<String> _countGauges =
      Arrays.asList(ServerGauge.SEGMENT_ALL_PREPROCESS_COUNT.getGaugeName(),
          ServerGauge.SEGMENT_STARTREE_PREPROCESS_COUNT.getGaugeName(),
          ServerGauge.SEGMENT_DOWNLOAD_COUNT.getGaugeName());

  @Test
  public void testBasicAcquireRelease()
      throws Exception {
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(4, 8, true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(4, 8, true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(4, 8, true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.availablePermits(), 4);
      Assert.assertEquals(operationsThrottler.totalPermits(), 4);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, 4);
      Assert.assertEquals(countGaugeValue, 0);

      operationsThrottler.acquire();
      Assert.assertEquals(operationsThrottler.availablePermits(), 3);
      Assert.assertEquals(operationsThrottler.totalPermits(), 4);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, 4);
      Assert.assertEquals(countGaugeValue, 1);

      operationsThrottler.release();
      Assert.assertEquals(operationsThrottler.availablePermits(), 4);
      Assert.assertEquals(operationsThrottler.totalPermits(), 4);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, 4);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(totalPermits, totalPermits * 2, true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(totalPermits, totalPermits * 2, true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(totalPermits, totalPermits * 2, true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), totalPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, totalPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < totalPermits; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.availablePermits(), totalPermits - j - 1);
        Assert.assertEquals(operationsThrottler.totalPermits(), totalPermits);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, totalPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      for (int j = 0; j < totalPermits; j++) {
        operationsThrottler.release();
        Assert.assertEquals(operationsThrottler.availablePermits(), j + 1);
        Assert.assertEquals(operationsThrottler.totalPermits(), totalPermits);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, totalPermits);
        Assert.assertEquals(countGaugeValue, totalPermits - j - 1);
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

    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(-1, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(0, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(1, -4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(1, 0, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(-1, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(0, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(1, -4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(1, 0, false));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM), Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM), Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM), Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES), true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      int defaultPermits = operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM)
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM)
              : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM);
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < 100; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermits);
        Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermits - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
    }
  }

  @Test
  public void testPositiveToNegativeThrottleChange() {
    int initialPermits = 2;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Change the value of cluster config for max segment operation parallelism to be a negative value
      // If config is <= 0, this is an invalid configuration change. Do nothing other than log a warning
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, "-1");
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < initialPermits; j++) {
        operationsThrottler.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), 0);

      // Increase the value of cluster config for max segment preprocess parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
          String.valueOf(initialPermits * 2));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, initialPermits);

      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);
      for (int j = 0; j < initialPermits; j++) {
        operationsThrottler.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
        Assert.assertEquals(countGaugeValue, initialPermits + j + 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);
      Assert.assertEquals(operationsThrottler.availablePermits(), 0);
      for (int j = 0; j < (initialPermits * 2); j++) {
        operationsThrottler.release();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
        Assert.assertEquals(countGaugeValue, (initialPermits * 2) - j - 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits * 2);
    }
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < initialPermits; j++) {
        operationsThrottler.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), 0);

      // Decrease the value of cluster config for max segment operation parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
          String.valueOf(initialPermits / 2));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits / 2);
      Assert.assertEquals(operationsThrottler.availablePermits(), -(initialPermits / 2));

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits / 2);
      Assert.assertEquals(countGaugeValue, initialPermits);

      for (int j = 0; j < initialPermits; j++) {
        operationsThrottler.release();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits / 2);
        Assert.assertEquals(countGaugeValue, initialPermits - j - 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits / 2);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits / 2);
    }
  }

  @Test
  public void testServingQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      int defaultPermitsBeforeQuery = operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? Integer.parseInt(
                  CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
              : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
      Assert.assertEquals(countGaugeValue, 0);

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      operationsThrottler.startServingQueries();
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireRelease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      int defaultPermitsBeforeQuery = operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
              : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Once the server is ready to serve queries, we should reset the throttling configurations to be as configured
      operationsThrottler.startServingQueries();
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.release();
        Assert.assertEquals(operationsThrottler.availablePermits(), (initialPermits - numPermitsToTake) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, numPermitsToTake - j - 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigIncrease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES) - 5, false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES) - 5, false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES) - 5, false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      int defaultPermitsBeforeQuery = operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
              : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery - 5);
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery - 5);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery - 5);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery - 5);
        Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery - j - 1 - 5);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery - 5);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Double the permits for before serving queries config
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(defaultPermitsBeforeQuery));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
      // We increased permits but took some before the increase
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      // Take more permits
      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(operationsThrottler.availablePermits(),
            defaultPermitsBeforeQuery - numPermitsToTake - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
        Assert.assertEquals(countGaugeValue, numPermitsToTake + j + 1);
      }

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      operationsThrottler.startServingQueries();
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits - (numPermitsToTake * 2));

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake * 2);

      for (int j = 0; j < numPermitsToTake * 2; j++) {
        operationsThrottler.release();
        Assert.assertEquals(operationsThrottler.availablePermits(), (initialPermits - numPermitsToTake * 2) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, (numPermitsToTake * 2) - j - 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigDecrease()
      throws InterruptedException {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES), false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, Integer.parseInt(
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES), false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      int defaultPermitsBeforeQuery = operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? Integer.parseInt(
              CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)
              : Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
      Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.acquire();
        Assert.assertEquals(operationsThrottler.totalPermits(), defaultPermitsBeforeQuery);
        Assert.assertEquals(operationsThrottler.availablePermits(), defaultPermitsBeforeQuery - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBeforeQuery);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Half the permits for before serving queries config
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      int newDefaultPermits = defaultPermitsBeforeQuery / 2;
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(newDefaultPermits));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), newDefaultPermits);
      // We doubled permits but took all of the previous ones
      Assert.assertEquals(operationsThrottler.availablePermits(), newDefaultPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newDefaultPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      operationsThrottler.startServingQueries();
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      for (int j = 0; j < numPermitsToTake; j++) {
        operationsThrottler.release();
        Assert.assertEquals(operationsThrottler.availablePermits(), (initialPermits - numPermitsToTake) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, numPermitsToTake - j - 1);
      }
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);
      Assert.assertEquals(operationsThrottler.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testThrowException()
      throws Exception {
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(1, 2, true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(1, 2, true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(1, 2, true));

    for (BaseSegmentOperationsThrottler operationsThrottler : segmentOperationsThrottlerList) {
      BaseSegmentOperationsThrottler spy = spy(operationsThrottler);
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
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Add some random configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmpty() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Set<String> keys = new HashSet<>();
      keys.add(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM);
      operationsThrottler.onChange(keys, null);

      int newTotalPermits = Integer.parseInt(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM);
      Assert.assertEquals(operationsThrottler.totalPermits(), newTotalPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newTotalPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmptyQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Set<String> keys = new HashSet<>();
      keys.add(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM);
      keys.add(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      operationsThrottler.onChange(keys, null);

      int newTotalPermits = Integer.parseInt(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
      Assert.assertEquals(operationsThrottler.totalPermits(), newTotalPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newTotalPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOtherThanRelevant() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Add some random configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
          ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM
          : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, "42");
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigs() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        true));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        true));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
          String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(initialPermits * 4));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = false, new total should match CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsWithServingQueriesDisabled() {
    int initialPermits = 4;
    List<BaseSegmentOperationsThrottler> segmentOperationsThrottlerList = new ArrayList<>();
    segmentOperationsThrottlerList.add(new SegmentAllIndexPreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentOperationsThrottlerList.add(new SegmentStarTreePreprocessThrottler(initialPermits, initialPermits * 2,
        false));
    segmentOperationsThrottlerList.add(new SegmentDownloadThrottler(initialPermits, initialPermits * 2,
        false));

    for (int i = 0; i < segmentOperationsThrottlerList.size(); i++) {
      BaseSegmentOperationsThrottler operationsThrottler = segmentOperationsThrottlerList.get(i);
      String thresholdGaugeName = _thresholdGauges.get(i);
      String countGaugeName = _countGauges.get(i);

      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 2);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
          String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(operationsThrottler instanceof SegmentAllIndexPreprocessThrottler
              ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
              : operationsThrottler instanceof SegmentStarTreePreprocessThrottler
                  ? CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
                  : CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
          String.valueOf(initialPermits * 4));
      operationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = false, new total should match higher threshold of before serving queries
      Assert.assertEquals(operationsThrottler.totalPermits(), initialPermits * 4);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 4);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOnSegmentPreprocessThrottler() {
    int initialPermits = 4;
    SegmentAllIndexPreprocessThrottler allIndexPreprocessThrottler = new SegmentAllIndexPreprocessThrottler(
        initialPermits, initialPermits * 2, true);
    SegmentStarTreePreprocessThrottler starTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(
        initialPermits, initialPermits * 2, true);
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler segmentOperationsThrottler = new SegmentOperationsThrottler(allIndexPreprocessThrottler,
        starTreePreprocessThrottler, downloadThrottler);

    Assert.assertEquals(segmentOperationsThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentDownloadThrottler().totalPermits(), initialPermits);

    for (String thresholdGaugeName : _thresholdGauges) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
    }

    for (String countGaugeName : _countGauges) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(countGaugeValue, 0);
    }

    // Add random and relevant configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
        String.valueOf(initialPermits * 2));
    segmentOperationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentDownloadThrottler().totalPermits(), initialPermits * 2);

    for (String thresholdGaugeName : _thresholdGauges) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
    }

    for (String countGaugeName : _countGauges) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOnSegmentPreprocessThrottlerQueriesDisabled() {
    int initialPermits = 4;
    SegmentAllIndexPreprocessThrottler allIndexPreprocessThrottler = new SegmentAllIndexPreprocessThrottler(
        initialPermits, initialPermits * 2, false);
    SegmentStarTreePreprocessThrottler starTreePreprocessThrottler = new SegmentStarTreePreprocessThrottler(
        initialPermits, initialPermits * 2, false);
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(
        initialPermits, initialPermits * 2, false);
    SegmentOperationsThrottler segmentOperationsThrottler = new SegmentOperationsThrottler(allIndexPreprocessThrottler,
        starTreePreprocessThrottler, downloadThrottler);

    Assert.assertEquals(segmentOperationsThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 2);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentDownloadThrottler().totalPermits(), initialPermits * 2);

    for (String thresholdGaugeName : _thresholdGauges) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
    }

    for (String countGaugeName : _countGauges) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(countGaugeValue, 0);
    }

    // Add random and relevant configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
        String.valueOf(initialPermits * 2));
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    updatedClusterConfigs.put(
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    updatedClusterConfigs.put(
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
        String.valueOf(initialPermits * 4));
    segmentOperationsThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentAllIndexPreprocessThrottler().totalPermits(),
        initialPermits * 4);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().totalPermits(),
        initialPermits * 4);
    Assert.assertEquals(segmentOperationsThrottler.getSegmentDownloadThrottler().totalPermits(), initialPermits * 4);

    for (String thresholdGaugeName : _thresholdGauges) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 4);
    }

    for (String countGaugeName : _countGauges) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }
}
