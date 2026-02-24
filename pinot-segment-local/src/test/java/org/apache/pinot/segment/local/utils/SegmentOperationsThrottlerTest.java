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
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class SegmentOperationsThrottlerTest {

  private static final int NUM_THROTTLERS = 4;

  private static final ServerGauge[] THRESHOLD_GAUGES = {
      ServerGauge.SEGMENT_ALL_PREPROCESS_THROTTLE_THRESHOLD,
      ServerGauge.SEGMENT_STARTREE_PREPROCESS_THROTTLE_THRESHOLD,
      ServerGauge.SEGMENT_DOWNLOAD_THROTTLE_THRESHOLD,
      ServerGauge.SEGMENT_MULTI_COL_TEXT_INDEX_PREPROCESS_THROTTLE_THRESHOLD
  };

  private static final ServerGauge[] COUNT_GAUGES = {
      ServerGauge.SEGMENT_ALL_PREPROCESS_COUNT,
      ServerGauge.SEGMENT_STARTREE_PREPROCESS_COUNT,
      ServerGauge.SEGMENT_DOWNLOAD_COUNT,
      ServerGauge.SEGMENT_MULTI_COL_TEXT_INDEX_PREPROCESS_COUNT
  };

  private static final String[] PARALLELISM_KEYS = {
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM
  };

  private static final String[] PARALLELISM_BEFORE_SERVING_KEYS = {
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
  };

  private static final String[] DEFAULT_PARALLELISM = {
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM
  };

  private static final String[] DEFAULT_PARALLELISM_BEFORE_SERVING = {
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
      CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES
  };

  private final ServerMetrics _serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  @BeforeClass
  public void setup() {
    ServerMetrics.deregister();
    ServerMetrics.register(_serverMetrics);
  }

  @AfterClass
  public void tearDown() {
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
  }

  /**
   * Creates an array of 4 BaseSegmentOperationsThrottler instances, each configured with the appropriate
   * gauge params so that metrics are emitted correctly.
   */
  private BaseSegmentOperationsThrottler[] createThrottlers(int maxConcurrency, int maxConcurrencyBeforeServing,
      boolean isServing) {
    BaseSegmentOperationsThrottler[] throttlers = new BaseSegmentOperationsThrottler[NUM_THROTTLERS];
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      throttlers[i] = new BaseSegmentOperationsThrottler(maxConcurrency, maxConcurrencyBeforeServing, isServing,
          THRESHOLD_GAUGES[i], COUNT_GAUGES[i], "throttler-" + i);
    }
    return throttlers;
  }

  /**
   * Creates a SegmentOperationsThrottler wrapping the given array of 4 throttlers.
   */
  private SegmentOperationsThrottler wrapInSegmentOperationsThrottler(BaseSegmentOperationsThrottler[] throttlers) {
    return new SegmentOperationsThrottler(throttlers[0], throttlers[1], throttlers[2], throttlers[3]);
  }

  @Test
  public void testBasicAcquireRelease()
      throws Exception {
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(4, 8, true);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.availablePermits(), 4);
      Assert.assertEquals(t.totalPermits(), 4);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, 4);
      Assert.assertEquals(countGaugeValue, 0);

      t.acquire();
      Assert.assertEquals(t.availablePermits(), 3);
      Assert.assertEquals(t.totalPermits(), 4);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, 4);
      Assert.assertEquals(countGaugeValue, 1);

      t.release();
      Assert.assertEquals(t.availablePermits(), 4);
      Assert.assertEquals(t.totalPermits(), 4);

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
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(totalPermits, totalPermits * 2, true);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), totalPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, totalPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < totalPermits; j++) {
        t.acquire();
        Assert.assertEquals(t.availablePermits(), totalPermits - j - 1);
        Assert.assertEquals(t.totalPermits(), totalPermits);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, totalPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      for (int j = 0; j < totalPermits; j++) {
        t.release();
        Assert.assertEquals(t.availablePermits(), j + 1);
        Assert.assertEquals(t.totalPermits(), totalPermits);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, totalPermits);
        Assert.assertEquals(countGaugeValue, totalPermits - j - 1);
      }
    }
  }

  @Test
  public void testThrowExceptionOnSettingInvalidConfigValues() {
    // All invalid constructor args should throw IllegalArgumentException regardless of isServingQueries
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(-1, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(0, 4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(1, -4, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(1, 0, true));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(-1, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(0, 4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(1, -4, false));
    Assert.assertThrows(IllegalArgumentException.class, () -> new BaseSegmentOperationsThrottler(1, 0, false));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      int defaultPermits = Integer.parseInt(DEFAULT_PARALLELISM[i]);
      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      BaseSegmentOperationsThrottler t = new BaseSegmentOperationsThrottler(defaultPermits, defaultPermitsBefore, true,
          THRESHOLD_GAUGES[i], COUNT_GAUGES[i], "throttler-" + i);

      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), defaultPermits);
      Assert.assertEquals(t.availablePermits(), defaultPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < 100; j++) {
        t.acquire();
        Assert.assertEquals(t.totalPermits(), defaultPermits);
        Assert.assertEquals(t.availablePermits(), defaultPermits - j - 1);

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
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Change the value of cluster config for max segment operation parallelism to be a negative value.
      // If config is <= 0, this is an invalid configuration change. The throttler should not be updated.
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], "-1");
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);

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
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < initialPermits; j++) {
        t.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), 0);

      // Increase the value of cluster config for max segment preprocess parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(initialPermits * 4));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(t.totalPermits(), initialPermits * 2);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, initialPermits);

      Assert.assertEquals(t.availablePermits(), initialPermits);
      for (int j = 0; j < initialPermits; j++) {
        t.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
        Assert.assertEquals(countGaugeValue, initialPermits + j + 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits * 2);
      Assert.assertEquals(t.availablePermits(), 0);
      for (int j = 0; j < (initialPermits * 2); j++) {
        t.release();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
        Assert.assertEquals(countGaugeValue, (initialPermits * 2) - j - 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits * 2);
      Assert.assertEquals(t.availablePermits(), initialPermits * 2);
    }
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < initialPermits; j++) {
        t.acquire();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, j + 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), 0);

      // Decrease the value of cluster config for max segment operation parallelism
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits / 2));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(initialPermits));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(t.totalPermits(), initialPermits / 2);
      Assert.assertEquals(t.availablePermits(), -(initialPermits / 2));

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits / 2);
      Assert.assertEquals(countGaugeValue, initialPermits);

      for (int j = 0; j < initialPermits; j++) {
        t.release();

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits / 2);
        Assert.assertEquals(countGaugeValue, initialPermits - j - 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits / 2);
      Assert.assertEquals(t.availablePermits(), initialPermits / 2);
    }
  }

  @Test
  public void testServingQueriesDisabled() {
    int initialPermits = 4;
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      BaseSegmentOperationsThrottler t = new BaseSegmentOperationsThrottler(initialPermits, defaultPermitsBefore,
          false, THRESHOLD_GAUGES[i], COUNT_GAUGES[i], "throttler-" + i);
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      // We set isServingQueries to false when the server is not yet ready to serve queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
      Assert.assertEquals(t.availablePermits(), defaultPermitsBefore);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
      Assert.assertEquals(countGaugeValue, 0);

      // Once the server is ready to serve queries, we should reset the throttling configurations to be as configured
      t.startServingQueries();
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);

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
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      BaseSegmentOperationsThrottler t = new BaseSegmentOperationsThrottler(initialPermits, defaultPermitsBefore,
          false, THRESHOLD_GAUGES[i], COUNT_GAUGES[i], "throttler-" + i);
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
      Assert.assertEquals(t.availablePermits(), defaultPermitsBefore);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        t.acquire();
        Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
        Assert.assertEquals(t.availablePermits(), defaultPermitsBefore - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Once the server is ready to serve queries, we should reset the throttling configurations to be as configured
      t.startServingQueries();
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      for (int j = 0; j < numPermitsToTake; j++) {
        t.release();
        Assert.assertEquals(t.availablePermits(), (initialPermits - numPermitsToTake) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, numPermitsToTake - j - 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigIncrease()
      throws InterruptedException {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = new BaseSegmentOperationsThrottler[NUM_THROTTLERS];
    for (int k = 0; k < NUM_THROTTLERS; k++) {
      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[k]);
      int reducedPermitsBefore = defaultPermitsBefore - 5;
      throttlers[k] = new BaseSegmentOperationsThrottler(initialPermits, reducedPermitsBefore, false,
          THRESHOLD_GAUGES[k], COUNT_GAUGES[k], "throttler-" + k);
    }
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      int reducedPermitsBefore = defaultPermitsBefore - 5;
      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(t.totalPermits(), reducedPermitsBefore);
      Assert.assertEquals(t.availablePermits(), reducedPermitsBefore);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, reducedPermitsBefore);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        t.acquire();
        Assert.assertEquals(t.totalPermits(), reducedPermitsBefore);
        Assert.assertEquals(t.availablePermits(), reducedPermitsBefore - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, reducedPermitsBefore);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Increase the permits for before serving queries config via SegmentOperationsThrottler onChange.
      // Include both keys so maxConcurrency stays at initialPermits for later startServingQueries.
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(defaultPermitsBefore));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
      // We increased permits but took some before the increase
      Assert.assertEquals(t.availablePermits(), defaultPermitsBefore - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      // Take more permits
      for (int j = 0; j < numPermitsToTake; j++) {
        t.acquire();
        Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
        Assert.assertEquals(t.availablePermits(), defaultPermitsBefore - numPermitsToTake - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
        Assert.assertEquals(countGaugeValue, numPermitsToTake + j + 1);
      }

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      t.startServingQueries();
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits - (numPermitsToTake * 2));

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake * 2);

      for (int j = 0; j < numPermitsToTake * 2; j++) {
        t.release();
        Assert.assertEquals(t.availablePermits(), (initialPermits - numPermitsToTake * 2) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, (numPermitsToTake * 2) - j - 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testServingQueriesDisabledWithAcquireReleaseWithConfigDecrease()
      throws InterruptedException {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = new BaseSegmentOperationsThrottler[NUM_THROTTLERS];
    for (int k = 0; k < NUM_THROTTLERS; k++) {
      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[k]);
      throttlers[k] = new BaseSegmentOperationsThrottler(initialPermits, defaultPermitsBefore, false,
          THRESHOLD_GAUGES[k], COUNT_GAUGES[k], "throttler-" + k);
    }
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      int defaultPermitsBefore = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      // Default is too high: Integer.MAX_VALUE, take a limited number of permits so that the test doesn't take too
      // long to finish
      int numPermitsToTake = 10000;
      // We set isServingQueries to false when the server is not yet ready to server queries. In this scenario ideally
      // preprocessing more segments is acceptable and cannot affect the query performance
      Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
      Assert.assertEquals(t.availablePermits(), defaultPermitsBefore);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
      Assert.assertEquals(countGaugeValue, 0);

      for (int j = 0; j < numPermitsToTake; j++) {
        t.acquire();
        Assert.assertEquals(t.totalPermits(), defaultPermitsBefore);
        Assert.assertEquals(t.availablePermits(), defaultPermitsBefore - j - 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, defaultPermitsBefore);
        Assert.assertEquals(countGaugeValue, j + 1);
      }

      // Half the permits for before serving queries config. Include both keys to preserve maxConcurrency.
      int newDefaultPermits = defaultPermitsBefore / 2;
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(newDefaultPermits));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(t.totalPermits(), newDefaultPermits);
      Assert.assertEquals(t.availablePermits(), newDefaultPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newDefaultPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      // Once the server is ready to server queries, we should reset the throttling configurations to be as configured
      t.startServingQueries();
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits - numPermitsToTake);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, numPermitsToTake);

      for (int j = 0; j < numPermitsToTake; j++) {
        t.release();
        Assert.assertEquals(t.availablePermits(), (initialPermits - numPermitsToTake) + j + 1);

        thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
        countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
        Assert.assertEquals(thresholdGaugeValue, initialPermits);
        Assert.assertEquals(countGaugeValue, numPermitsToTake - j - 1);
      }
      Assert.assertEquals(t.totalPermits(), initialPermits);
      Assert.assertEquals(t.availablePermits(), initialPermits);
    }
  }

  @Test
  public void testThrowException()
      throws Exception {
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(1, 2, true);

    for (BaseSegmentOperationsThrottler t : throttlers) {
      BaseSegmentOperationsThrottler spyThrottler = spy(t);
      spyThrottler.acquire();
      Assert.assertEquals(spyThrottler.availablePermits(), 0);
      doThrow(new InterruptedException("interrupt")).when(spyThrottler).acquire();

      Assert.assertThrows(InterruptedException.class, spyThrottler::acquire);
      Assert.assertEquals(spyThrottler.availablePermits(), 0);
      spyThrottler.release();
      Assert.assertEquals(spyThrottler.availablePermits(), 1);
    }
  }

  @Test
  public void testChangeConfigsEmpty() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Pass empty configs to onChange - should be a no-op
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      Assert.assertEquals(t.totalPermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmpty() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Map<String, String> changedKeys = new HashMap<>();
      changedKeys.put(PARALLELISM_KEYS[i], null);
      sot.onChange(changedKeys.keySet(), null);

      int newTotalPermits = Integer.parseInt(DEFAULT_PARALLELISM[i]);
      Assert.assertEquals(t.totalPermits(), newTotalPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newTotalPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigDeletedConfigsEmptyQueriesDisabled() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, false);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits * 2);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);

      // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
      Map<String, String> changedKeys = new HashMap<>();
      changedKeys.put(PARALLELISM_KEYS[i], null);
      changedKeys.put(PARALLELISM_BEFORE_SERVING_KEYS[i], null);
      sot.onChange(changedKeys.keySet(), null);

      int newTotalPermits = Integer.parseInt(DEFAULT_PARALLELISM_BEFORE_SERVING[i]);
      Assert.assertEquals(t.totalPermits(), newTotalPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, newTotalPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOtherThanRelevant() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 1; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Add configs for a different throttler (not throttler[i]) and a random key
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      int otherIndex = (i - 1) % NUM_THROTTLERS;
      updatedClusterConfigs.put(PARALLELISM_KEYS[otherIndex], "42");
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

      // Throttler[i] should be unchanged since its config keys were not in the changed set
      Assert.assertEquals(t.totalPermits(), initialPermits);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigs() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
      Assert.assertEquals(countGaugeValue, 0);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(initialPermits * 4));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = true, new total should match CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM
      Assert.assertEquals(t.totalPermits(), initialPermits * 2);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsWithServingQueriesDisabled() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, false);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      BaseSegmentOperationsThrottler t = throttlers[i];
      String thresholdGaugeName = THRESHOLD_GAUGES[i].getGaugeName();
      String countGaugeName = COUNT_GAUGES[i].getGaugeName();

      Assert.assertEquals(t.totalPermits(), initialPermits * 2);

      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
      Assert.assertEquals(countGaugeValue, 0);

      // Add random and relevant configs and call 'onChange'
      Map<String, String> updatedClusterConfigs = new HashMap<>();
      updatedClusterConfigs.put("random.config.key", "random.config.value");
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(initialPermits * 4));
      sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
      // Since isServingQueries = false, new total should match higher threshold of before serving queries
      Assert.assertEquals(t.totalPermits(), initialPermits * 4);

      thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGaugeName);
      countGaugeValue = _serverMetrics.getGaugeValue(countGaugeName);
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 4);
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOnSegmentOperationsThrottler() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, true);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      Assert.assertEquals(throttlers[i].totalPermits(), initialPermits);
    }

    for (ServerGauge thresholdGauge : THRESHOLD_GAUGES) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGauge.getGaugeName());
      Assert.assertEquals(thresholdGaugeValue, initialPermits);
    }

    for (ServerGauge countGauge : COUNT_GAUGES) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGauge.getGaugeName());
      Assert.assertEquals(countGaugeValue, 0);
    }

    // Add random and relevant configs for ALL throttlers and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits * 2));
    }
    sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      Assert.assertEquals(throttlers[i].totalPermits(), initialPermits * 2);
    }

    for (ServerGauge thresholdGauge : THRESHOLD_GAUGES) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGauge.getGaugeName());
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
    }

    for (ServerGauge countGauge : COUNT_GAUGES) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGauge.getGaugeName());
      Assert.assertEquals(countGaugeValue, 0);
    }
  }

  @Test
  public void testChangeConfigsOnSegmentOperationsThrottlerQueriesDisabled() {
    int initialPermits = 4;
    BaseSegmentOperationsThrottler[] throttlers = createThrottlers(initialPermits, initialPermits * 2, false);
    SegmentOperationsThrottler sot = wrapInSegmentOperationsThrottler(throttlers);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      Assert.assertEquals(throttlers[i].totalPermits(), initialPermits * 2);
    }

    for (ServerGauge thresholdGauge : THRESHOLD_GAUGES) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGauge.getGaugeName());
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 2);
    }

    for (ServerGauge countGauge : COUNT_GAUGES) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGauge.getGaugeName());
      Assert.assertEquals(countGaugeValue, 0);
    }

    // Add random and relevant configs for ALL throttlers and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    for (int i = 0; i < NUM_THROTTLERS; i++) {
      updatedClusterConfigs.put(PARALLELISM_KEYS[i], String.valueOf(initialPermits * 2));
      updatedClusterConfigs.put(PARALLELISM_BEFORE_SERVING_KEYS[i], String.valueOf(initialPermits * 4));
    }

    sot.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    for (int i = 0; i < NUM_THROTTLERS; i++) {
      Assert.assertEquals(throttlers[i].totalPermits(), initialPermits * 4);
    }

    for (ServerGauge thresholdGauge : THRESHOLD_GAUGES) {
      Long thresholdGaugeValue = _serverMetrics.getGaugeValue(thresholdGauge.getGaugeName());
      Assert.assertEquals(thresholdGaugeValue, initialPermits * 4);
    }

    for (ServerGauge countGauge : COUNT_GAUGES) {
      Long countGaugeValue = _serverMetrics.getGaugeValue(countGauge.getGaugeName());
      Assert.assertEquals(countGaugeValue, 0);
    }
  }
}
