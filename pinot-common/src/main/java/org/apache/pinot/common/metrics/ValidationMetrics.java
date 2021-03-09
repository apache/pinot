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
package org.apache.pinot.common.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Validation metrics utility class, which contains the glue code to publish metrics.
 */
public class ValidationMetrics {
  private final PinotMetricsRegistry _metricsRegistry;
  private final Map<String, Long> _gaugeValues = new HashMap<>();
  private final Set<PinotMetricName> _metricNames = new HashSet<>();

  /**
   * A simple gauge that returns whatever last value was stored in the _gaugeValues hash map.
   */
  private class StoredValueGauge implements PinotGauge<Long> {
    private final String key;

    public StoredValueGauge(String key) {
      this.key = key;
    }

    @Override
    public Long value() {
      return _gaugeValues.get(key);
    }

    @Override
    public Object getGauge() {
      return PinotMetricUtils.makePinotGauge(avoid -> value()).getGauge();
    }

    @Override
    public Object getMetric() {
      return getGauge();
    }
  }

  /**
   * A simple gauge that returns the difference in hours between the current system time and the value stored in the
   * _gaugeValues hash map.
   */
  private class CurrentTimeMillisDeltaGaugeHours implements PinotGauge<Double> {
    private final String key;

    private final double MILLIS_PER_HOUR = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);

    public CurrentTimeMillisDeltaGaugeHours(String key) {
      this.key = key;
    }

    @Override
    public Double value() {
      Long gaugeValue = _gaugeValues.get(key);

      if (gaugeValue != null && gaugeValue != Long.MIN_VALUE) {
        return (System.currentTimeMillis() - gaugeValue) / MILLIS_PER_HOUR;
      } else {
        return Double.MIN_VALUE;
      }
    }

    @Override
    public Object getMetric() {
      return getGauge();
    }

    @Override
    public Object getGauge() {
      return PinotMetricUtils.makePinotGauge(avoid -> value()).getGauge();
    }
  }

  private interface GaugeFactory<T> {
    PinotGauge<T> buildGauge(final String key);
  }

  private class StoredValueGaugeFactory implements GaugeFactory<Long> {
    @Override
    public PinotGauge<Long> buildGauge(final String key) {
      return new StoredValueGauge(key);
    }
  }

  private class CurrentTimeMillisDeltaGaugeHoursFactory implements GaugeFactory<Double> {
    @Override
    public PinotGauge<Double> buildGauge(final String key) {
      return new CurrentTimeMillisDeltaGaugeHours(key);
    }
  }

  private final StoredValueGaugeFactory _storedValueGaugeFactory = new StoredValueGaugeFactory();
  private final CurrentTimeMillisDeltaGaugeHoursFactory _currentTimeMillisDeltaGaugeHoursFactory =
      new CurrentTimeMillisDeltaGaugeHoursFactory();

  /**
   * Builds the validation metrics.
   *
   * @param metricsRegistry The metrics registry used to store all the gauges.
   */
  public ValidationMetrics(PinotMetricsRegistry metricsRegistry) {
    _metricsRegistry = metricsRegistry;
  }

  /**
   * Updates the missing segment count gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param missingSegmentCount The number of missing segments
   */
  public void updateMissingSegmentCountGauge(final String resource, final int missingSegmentCount) {
    final String fullGaugeName = makeGaugeName(resource, "missingSegmentCount");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _storedValueGaugeFactory, missingSegmentCount);
  }

  /**
   * Updates the offline segment delay gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param lastOfflineSegmentTime The last offline segment end time, in milliseconds since the epoch, or Long.MIN_VALUE
   *                               if there is no such time.
   */
  public void updateOfflineSegmentDelayGauge(final String resource, final long lastOfflineSegmentTime) {
    final String fullGaugeNameHours = makeGaugeName(resource, "offlineSegmentDelayHours");
    makeGauge(fullGaugeNameHours, makeMetricName(fullGaugeNameHours), _currentTimeMillisDeltaGaugeHoursFactory,
        lastOfflineSegmentTime);
  }

  /**
   * Updates the last push time gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param lastPushTimeMillis The last push time, in milliseconds since the epoch, or Long.MIN_VALUE if there is no
   *                           such time.
   */
  public void updateLastPushTimeGauge(final String resource, final long lastPushTimeMillis) {
    final String fullGaugeNameHours = makeGaugeName(resource, "lastPushTimeDelayHours");
    makeGauge(fullGaugeNameHours, makeMetricName(fullGaugeNameHours), _currentTimeMillisDeltaGaugeHoursFactory,
        lastPushTimeMillis);
  }

  /**
   * Updates the total document count gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param documentCount Total document count for the given resource name or table name
   */
  public void updateTotalDocumentCountGauge(final String resource, final long documentCount) {
    final String fullGaugeName = makeGaugeName(resource, "TotalDocumentCount");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _storedValueGaugeFactory, documentCount);
  }

  /**
   * Updates the non consuming partition count metric.
   *
   * @param resource The resource for which the gauge is updated
   * @param partitionCount Number of partitions that do not have any segment in CONSUMING state.
   */
  public void updateNonConsumingPartitionCountMetric(final String resource, final int partitionCount) {
    final String fullGaugeName = makeGaugeName(resource, "NonConsumingPartitionCount");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _storedValueGaugeFactory, partitionCount);
  }

  /**
   * Updates the segment count gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param segmentCount Total segment count for the given resource name or table name
   */
  public void updateSegmentCountGauge(final String resource, final long segmentCount) {
    final String fullGaugeName = makeGaugeName(resource, "SegmentCount");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _storedValueGaugeFactory, segmentCount);
  }

  @VisibleForTesting
  public static String makeGaugeName(final String resource, final String gaugeName) {
    return "pinot.controller." + resource + "." + gaugeName;
  }

  private PinotMetricName makeMetricName(final String gaugeName) {
    return PinotMetricUtils.makePinotMetricName(ValidationMetrics.class, gaugeName);
  }

  private void makeGauge(final String gaugeName, final PinotMetricName metricName, final GaugeFactory<?> gaugeFactory,
      final long value) {
    if (!_gaugeValues.containsKey(gaugeName)) {
      _gaugeValues.put(gaugeName, value);
      PinotMetricUtils.makeGauge(_metricsRegistry, metricName, gaugeFactory.buildGauge(gaugeName));
      _metricNames.add(metricName);
    } else {
      _gaugeValues.put(gaugeName, value);
    }
  }

  /**
   * Unregisters all validation metrics.
   */
  public void unregisterAllMetrics() {
    for (PinotMetricName metricName : _metricNames) {
      PinotMetricUtils.removeMetric(_metricsRegistry, metricName);
    }

    _metricNames.clear();
    _gaugeValues.clear();
  }

  @VisibleForTesting
  public long getValueOfGauge(final String fullGaugeName) {
    Long value  = _gaugeValues.get(fullGaugeName);
    if (value == null) {
      return 0;
    }
    return value;
  }
}
