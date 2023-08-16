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
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Validation metrics utility class, which contains the glue code to publish metrics.
 */
public class ValidationMetrics {
  private static final double MILLIS_PER_HOUR = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);

  private final PinotMetricsRegistry _metricsRegistry;
  private final Map<String, Long> _gaugeValues = new HashMap<>();
  private final Set<PinotMetricName> _metricNames = new HashSet<>();

  /**
   * A simple gauge that returns whatever last value was stored in the _gaugeValues hash map.
   */
  private class StoredValueGauge implements PinotGauge<Long> {
    private final String _key;

    public StoredValueGauge(String key) {
      _key = key;
    }

    @Override
    public Long value() {
      return _gaugeValues.get(_key);
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
    private final String _key;

    public CurrentTimeMillisDeltaGaugeHours(String key) {
      _key = key;
    }

    @Override
    public Double value() {
      Long gaugeValue = _gaugeValues.get(_key);

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
    makeGauge(resource, ValidationMetricName.MISSING_SEGMENT_COUNT, _storedValueGaugeFactory, missingSegmentCount);
  }

  /**
   * Cleans up the missing segment count gauge.
   *
   * @param resource The resource for which the gauge is removed
   */
  public void cleanupMissingSegmentCountGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.MISSING_SEGMENT_COUNT);
  }

  /**
   * Updates the offline segment delay gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param lastOfflineSegmentTime The last offline segment end time, in milliseconds since the epoch, or Long.MIN_VALUE
   *                               if there is no such time.
   */
  public void updateOfflineSegmentDelayGauge(final String resource, final long lastOfflineSegmentTime) {
    makeGauge(resource, ValidationMetricName.OFFLINE_SEGMENT_DELAY_HOURS, _currentTimeMillisDeltaGaugeHoursFactory,
        lastOfflineSegmentTime);
  }

  /**
   * Cleans up offline segment delay gauge.
   *
   * @param resource The resource for which the gauge is removed
   */
  public void cleanupOfflineSegmentDelayGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.OFFLINE_SEGMENT_DELAY_HOURS);
  }

  /**
   * Updates the last push time gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param lastPushTimeMillis The last push time, in milliseconds since the epoch, or Long.MIN_VALUE if there is no
   *                           such time.
   */
  public void updateLastPushTimeGauge(final String resource, final long lastPushTimeMillis) {
    makeGauge(resource, ValidationMetricName.LAST_PUSH_TIME_DELAY_HOURS, _currentTimeMillisDeltaGaugeHoursFactory,
        lastPushTimeMillis);
  }

  /**
   * Cleans up the last push time gauge.
   *
   * @param resource The resource for which the gauge is removed
   */
  public void cleanupLastPushTimeGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.LAST_PUSH_TIME_DELAY_HOURS);
  }

  /**
   * Updates the total document count gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param documentCount Total document count for the given resource name or table name
   */
  public void updateTotalDocumentCountGauge(final String resource, final long documentCount) {
    makeGauge(resource, ValidationMetricName.TOTAL_DOCUMENT_COUNT, _storedValueGaugeFactory, documentCount);
  }

  /**
   * Cleans up the total document count gauge.
   *
   * @param resource The resource for which the gauge is removed
   */
  public void cleanupTotalDocumentCountGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.TOTAL_DOCUMENT_COUNT);
  }

  /**
   * Updates the non consuming partition count metric.
   *
   * @param resource The resource for which the gauge is updated
   * @param partitionCount Number of partitions that do not have any segment in CONSUMING state.
   */
  public void updateNonConsumingPartitionCountMetric(final String resource, final int partitionCount) {
    makeGauge(resource, ValidationMetricName.NON_CONSUMING_PARTITION_COUNT, _storedValueGaugeFactory, partitionCount);
  }

  /**
   * Updates the segment count gauge.
   *
   * @param resource The resource for which the gauge is updated
   * @param segmentCount Total segment count for the given resource name or table name
   */
  public void updateSegmentCountGauge(final String resource, final long segmentCount) {
    makeGauge(resource, ValidationMetricName.SEGMENT_COUNT, _storedValueGaugeFactory, segmentCount);
  }

  /**
   * Cleans up the segment count gauge.
   *
   * @param resource The resource for which the gauge is removed
   */
  public void cleanupSegmentCountGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.SEGMENT_COUNT);
  }

  public void updateTmpSegCountGauge(final String resource, final long tmpSegmentCount) {
    makeGauge(resource, ValidationMetricName.DELETED_TMP_SEGMENT_COUNT, _storedValueGaugeFactory, tmpSegmentCount);
  }

  public void cleanupTmpSegCountGauge(final String resource) {
    removeGauge(resource, ValidationMetricName.DELETED_TMP_SEGMENT_COUNT);
  }

  @VisibleForTesting
  public static String makeGaugeName(final String resource, final String gaugeName) {
    return "pinot.controller." + resource + "." + gaugeName;
  }

  private PinotMetricName makeMetricName(final String gaugeName) {
    return PinotMetricUtils.makePinotMetricName(ValidationMetrics.class, gaugeName);
  }

  private void makeGauge(final String resource, final ValidationMetricName validationMetricName,
      final GaugeFactory<?> gaugeFactory, final long value) {
    final String fullGaugeName = makeGaugeName(resource, validationMetricName.getMetricName());
    PinotMetricName metricName = makeMetricName(fullGaugeName);
    if (!_gaugeValues.containsKey(fullGaugeName)) {
      _gaugeValues.put(fullGaugeName, value);
      PinotMetricUtils.makeGauge(_metricsRegistry, metricName, gaugeFactory.buildGauge(fullGaugeName));
      _metricNames.add(metricName);
    } else {
      _gaugeValues.put(fullGaugeName, value);
    }
  }

  private void removeGauge(final String resource, final ValidationMetricName validationMetricName) {
    final String fullGaugeName = makeGaugeName(resource, validationMetricName.getMetricName());
    PinotMetricName pinotMetricName = makeMetricName(fullGaugeName);
    PinotMetricUtils.removeMetric(_metricsRegistry, pinotMetricName);
    _metricNames.remove(pinotMetricName);
    _gaugeValues.remove(fullGaugeName);
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
    Long value = _gaugeValues.get(fullGaugeName);
    if (value == null) {
      return 0;
    }
    return value;
  }

  /**
   * Names of validation metrics.
   */
  public enum ValidationMetricName {
    MISSING_SEGMENT_COUNT("missingSegmentCount"),
    OFFLINE_SEGMENT_DELAY_HOURS("offlineSegmentDelayHours"),
    LAST_PUSH_TIME_DELAY_HOURS("lastPushTimeDelayHours"),
    TOTAL_DOCUMENT_COUNT("TotalDocumentCount"),
    NON_CONSUMING_PARTITION_COUNT("NonConsumingPartitionCount"),
    SEGMENT_COUNT("SegmentCount"),
    DELETED_TMP_SEGMENT_COUNT("DeletedTmpSegmentCount");

    private final String _metricName;

    ValidationMetricName(String metricName) {
      _metricName = metricName;
    }

    public String getMetricName() {
      return _metricName;
    }
  }
}
