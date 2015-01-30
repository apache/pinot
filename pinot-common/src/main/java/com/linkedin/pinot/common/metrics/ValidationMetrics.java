package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.HashMap;
import java.util.Map;


/**
 * Validation metrics utility class, which contains the glue code to publish metrics.
 *
 * @author jfim
 */
public class ValidationMetrics {
  private final MetricsRegistry _metricsRegistry;

  private final Map<String, Long> gaugeValues = new HashMap<String, Long>();

  /**
   * A simple gauge that returns whatever last value was stored in the gaugeValues hash map.
   */
  private class StoredValueGauge extends Gauge<Long> {
    private final String key;

    public StoredValueGauge(String key) {
      this.key = key;
    }

    @Override
    public Long value() {
      return gaugeValues.get(key);
    }
  }

  /**
   * A simple gauge that returns the difference between the current system time in millis and the value stored in the
   * gaugeValues hash map.
   */
  private class CurrentTimeMillisDeltaGauge extends Gauge<Long> {
    private final String key;

    public CurrentTimeMillisDeltaGauge(String key) {
      this.key = key;
    }

    @Override
    public Long value() {
      Long gaugeValue = gaugeValues.get(key);

      if (gaugeValue != null && gaugeValue != Long.MIN_VALUE)
        return System.currentTimeMillis() - gaugeValue;
      else
        return Long.MIN_VALUE;
    }
  }
  
  private interface GaugeFactory<T> {
    public Gauge<T> buildGauge(final String key);
  }
  
  private class StoredValueGaugeFactory implements GaugeFactory<Long> {
    @Override
    public Gauge<Long> buildGauge(final String key) {
      return new StoredValueGauge(key);
    }
  }
  
  private class CurrentTimeMillisDeltaGaugeFactory implements GaugeFactory<Long> {
    @Override
    public Gauge<Long> buildGauge(final String key) {
      return new CurrentTimeMillisDeltaGauge(key);
    }
  }

  private final StoredValueGaugeFactory _storedValueGaugeFactory = new StoredValueGaugeFactory();
  private final CurrentTimeMillisDeltaGaugeFactory _currentTimeMillisDeltaGaugeFactory = new CurrentTimeMillisDeltaGaugeFactory();

  /**
   * Builds the validation metrics.
   *
   * @param metricsRegistry The metrics registry used to store all the gauges.
   */
  public ValidationMetrics(MetricsRegistry metricsRegistry) {
    _metricsRegistry = metricsRegistry;
  }

  /**
   * Updates the gauge for the number of missing segments.
   *
   * @param resource The resource for which the gauge is updated
   * @param tableName The table name for which the gauge is updated
   * @param missingSegmentCount The number of missing segments
   */
  public void updateMissingSegmentsGauge(final String resource, final String tableName, final int missingSegmentCount) {
    final String fullGaugeName = makeGaugeName(resource, tableName, "missingSegmentCount");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _storedValueGaugeFactory, missingSegmentCount);
  }

  /**
   * Updates the gauge for the offline segment delay.
   *
   * @param resource The resource for which the gauge is updated
   * @param tableName The table name for which the gauge is updated
   * @param lastOfflineSegmentTime The last offline segment end time, in milliseconds since the epoch, or Long.MIN_VALUE
   *                               if there is no such time.
   */
  public void updateOfflineSegmentDelayGauge(final String resource, final String tableName, final long lastOfflineSegmentTime) {
    final String fullGaugeName = makeGaugeName(resource, tableName, "offlineSegmentDelayMillis");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _currentTimeMillisDeltaGaugeFactory, lastOfflineSegmentTime);
  }

  /**
   * Updates the gauge for the last push time.
   *
   * @param resource The resource for which the gauge is updated
   * @param tableName The table name for which the gauge is updated
   * @param lastPushTimeMillis The last push time, in milliseconds since the epoch, or Long.MIN_VALUE if there is no
   *                           such time.
   */
  public void updateLastPushTimeGauge(final String resource, final String tableName, final long lastPushTimeMillis) {
    final String fullGaugeName = makeGaugeName(resource, tableName, "lastPushTimeDelayMillis");
    makeGauge(fullGaugeName, makeMetricName(fullGaugeName), _currentTimeMillisDeltaGaugeFactory, lastPushTimeMillis);
  }
  
  private String makeGaugeName(final String resource, final String tableName, final String gaugeName) {
    return "pinot.controller." + resource + "." + tableName + "." + gaugeName; 
  }
  
  private MetricName makeMetricName(final String gaugeName) {
    return new MetricName(ValidationMetrics.class, gaugeName);
  }
  
  private void makeGauge(final String gaugeName, final MetricName metricName, final GaugeFactory<Long> gaugeFactory, final long value) {
    if (!gaugeValues.containsKey(gaugeName)) {
      gaugeValues.put(gaugeName, value);
      MetricsHelper.newGauge(_metricsRegistry, metricName, gaugeFactory.buildGauge(gaugeName));
    } else {
      gaugeValues.put(gaugeName, value);
    }
  }
}
