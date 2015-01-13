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
      return System.currentTimeMillis() - gaugeValues.get(key);
    }
  }

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
    final String fullGaugeName = "pinot.controller." + resource + "." + tableName + ".missingSegmentCount";
    final MetricName metricName = new MetricName(ValidationMetrics.class, fullGaugeName);
    final boolean needToCreateNewGauge = !gaugeValues.containsKey(fullGaugeName);

    gaugeValues.put(fullGaugeName, (long) missingSegmentCount);

    if (needToCreateNewGauge) {
      MetricsHelper.newGauge(_metricsRegistry, metricName, new StoredValueGauge(fullGaugeName));
    }
  }

  /**
   * Updates the gauge for the offline segment delay.
   *
   * @param resource The resource for which the gauge is updated
   * @param tableName The table name for which the gauge is updated
   * @param lastOfflineSegmentTime The last offline segment end time, in milliseconds since the epoch.
   */
  public void updateOfflineSegmentDelayGauge(final String resource, final String tableName, final long lastOfflineSegmentTime) {
    final String fullGaugeName = "pinot.controller." + resource + "." + tableName + ".offlineSegmentDelayMillis";
    final MetricName metricName = new MetricName(ValidationMetrics.class, fullGaugeName);
    final boolean needToCreateNewGauge = !gaugeValues.containsKey(fullGaugeName);

    gaugeValues.put(fullGaugeName, lastOfflineSegmentTime);

    if (needToCreateNewGauge) {
      MetricsHelper.newGauge(_metricsRegistry, metricName, new CurrentTimeMillisDeltaGauge(fullGaugeName));
    }
  }

  /**
   * Updates the gauge for the last push time.
   *
   * @param resource The resource for which the gauge is updated
   * @param tableName The table name for which the gauge is updated
   * @param lastPushTimeMillis The last push time, in milliseconds since the epoch.
   */
  public void updateLastPushTimeGauge(final String resource, final String tableName, final long lastPushTimeMillis) {
    final String fullGaugeName = "pinot.controller." + resource + "." + tableName + ".lastPushTimeDelayMillis";
    final MetricName metricName = new MetricName(ValidationMetrics.class, fullGaugeName);
    final boolean needToCreateNewGauge = !gaugeValues.containsKey(fullGaugeName);

    gaugeValues.put(fullGaugeName, lastPushTimeMillis);

    if (needToCreateNewGauge) {
      MetricsHelper.newGauge(_metricsRegistry, metricName, new CurrentTimeMillisDeltaGauge(fullGaugeName));
    }
  }
}
