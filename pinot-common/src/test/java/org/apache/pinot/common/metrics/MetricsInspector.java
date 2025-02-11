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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistryListener;
import com.yammer.metrics.core.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * A helper for accessing metric values, agnostic of the concrete instance type of the metric. General usage is as
 * follows: <br>
 * <ul>
 *   <li>
 *     Construct a {@code MetricsInspector} from a {@code PinotMetricsRegistry}:<br>
 *     <pre>final MetricsInspector inspector = new MetricsInspector(pinotMetricsRegistry);</pre>
 *   </li>
 *   <li>
 *     Every time a metric is added to the registry this MetricsInspector will record it. The most recently added
 *     metric can be accessed with:<br>
 *     <pre>final MetricName metricName = inspector.lastMetric();</pre>
 *   </li>
 *   <li>
 *     Use the {@code MetricName} returned by {@link #_lastMetric} to query the properties of the corresponding
 *     metric, for example:
 *     <pre>inspector.getTimer(metricName)</pre>
 *     It's the caller's responsibility to know the type of the metric (Timer, Meter, etc.) and call the appropriate
 *     getter.
 *   </li>
 * </ul>
 *
 */
public class MetricsInspector {
  private final Map<MetricName, Metric> _metricMap = new HashMap<>();
  private MetricName _lastMetric;

  public MetricsInspector(PinotMetricsRegistry registry) {

    /* We detect newly added metrics by adding a listener to the metrics registry. Callers typically don't have
    direct references to the metrics they create because factory methods usually create the metric and add it to the
    registry without returning it. Since there is no easy way to look up the created metric afterward, the listener
    approach provides callers with a way to access the metrics they've just created.
    */
    registry.addListener(() -> new MetricsRegistryListener() {
      @Override
      public void onMetricAdded(MetricName metricName, Metric metric) {
        _metricMap.put(metricName, metric);
        _lastMetric = metricName;
      }
      @Override
      public void onMetricRemoved(MetricName metricName) {
        _metricMap.remove(metricName);
      }
    });
  }

  /**
   * @return the {@code MetricName} of the last metric that was added to the registry
   */
  public MetricName lastMetric() {
    return _lastMetric;
  }

  /**
   * Extracts the {@code Timer} from a {@code Timer} metric.
   *
   * @param metric a {@code MetricName} returned by a previous call to {@link #_lastMetric}
   * @return the {@code Timer} from the associated metric.
   * @throws IllegalArgumentException if the provided {@code MetricName} is not associated with a {@code Timer} metric
   */
  public Timer getTimer(MetricName metric) {
    return access(metric, m -> m._timer);
  }

  /**
   * Extracts the {@code Metered} from a {@code Metered} metric.
   *
   * @param metric a {@code MetricName} returned by a previous call to {@link #_lastMetric}
   * @return the {@code Metered} from the associated metric.
   * @throws IllegalArgumentException if the provided {@code MetricName} is not associated with a {@code Metered} metric
   */

  public Metered getMetered(MetricName metric) {
    return access(metric, m -> m._metered);
  }

  private <T> T access(MetricName metricName, Function<MetricAccessor, T> property) {
    Metric metric = _metricMap.get(metricName);
    if (metric == null) {
      throw new IllegalArgumentException("Metric not found: " + metricName);
    }

    MetricAccessor accessor = new MetricAccessor();
    try {
      metric.processWith(accessor, null, null);
    } catch (Exception e) {
      // Convert checked exception (from processWith API) to unchecked because our MetricProcessor doesn't throw
      throw new IllegalStateException("Unexpected error processing metric: " + metric, e);
    }

    T result = property.apply(accessor);
    if (result == null) {
      throw new IllegalArgumentException("Requested metric type not found in metric [" + metricName.getName() + "]");
    }
    return result;
  }

  /**
   * A MetricProcessor that simply captures the internals of a {@code Metric}. For internal use only.<br>
   */
  private static class MetricAccessor implements MetricProcessor<Void> {

    public Metered _metered;
    public Counter _counter;
    public Histogram _histogram;
    public Timer _timer;
    public Gauge<?> _gauge;

    @Override
    public void processMeter(MetricName n, Metered metered, Void v) {
      _metered = metered;
    }

    @Override
    public void processCounter(MetricName n, Counter counter, Void v) {
      _counter = counter;
    }

    @Override
    public void processHistogram(MetricName n, Histogram histogram, Void v) {
      _histogram = histogram;
    }

    @Override
    public void processTimer(MetricName n, Timer timer, Void v) {
      _timer = timer;
    }

    @Override
    public void processGauge(MetricName metricName, Gauge<?> gauge, Void v) {
      _gauge = gauge;
    }
  }
}
