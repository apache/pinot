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
package org.apache.pinot.plugin.metrics.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Genuinely-native Micrometer implementation of {@link PinotMetricsRegistry}.
///
/// <p>A single {@link CompositeMeterRegistry} is the canonical store. Config-toggled export legs are registered as
/// members of the composite, which natively fans every meter to each member — there is no Dropwizard store and no
/// read-time mirroring:
/// <ul>
///   <li><b>Prometheus leg (opt-in):</b> a Micrometer {@link io.micrometer.prometheusmetrics.PrometheusMeterRegistry}
///       (held by {@link MicrometerPrometheusLeg}) for direct Prometheus exposition.</li>
///   <li><b>JMX leg (opt-in):</b> a Micrometer {@link JmxMeterRegistry} that exposes meters as MBeans using
///       Micrometer-native names (NOT Yammer-compatible).</li>
/// </ul>
///
/// <p>When at least one leg is enabled, the standard JVM/system binders (memory, GC, threads, class loader, CPU,
/// uptime, file descriptors) are bound to the composite so the legs export basic JVM observability. The closeable
/// binders ({@link JvmGcMetrics}, {@link JvmHeapPressureMetrics}) are held and released on {@link #shutdown()}.
///
/// <p>Exported names/resolutions follow Micrometer's conventions and intentionally differ from the legacy
/// Yammer/Dropwizard JMX names. Callers needing the exact legacy names should use the separate compound registry.
///
/// <p>Thread-safety: metric-creation methods delegate to the thread-safe {@link CompositeMeterRegistry} (which keys
/// meters in a concurrent map), so concurrent registration of the same name is safe and idempotent.
public class MicrometerMetricsRegistry implements PinotMetricsRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerMetricsRegistry.class);

  private final CompositeMeterRegistry _composite;
  @Nullable
  private final MicrometerPrometheusLeg _prometheusLeg;
  @Nullable
  private final JmxMeterRegistry _jmxRegistry;
  // JVM binders that hold resources (GC notification listeners) and must be closed on shutdown; null when no leg is
  // enabled (nothing would export the JVM metrics).
  @Nullable
  private final JvmGcMetrics _jvmGcMetrics;
  @Nullable
  private final JvmHeapPressureMetrics _jvmHeapPressureMetrics;
  // Micrometer registers a Gauge over its state object via a WeakReference by default; the production caller
  // (AbstractMetrics) discards the returned wrapper, so without a strong reference here the holder would be GC'd and
  // the gauge would silently report NaN. This map keeps each holder alive for the registry's lifetime AND makes
  // same-name re-registration idempotent (the returned wrapper always wraps the canonical, exported holder).
  private final Map<String, MicrometerSettableGauge<?>> _gaugeHolders = new ConcurrentHashMap<>();
  // Client-side percentiles published for timers and distribution summaries (empty = none).
  private final double[] _clientPercentiles;

  /// Creates a registry with no export legs. The composite still accepts meters but nothing is exported; useful for
  /// callers that never invoke the factory's {@code init()} (e.g. some tests).
  public MicrometerMetricsRegistry() {
    this(false, MicrometerMetricsFactory.DEFAULT_PROMETHEUS_PORT, MicrometerMetricsFactory.DEFAULT_PROMETHEUS_PATH,
        false, new double[0]);
  }

  /// @param prometheusEnabled whether to add a Prometheus export leg
  /// @param prometheusPort    HTTP scrape-endpoint port for the Prometheus leg ({@code <= 0} = no HTTP server)
  /// @param prometheusPath    HTTP scrape-endpoint path for the Prometheus leg
  /// @param jmxEnabled        whether to add a (Micrometer-native) JMX export leg
  /// @param clientPercentiles client-side percentiles to publish for timers/distribution summaries (empty = none)
  public MicrometerMetricsRegistry(boolean prometheusEnabled, int prometheusPort, String prometheusPath,
      boolean jmxEnabled, double[] clientPercentiles) {
    _clientPercentiles = clientPercentiles.clone();
    _composite = new CompositeMeterRegistry();
    _prometheusLeg = prometheusEnabled ? new MicrometerPrometheusLeg(prometheusPort, prometheusPath) : null;
    if (_prometheusLeg != null) {
      _composite.add(_prometheusLeg.getRegistry());
    }
    _jmxRegistry = jmxEnabled ? new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM) : null;
    if (_jmxRegistry != null) {
      _composite.add(_jmxRegistry);
    }
    // Bind JVM/system metrics only when something will export them.
    if (_prometheusLeg != null || _jmxRegistry != null) {
      new ClassLoaderMetrics().bindTo(_composite);
      new JvmMemoryMetrics().bindTo(_composite);
      new JvmThreadMetrics().bindTo(_composite);
      new ProcessorMetrics().bindTo(_composite);
      new UptimeMetrics().bindTo(_composite);
      new FileDescriptorMetrics().bindTo(_composite);
      _jvmGcMetrics = new JvmGcMetrics();
      _jvmGcMetrics.bindTo(_composite);
      _jvmHeapPressureMetrics = new JvmHeapPressureMetrics();
      _jvmHeapPressureMetrics.bindTo(_composite);
    } else {
      _jvmGcMetrics = null;
      _jvmHeapPressureMetrics = null;
      // No export leg enabled: meters are still accepted but go nowhere. Warn so a misconfigured deployment that
      // selected this backend without enabling prometheus/jmx is not silently blind.
      LOGGER.warn("Micrometer metrics backend has no export leg enabled ({}=false, {}=false); no metrics will be "
              + "exported. Enable at least one leg or use the compound registry for legacy JMX names.",
          MicrometerMetricsFactory.PROMETHEUS_ENABLED_PROP, MicrometerMetricsFactory.JMX_ENABLED_PROP);
    }
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    String metricName = name.getMetricName().toString();
    Tags tags = tagsOf(name);
    String holderKey = gaugeHolderKey(metricName, tags);
    _gaugeHolders.remove(holderKey);
    // Remove all meters matching name+tags from the composite.
    for (Meter meter : _composite.find(metricName).tags(tags).meters()) {
      _composite.remove(meter);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> PinotGauge<T> newGauge(PinotMetricName name, @Nullable final PinotGauge<T> gauge) {
    String metricName = name.getMetricName().toString();
    Tags tags = tagsOf(name);
    // Keyed by name+tags so the same base name with different tag combos registers distinct meters.
    String holderKey = gaugeHolderKey(metricName, tags);
    // computeIfAbsent registers the gauge exactly once per name+tags and retains a strong reference to the holder, so
    // the exported gauge never decays to NaN and a re-registration returns a wrapper over the canonical (exported)
    // holder.
    MicrometerSettableGauge<T> holder = (MicrometerSettableGauge<T>) _gaugeHolders.computeIfAbsent(holderKey, k -> {
      // The SPI hands us a MicrometerGauge whose backing handle (getGauge()) is the value holder.
      MicrometerSettableGauge<?> newHolder =
          gauge == null ? new MicrometerSettableGauge<>((Object) null) : (MicrometerSettableGauge<?>) gauge.getGauge();
      Gauge.builder(metricName, newHolder, h -> toDouble(h.getValue())).tags(tags).strongReference(true)
          .register(_composite);
      return newHolder;
    });
    return new MicrometerGauge<>(holder);
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    String metricName = name.getMetricName().toString();
    Counter counter = Counter.builder(metricName).tags(tagsOf(name)).register(_composite);
    return new MicrometerMeter(counter);
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    String metricName = name.getMetricName().toString();
    Counter counter = Counter.builder(metricName).tags(tagsOf(name)).register(_composite);
    return new MicrometerCounter(counter);
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    String metricName = name.getMetricName().toString();
    Timer.Builder builder = Timer.builder(metricName).tags(tagsOf(name));
    if (_clientPercentiles.length > 0) {
      builder.publishPercentiles(_clientPercentiles);
    }
    Timer timer = builder.register(_composite);
    return new MicrometerTimer(timer);
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    String metricName = name.getMetricName().toString();
    DistributionSummary.Builder builder = DistributionSummary.builder(metricName).tags(tagsOf(name));
    if (_clientPercentiles.length > 0) {
      builder.publishPercentiles(_clientPercentiles);
    }
    DistributionSummary summary = builder.register(_composite);
    return new MicrometerHistogram(summary);
  }

  /// Converts the tags from a {@link PinotMetricName} into a Micrometer {@link Tags} instance. Empty for names with
  /// no structured tags (global / legacy path).
  private static Tags tagsOf(PinotMetricName name) {
    Map<String, String> tagMap = name.getTags();
    if (tagMap.isEmpty()) {
      return Tags.empty();
    }
    String[] kvArray = new String[tagMap.size() * 2];
    int i = 0;
    for (Map.Entry<String, String> entry : tagMap.entrySet()) {
      kvArray[i++] = entry.getKey();
      kvArray[i++] = entry.getValue();
    }
    return Tags.of(kvArray);
  }

  /// Compound key for the gauge-holder map: combines the meter name and its tags so gauges with the same base name but
  /// different dimension values (e.g. different tables) are stored as separate holders.
  private static String gaugeHolderKey(String metricName, Tags tags) {
    return tags.iterator().hasNext() ? metricName + "|" + tags : metricName;
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    Map<PinotMetricName, PinotMetric> allMetrics = new HashMap<>();
    for (Meter meter : _composite.getMeters()) {
      String name = meter.getId().getName();
      allMetrics.put(new MicrometerMetricName(name), () -> meter);
    }
    return allMetrics;
  }

  /// Unsupported in the native Micrometer backend: there is no per-registry creation-listener equivalent in the
  /// {@link PinotMetricsRegistryListener} SPI (its {@code getMetricsRegistryListener()} is meant to surface a
  /// backend-native listener, which Micrometer does not model the same way). Implemented as a documented no-op;
  /// Micrometer's own {@code MeterRegistry.config().onMeterAdded(...)} hook is the native alternative if needed.
  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    LOGGER.debug("addListener is a no-op in the native Micrometer registry; use composite onMeterAdded if needed");
  }

  @Override
  public Object getMetricsRegistry() {
    return _composite;
  }

  /// Returns the optional Prometheus export leg, or {@code null} when it is not enabled.
  /// Package-private: exposed for in-module use (tests) only.
  @Nullable
  MicrometerPrometheusLeg getPrometheusLeg() {
    return _prometheusLeg;
  }

  /// Returns the optional (Micrometer-native) JMX export registry, or {@code null} when it is not enabled.
  /// Package-private: exposed for in-module use (tests) only.
  @Nullable
  JmxMeterRegistry getJmxRegistry() {
    return _jmxRegistry;
  }

  @Override
  public void shutdown() {
    if (_jvmGcMetrics != null) {
      _jvmGcMetrics.close();
    }
    if (_jvmHeapPressureMetrics != null) {
      _jvmHeapPressureMetrics.close();
    }
    if (_prometheusLeg != null) {
      _prometheusLeg.close();
    }
    if (_jmxRegistry != null) {
      _jmxRegistry.close();
    }
    _composite.close();
  }

  /// Best-effort conversion of an arbitrary gauge value to a double; {@code null} and non-numeric values report
  /// {@code NaN} so a single bad gauge cannot break a scrape.
  private static double toDouble(@Nullable Object value) {
    return value instanceof Number ? ((Number) value).doubleValue() : Double.NaN;
  }
}
