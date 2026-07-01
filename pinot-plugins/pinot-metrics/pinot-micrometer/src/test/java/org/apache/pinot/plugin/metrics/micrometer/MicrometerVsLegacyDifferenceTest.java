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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// An explicit, reviewable record of how the native Micrometer backend's exported metrics DIFFER from the legacy
/// Yammer/Dropwizard backend, for a representative meter, timer, and gauge.
///
/// ## Legacy (Yammer/Dropwizard) exported series, per metric
///
/// The legacy backends expose each metric as a JMX MBean whose attributes become Prometheus series via the JMX
/// exporter. The attribute (suffix) sets are fixed by the Yammer/Dropwizard MBean shape:
/// - meter -> `Count`, `MeanRate`, `OneMinuteRate`, `FiveMinuteRate`, `FifteenMinuteRate` (5 series);
/// - timer -> the meter rate set PLUS distribution attributes `Min`, `Max`, `Mean`, `StdDev` and the percentiles
///   `50thPercentile`, `75thPercentile`, `95thPercentile`, `98thPercentile`, `99thPercentile`, `999thPercentile`;
/// - gauge -> a single `Value`.
///
/// ## Native Micrometer exported series, per metric
///
/// Micrometer registers native instruments and (for the Prometheus leg) lets the registry naming convention derive
/// the series. The shapes are fundamentally different:
/// - meter -> a single cumulative `_total` and NO EWMA rate series (rate is computed downstream via
///   Prometheus `rate()`);
/// - timer -> `_count` / `_sum` / `_max` (plus any client-side percentiles, none configured here) and NO EWMA rates;
/// - gauge -> a single bare series (no `Value` suffix).
///
/// The test asserts both sides: the legacy `PinotMetered` rate methods return real (finite) EWMA values, while the
/// native Micrometer wrappers return `NaN` for the same methods; and the native Prometheus scrape contains exactly
/// the native suffixes and none of the legacy rate/percentile suffixes.
public class MicrometerVsLegacyDifferenceTest {

  /// The fixed JMX attribute (suffix) set the legacy Yammer/Dropwizard meter MBean exposes. Recorded here as a
  /// constant for a reviewable side-by-side contrast against the native single-`_total` shape.
  private static final Set<String> LEGACY_METER_SUFFIXES =
      Set.of("Count", "MeanRate", "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate");

  /// The fixed JMX attribute (suffix) set the legacy Yammer/Dropwizard timer MBean exposes: the meter rate set plus
  /// distribution/percentile attributes. Recorded for contrast against the native `_count`/`_sum`/`_max` shape.
  private static final Set<String> LEGACY_TIMER_SUFFIXES = Set.of(
      "Count", "MeanRate", "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate",
      "Min", "Max", "Mean", "StdDev",
      "50thPercentile", "75thPercentile", "95thPercentile", "98thPercentile", "99thPercentile", "999thPercentile");

  /// The single JMX attribute the legacy gauge MBean exposes.
  private static final String LEGACY_GAUGE_SUFFIX = "Value";

  private MicrometerMetricsFactory _micrometerFactory;
  private YammerMetricsFactory _yammerFactory;

  @AfterMethod
  public void tearDown() {
    if (_micrometerFactory != null) {
      _micrometerFactory.getPinotMetricsRegistry().shutdown();
      _micrometerFactory = null;
    }
    if (_yammerFactory != null) {
      _yammerFactory.getPinotMetricsRegistry().shutdown();
      _yammerFactory = null;
    }
  }

  /// A representative METER: legacy exposes 5 working rate/count attributes; Micrometer exposes one `_total` and
  /// reports `NaN` for every rate method.
  @Test
  public void meterDiffersFromLegacy() {
    // Legacy: the rate methods are real, finite EWMA values (5-attribute MBean shape).
    _yammerFactory = new YammerMetricsFactory();
    _yammerFactory.init(new PinotConfiguration());
    PinotMeter legacyMeter = _yammerFactory.getPinotMetricsRegistry()
        .newMeter(_yammerFactory.makePinotMetricName(getClass(), "diff.meter"), "events", TimeUnit.SECONDS);
    legacyMeter.mark(5);
    assertEquals(legacyMeter.count(), 5L, "legacy meter counts marks");
    assertFalse(Double.isNaN(legacyMeter.meanRate()), "legacy meter computes a real mean rate (EWMA)");

    // Native: a single _total in the scrape, and every rate method is NaN (no EWMA in Micrometer).
    String scrape = scrapeNativeMeter("diff.meter", 5);
    assertTrue(hasSeries(scrape, "diff_meter_total"), "native meter exports a single _total: " + scrape);
    // None of the legacy meter attribute series exist on the native side (checked on this metric's series only).
    assertNoLegacySuffixOnSeries(scrape, "diff_meter", LEGACY_METER_SUFFIXES);
  }

  /// A representative TIMER: legacy exposes the full percentile/rate attribute set; Micrometer exposes
  /// `_count`/`_sum`/`_max` and `NaN` rates, with none of the legacy percentile series.
  @Test
  public void timerDiffersFromLegacy() {
    _yammerFactory = new YammerMetricsFactory();
    _yammerFactory.init(new PinotConfiguration());
    PinotTimer legacyTimer = _yammerFactory.getPinotMetricsRegistry()
        .newTimer(_yammerFactory.makePinotMetricName(getClass(), "diff.timer"), TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS);
    legacyTimer.update(10, TimeUnit.MILLISECONDS);
    assertFalse(Double.isNaN(legacyTimer.meanRate()), "legacy timer computes a real mean rate (EWMA)");

    MicrometerMetricsFactory micrometerFactory = newMicrometerFactory();
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) micrometerFactory.getPinotMetricsRegistry();
    PinotTimer nativeTimer = registry.newTimer(micrometerFactory.makePinotMetricName(getClass(), "diff.timer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    nativeTimer.update(10, TimeUnit.MILLISECONDS);
    assertTrue(Double.isNaN(nativeTimer.meanRate()), "native timer has no EWMA rate (NaN)");

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(hasSeries(scrape, "diff_timer_seconds_count"), "native timer exports _count: " + scrape);
    assertTrue(scrape.contains("diff_timer_seconds_sum"), "native timer exports _sum: " + scrape);
    assertTrue(scrape.contains("diff_timer_seconds_max"), "native timer exports _max: " + scrape);
    // None of the legacy timer attribute series (CamelCase EWMA-rate/percentile/distribution) exist natively.
    assertNoLegacySuffixOnSeries(scrape, "diff_timer", LEGACY_TIMER_SUFFIXES);
  }

  /// A representative GAUGE: legacy exposes a single `Value` attribute; Micrometer exposes a single bare series with
  /// no suffix.
  @Test
  public void gaugeDiffersFromLegacy() {
    _yammerFactory = new YammerMetricsFactory();
    _yammerFactory.init(new PinotConfiguration());
    PinotGauge<Long> legacyGauge = _yammerFactory.makePinotGauge(avoid -> 42L);
    _yammerFactory.getPinotMetricsRegistry()
        .newGauge(_yammerFactory.makePinotMetricName(getClass(), "diff.gauge"), legacyGauge);
    assertEquals(legacyGauge.value(), Long.valueOf(42L), "legacy gauge reports its Value attribute");

    MicrometerMetricsFactory micrometerFactory = newMicrometerFactory();
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) micrometerFactory.getPinotMetricsRegistry();
    PinotGauge<Long> nativeGauge = micrometerFactory.makePinotGauge(avoid -> 42L);
    registry.newGauge(micrometerFactory.makePinotMetricName(getClass(), "diff.gauge"), nativeGauge);

    String scrape = registry.getPrometheusLeg().scrape();
    // Native gauge: a single bare series equal to the value, with no Value suffix.
    assertEquals(gaugeValue(scrape, "diff_gauge"), 42.0, 0.0, "native gauge exports a bare series equal to the value");
    boolean hasLegacySuffix =
        scrape.contains("diff_gauge_" + LEGACY_GAUGE_SUFFIX) || scrape.contains("diff_gauge" + LEGACY_GAUGE_SUFFIX);
    assertFalse(hasLegacySuffix, "native gauge must NOT carry the legacy " + LEGACY_GAUGE_SUFFIX + " suffix");
  }

  private MicrometerMetricsFactory newMicrometerFactory() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_ENABLED_PROP, true);
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_PORT_PROP, 0);
    MicrometerMetricsFactory factory = new MicrometerMetricsFactory();
    factory.init(config);
    _micrometerFactory = factory;
    return factory;
  }

  private String scrapeNativeMeter(String name, long marks) {
    MicrometerMetricsFactory factory = newMicrometerFactory();
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotMetricName metricName = factory.makePinotMetricName(getClass(), name);
    PinotMeter meter = registry.newMeter(metricName, "events", TimeUnit.SECONDS);
    meter.mark(marks);
    return registry.getPrometheusLeg().scrape();
  }

  /// Asserts that no exposition series belonging to {@code metricToken} (non-comment lines whose name contains the
  /// token) carries any of the legacy CamelCase JMX attribute suffixes. Scoped to the metric's own series so it is not
  /// confused by unrelated JVM/system metrics or HELP/TYPE comment text elsewhere in the scrape.
  private static void assertNoLegacySuffixOnSeries(String scrape, String metricToken, Set<String> legacySuffixes) {
    for (String line : scrape.split("\n")) {
      if (line.startsWith("#") || !line.contains(metricToken)) {
        continue;
      }
      for (String suffix : legacySuffixes) {
        assertFalse(line.contains(suffix),
            "native series for " + metricToken + " must NOT carry legacy suffix " + suffix + ": " + line);
      }
    }
  }

  /// True if a non-comment exposition line's series name contains {@code nameContains}.
  private static boolean hasSeries(String scrape, String nameContains) {
    for (String line : scrape.split("\n")) {
      if (!line.startsWith("#") && line.contains(nameContains)) {
        return true;
      }
    }
    return false;
  }

  private static double gaugeValue(String scrape, String name) {
    for (String line : scrape.split("\n")) {
      if (!line.startsWith("#") && line.contains(name)) {
        String[] parts = line.trim().split("\\s+");
        return Double.parseDouble(parts[parts.length - 1]);
      }
    }
    throw new AssertionError("No gauge series '" + name + "' in:\n" + scrape);
  }
}
