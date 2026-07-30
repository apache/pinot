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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies the native Micrometer behavior of the registry when the Prometheus leg is enabled: each SPI metric is
/// registered directly as a Micrometer meter in the {@code CompositeMeterRegistry} and shows up in the
/// {@code PrometheusMeterRegistry.scrape()} output with the correct native series and value. Also verifies the
/// standard JVM/system binders are bound and exported, the HTTP scrape endpoint serves the exposition, and that a
/// non-numeric gauge value is NaN-safe (does not break a scrape).
public class MicrometerPrometheusLegTest {
  private MicrometerMetricsFactory _factory;

  private MicrometerMetricsFactory newFactory(PinotConfiguration config) {
    MicrometerMetricsFactory factory = new MicrometerMetricsFactory();
    factory.init(config);
    _factory = factory;
    return factory;
  }

  private static PinotConfiguration prometheusConfig(int port) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_ENABLED_PROP, true);
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_PORT_PROP, port);
    return config;
  }

  @AfterMethod
  public void tearDown() {
    if (_factory != null) {
      _factory.getPinotMetricsRegistry().shutdown();
      _factory = null;
    }
    PinotMetricUtils.cleanUp();
  }

  /// Initializes {@link PinotMetricUtils} with the Micrometer factory and returns the resulting registry.
  /// Required for tests that exercise {@link ServerMetrics}: {@code AbstractMetrics} dispatches
  /// {@link PinotMetricUtils#makePinotGauge} and {@link PinotMetricUtils#makePinotMetricName} through the global
  /// factory singleton, which must be the Micrometer factory for tag-first naming to take effect.
  /// {@link #tearDown()} calls {@link PinotMetricUtils#cleanUp()} to release this factory.
  private MicrometerMetricsRegistry initWiredFactory() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, MicrometerMetricsFactory.class.getName());
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_ENABLED_PROP, true);
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_PORT_PROP, 0);
    PinotMetricUtils.init(config);
    return (MicrometerMetricsRegistry) PinotMetricUtils.getPinotMetricsRegistry();
  }

  @Test
  public void defaultFactoryHasNoPrometheusLeg() {
    MicrometerMetricsFactory factory = newFactory(new PinotConfiguration());
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    assertNull(registry.getPrometheusLeg(), "default (no legs) must not create a Prometheus leg");
    assertNull(registry.getJmxRegistry(), "default (no legs) must not create a JMX leg");
  }

  /// Registers one of every metric kind, records values, and asserts the NATIVE Micrometer series appear in the
  /// Prometheus scrape with correct values:
  /// - meter / counter -> a single {@code _total} series (cumulative);
  /// - timer -> {@code _count}/{@code _sum}/{@code _max};
  /// - gauge -> a bare series equal to the value;
  /// - histogram (DistributionSummary) -> {@code _count}/{@code _sum}.
  @Test
  public void nativeMetersExportExpectedPrometheusSeries() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    MicrometerPrometheusLeg leg = registry.getPrometheusLeg();
    assertNotNull(leg, "Prometheus leg must be created when prometheus.enabled=true");

    PinotMeter meter =
        registry.newMeter(factory.makePinotMetricName(getClass(), "pinot.test.testMeter"), "events", TimeUnit.SECONDS);
    meter.mark(5);

    PinotCounter counter = registry.newCounter(factory.makePinotMetricName(getClass(), "pinot.test.testCounter"));
    ((Counter) counter.getCounter()).increment(7);

    PinotTimer timer = registry.newTimer(factory.makePinotMetricName(getClass(), "pinot.test.testTimer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    timer.update(10, TimeUnit.MILLISECONDS);

    PinotGauge<Long> gauge = factory.makePinotGauge(avoid -> 42L);
    registry.newGauge(factory.makePinotMetricName(getClass(), "pinot.test.testGauge"), gauge);

    PinotHistogram histogram =
        registry.newHistogram(factory.makePinotMetricName(getClass(), "pinot.test.testHistogram"), false);
    ((DistributionSummary) histogram.getHistogram()).record(123);

    // The wrapper reads back the running total through Micrometer.
    assertEquals(meter.count(), 5L, "Micrometer meter must record through the wrapper");
    // Rate methods have no Micrometer equivalent -> NaN.
    assertTrue(Double.isNaN(meter.meanRate()), "Micrometer meter has no EWMA rate");
    assertTrue(Double.isNaN(timer.oneMinuteRate()), "Micrometer timer has no EWMA rate");

    String scrape = leg.scrape();
    assertTrue(scrape != null && !scrape.isEmpty(), "Prometheus scrape output must be non-empty");

    // meter / counter -> single _total series.
    assertEquals(seriesValue(scrape, "testMeter_total"), 5.0, 0.0, "meter exports a single _total");
    assertEquals(seriesValue(scrape, "testCounter_total"), 7.0, 0.0, "counter exports a single _total");
    // timer -> _count / _sum / _max.
    assertEquals(seriesValue(scrape, "testTimer_seconds_count"), 1.0, 0.0, "timer exports _count");
    assertTrue(scrape.contains("testTimer_seconds_sum"), "timer exports _sum: " + scrape);
    assertTrue(scrape.contains("testTimer_seconds_max"), "timer exports _max: " + scrape);
    // gauge -> a bare series = value.
    assertEquals(seriesValue(scrape, "testGauge"), 42.0, 0.0, "gauge exports a bare series equal to the value");
    // histogram (DistributionSummary) -> _count / _sum.
    assertEquals(seriesValue(scrape, "testHistogram_count"), 1.0, 0.0, "histogram exports _count");
    assertTrue(scrape.contains("testHistogram_sum"), "histogram exports _sum: " + scrape);
  }

  /// Basic JVM observability (memory, GC, threads, CPU) must be exported whenever a leg is enabled, since Micrometer
  /// binders are bound to the composite by the registry.
  @Test
  public void prometheusLegExportsJvmMetrics() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    String scrape = registry.getPrometheusLeg().scrape();

    assertTrue(scrape.contains("jvm_memory_used"), "heap/non-heap memory must be exported: " + scrape);
    assertTrue(scrape.contains("jvm_gc_"), "GC metrics must be exported: " + scrape);
    assertTrue(scrape.contains("jvm_threads_live"), "thread metrics must be exported: " + scrape);
    assertTrue(scrape.contains("system_cpu_count"), "CPU/system metrics must be exported: " + scrape);
  }

  @Test
  public void prometheusHttpEndpointServesScrape()
      throws Exception {
    int port = findFreePort();
    PinotConfiguration config = prometheusConfig(port);
    config.setProperty(MicrometerMetricsFactory.PROMETHEUS_PATH_PROP, "/metrics");

    MicrometerMetricsFactory factory = newFactory(config);
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotMeter meter =
        registry.newMeter(factory.makePinotMetricName(getClass(), "pinot.test.httpMeter"), "events", TimeUnit.SECONDS);
    meter.mark(3);

    HttpResponse<String> response = HttpClient.newHttpClient().send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics")).GET().build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(response.statusCode(), 200, "scrape endpoint must return 200");
    assertEquals(seriesValue(response.body(), "httpMeter_total"), 3.0, 0.0,
        "scrape endpoint must serve the native value");
  }

  @Test
  public void nonNumericGaugeDoesNotBreakScrape() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();

    PinotGauge<Long> numeric = factory.makePinotGauge(avoid -> 99L);
    registry.newGauge(factory.makePinotMetricName(getClass(), "pinot.test.okGauge"), numeric);
    // A non-numeric gauge value exercises the registry's toDouble() NaN fallback.
    PinotGauge<String> nonNumeric = factory.makePinotGauge(avoid -> "not-a-number");
    registry.newGauge(factory.makePinotMetricName(getClass(), "pinot.test.badGauge"), nonNumeric);

    // Must not throw, and the numeric companion must still export correctly.
    String scrape = registry.getPrometheusLeg().scrape();
    assertEquals(seriesValue(scrape, "okGauge"), 99.0, 0.0, "numeric gauge must export even alongside a NaN gauge");
  }

  /// Regression test for the gauge-weak-reference bug: Micrometer holds a gauge's state object weakly, and the
  /// production caller (AbstractMetrics) discards the wrapper returned by newGauge. The registry must strong-reference
  /// the holder so the gauge does not silently decay to NaN after a GC.
  @Test
  public void gaugeSurvivesGc()
      throws Exception {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();

    PinotGauge<Long> gauge = factory.makePinotGauge(avoid -> 7L);
    registry.newGauge(factory.makePinotMetricName(getClass(), "pinot.test.gcGauge"), gauge);
    // Drop the only application-side strong references (matching how AbstractMetrics discards the wrapper).
    gauge = null;
    System.gc();
    Thread.sleep(100);
    System.gc();

    String scrape = registry.getPrometheusLeg().scrape();
    assertEquals(seriesValue(scrape, "gcGauge"), 7.0, 0.0, "gauge must survive GC (strong-referenced by the registry)");
  }

  /// When the JMX leg is enabled, meters must be exposed as MBeans (under Micrometer-native names) in the platform
  /// MBean server.
  @Test
  public void jmxLegExposesMicrometerMBeans()
      throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MicrometerMetricsFactory.JMX_ENABLED_PROP, true);
    MicrometerMetricsFactory factory = newFactory(config);
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    assertNotNull(registry.getJmxRegistry(), "JMX leg must be created when jmx.enabled=true");

    PinotMeter meter =
        registry.newMeter(factory.makePinotMetricName(getClass(), "pinot.test.jmxMeter"), "events", TimeUnit.SECONDS);
    meter.mark(2);

    // Micrometer's JmxMeterRegistry registers MBeans under the "metrics" domain with Micrometer-derived names.
    // Micrometer camelCases the dotted name (jmxMeter -> ...JmxMeter), so match case-insensitively.
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = mbs.queryNames(new ObjectName("metrics:*"), null);
    boolean found = names.stream().anyMatch(n -> n.getCanonicalName().toLowerCase(Locale.US).contains("jmxmeter"));
    assertTrue(found, "JMX leg must expose the meter as an MBean; found: " + names);
  }

  /// The default namer drops the owning metrics-class prefix, yielding Yammer-closer names
  /// (`pinot_server_queries_total`, not `String_pinot_server_queries_total`).
  @Test
  public void defaultNamerDropsClassPrefix() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    // Pass a class whose simple name would be obvious if it leaked into the metric name.
    PinotMeter meter =
        registry.newMeter(factory.makePinotMetricName(String.class, "pinot.server.queries"), "queries",
            TimeUnit.SECONDS);
    meter.mark(1);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("pinot_server_queries_total"), "name should drop the class prefix: " + scrape);
    assertFalse(scrape.contains("String_pinot_server_queries"), "class prefix must be dropped: " + scrape);
  }

  /// A custom namer configured via `namer.className` is honored.
  @Test
  public void pluggableNamerIsHonored() {
    PinotConfiguration config = prometheusConfig(0);
    config.setProperty(MicrometerMetricsFactory.NAMER_CLASS_PROP, FixedNamer.class.getName());
    MicrometerMetricsFactory factory = newFactory(config);
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotMeter meter =
        registry.newMeter(factory.makePinotMetricName(getClass(), "pinot.server.queries"), "queries",
            TimeUnit.SECONDS);
    meter.mark(1);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("custom_pinot_server_queries_total"), "custom namer must be applied: " + scrape);
  }

  /// Timers publish the default (Yammer-matching) client-side percentiles as `{quantile="..."}` series.
  @Test
  public void timersPublishPercentilesByDefault() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotTimer timer = registry.newTimer(factory.makePinotMetricName(getClass(), "pinot.test.pctTimer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    timer.update(10, TimeUnit.MILLISECONDS);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("pctTimer") && scrape.contains("quantile=\"0.99\""),
        "default timer percentiles must be published: " + scrape);
  }

  /// The histogram (DistributionSummary) percentile path mirrors the timer path and must also publish quantiles.
  @Test
  public void histogramsPublishPercentilesByDefault() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotHistogram histogram =
        registry.newHistogram(factory.makePinotMetricName(getClass(), "pinot.test.pctHisto"), false);
    ((DistributionSummary) histogram.getHistogram()).record(5);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("pctHisto") && scrape.contains("quantile=\"0.99\""),
        "default histogram percentiles must be published: " + scrape);
  }

  /// Empty percentile config opts out of quantile series.
  @Test
  public void emptyPercentilesConfigSuppressesQuantiles() {
    PinotConfiguration config = prometheusConfig(0);
    config.setProperty(MicrometerMetricsFactory.PERCENTILES_PROP, "");
    MicrometerMetricsFactory factory = newFactory(config);
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotTimer timer = registry.newTimer(factory.makePinotMetricName(getClass(), "pinot.test.noPctTimer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    timer.update(10, TimeUnit.MILLISECONDS);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("noPctTimer_seconds_count"), "timer still exports its base series: " + scrape);
    assertFalse(scrape.contains("noPctTimer_seconds{quantile"), "empty config must suppress quantiles: " + scrape);
  }

  /// Only the configured percentiles are published.
  @Test
  public void customPercentilesHonored() {
    PinotConfiguration config = prometheusConfig(0);
    config.setProperty(MicrometerMetricsFactory.PERCENTILES_PROP, "0.5");
    MicrometerMetricsFactory factory = newFactory(config);
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();
    PinotTimer timer = registry.newTimer(factory.makePinotMetricName(getClass(), "pinot.test.customPctTimer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    timer.update(10, TimeUnit.MILLISECONDS);
    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("quantile=\"0.5\""), "configured percentile must appear: " + scrape);
    assertFalse(scrape.contains("quantile=\"0.99\""), "unconfigured percentile must NOT appear: " + scrape);
  }

  /// A Yammer-style integer percentile (out of Micrometer's (0,1] domain) must fail fast, not silently mis-export.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void malformedPercentilesRejected() {
    PinotConfiguration config = prometheusConfig(0);
    config.setProperty(MicrometerMetricsFactory.PERCENTILES_PROP, "99");
    newFactory(config);
  }

  /// An unknown namer className must fail fast with IllegalArgumentException (not a raw reflection error or a silent
  /// fallback to the default).
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void unknownNamerClassRejected() {
    PinotConfiguration config = prometheusConfig(0);
    config.setProperty(MicrometerMetricsFactory.NAMER_CLASS_PROP, "com.example.DoesNotExist");
    newFactory(config);
  }

  /// Tag-first meter test: driving through the real {@link ServerMetrics} stack via
  /// {@code addMeteredTableValue("myTable_OFFLINE", NUM_DOCS_SCANNED, 1)} must produce a Prometheus series named
  /// {@code pinot_server_numDocsScanned_total} with {@code table="myTable"} and {@code tableType="OFFLINE"} labels,
  /// and must NOT embed {@code myTable_OFFLINE} in the metric name.
  @Test
  public void tableMeterEmitsLabelsNotEmbeddedName() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.addMeteredTableValue("myTable_OFFLINE", ServerMeter.NUM_DOCS_SCANNED, 1L);

    String scrape = registry.getPrometheusLeg().scrape();
    // Base name must appear (without embedded table segment)
    assertTrue(scrape.contains("pinot_server_numDocsScanned_total"),
        "series name must be the dimension-free base: " + scrape);
    // Labels must appear
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
    // Table/type must NOT be embedded in the metric name
    assertFalse(hasSeriesContaining(scrape, "myTable_OFFLINE"),
        "myTable_OFFLINE must not be embedded in any series name: " + scrape);
  }

  /// Tag-first gauge test: {@code setValueOfTableGauge("myTable_OFFLINE", DOCUMENT_COUNT, 42)} must emit
  /// {@code pinot_server_documentCount} with {@code table} and {@code tableType} labels.
  @Test
  public void tableGaugeEmitsLabelsNotEmbeddedName() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.setValueOfTableGauge("myTable_OFFLINE", ServerGauge.DOCUMENT_COUNT, 42L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("pinot_server_documentCount"), "base gauge name must appear: " + scrape);
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
    assertFalse(hasSeriesContaining(scrape, "myTable_OFFLINE"),
        "myTable_OFFLINE must not be embedded in any series name: " + scrape);
  }

  /// Tag-first partition-gauge test: {@code setValueOfPartitionGauge("myTable_OFFLINE", 3, LLC_PARTITION_CONSUMING, 1)}
  /// must emit a {@code partition="3"} label in addition to the table labels.
  @Test
  public void partitionGaugeEmitsPartitionLabel() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.setValueOfPartitionGauge("myTable_OFFLINE", 3, ServerGauge.LLC_PARTITION_CONSUMING, 1L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
    assertTrue(scrape.contains("partition=\"3\""), "partition label must be present: " + scrape);
  }

  /// SPI-level tag test: creating a metric directly via the 4-arg factory method and asserting labels in the scrape.
  @Test
  public void spiTaggedMetricNameEmitsLabels() {
    MicrometerMetricsFactory factory = newFactory(prometheusConfig(0));
    MicrometerMetricsRegistry registry = (MicrometerMetricsRegistry) factory.getPinotMetricsRegistry();

    PinotMetricName name = factory.makePinotMetricName(ServerMetrics.class,
        "pinot.server.myTable_OFFLINE.numDocsScanned", "pinot.server.numDocsScanned",
        Map.of("table", "myTable", "tableType", "OFFLINE"));
    registry.newMeter(name, "rows", TimeUnit.SECONDS).mark(7);

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("pinot_server_numDocsScanned_total"), "base name must appear: " + scrape);
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
    assertFalse(hasSeriesContaining(scrape, "myTable_OFFLINE"),
        "table must not be embedded in the metric name: " + scrape);
  }

  /// True if any non-comment exposition line's series name-part (before '{' or whitespace) contains
  /// {@code nameContains}.
  private static boolean hasSeriesContaining(String scrape, String nameContains) {
    for (String line : scrape.split("\n")) {
      if (line.startsWith("#")) {
        continue;
      }
      // Extract just the name part (before any labels or value)
      String namePart = line.split("[{\\s]")[0];
      if (namePart.contains(nameContains)) {
        return true;
      }
    }
    return false;
  }

  /// Database-qualified table: {@code mydb.myTable_OFFLINE} must produce labels
  /// {@code database="mydb"}, {@code table="myTable"}, {@code tableType="OFFLINE"}.
  @Test
  public void databaseQualifiedTableEmitsDatabaseLabel() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.addMeteredTableValue("mydb.myTable_OFFLINE", ServerMeter.NUM_DOCS_SCANNED, 1L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("database=\"mydb\""), "database label must be present: " + scrape);
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
  }

  /// allTables sentinel: when per-table metrics are disabled (table not in allowedTables), {@code getTableName}
  /// returns "allTables". The scrape must NOT contain {@code table="allTables"} — the sentinel is not a real table.
  @Test
  public void allTablesSentinelEmitsNoTableLabel() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry, false, java.util.List.of());

    serverMetrics.addMeteredTableValue("notAllowed_OFFLINE", ServerMeter.NUM_DOCS_SCANNED, 1L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertFalse(scrape.contains("table=\"allTables\""),
        "allTables sentinel must NOT appear as a real table label: " + scrape);
  }

  /// Table gauge remove: registering and then removing a table gauge must remove it from the Prometheus scrape.
  @Test
  public void tableGaugeRemoveCleanup() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.setValueOfTableGauge("myTable_OFFLINE", ServerGauge.DOCUMENT_COUNT, 100L);
    String scrapeAfterAdd = registry.getPrometheusLeg().scrape();
    assertTrue(scrapeAfterAdd.contains("pinot_server_documentCount"),
        "gauge must be present before removal: " + scrapeAfterAdd);

    serverMetrics.removeTableGauge("myTable_OFFLINE", ServerGauge.DOCUMENT_COUNT);
    String scrapeAfterRemove = registry.getPrometheusLeg().scrape();
    assertFalse(
        scrapeAfterRemove.contains("table=\"myTable\"") && scrapeAfterRemove.contains("pinot_server_documentCount"),
        "gauge with table label must be absent after removal: " + scrapeAfterRemove);
  }

  /// Partition gauge remove: registering and then removing a partition gauge must clean up correctly.
  @Test
  public void partitionGaugeRemoveCleanup() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.setValueOfPartitionGauge("myTable_REALTIME", 5, ServerGauge.LLC_PARTITION_CONSUMING, 1L);
    String scrapeAfterAdd = registry.getPrometheusLeg().scrape();
    assertTrue(scrapeAfterAdd.contains("partition=\"5\""),
        "partition label must be present before removal: " + scrapeAfterAdd);

    serverMetrics.removePartitionGauge("myTable_REALTIME", 5, ServerGauge.LLC_PARTITION_CONSUMING);
    String scrapeAfterRemove = registry.getPrometheusLeg().scrape();
    assertFalse(
        scrapeAfterRemove.contains("partition=\"5\"") && scrapeAfterRemove.contains("llcPartitionConsuming"),
        "partition gauge must be absent after removal: " + scrapeAfterRemove);
  }

  /// Strengthened tag-first meter test: asserts the EXACT sample VALUE and that the embedded form
  /// {@code myTable_OFFLINE} (full type-suffixed string) is absent from the scrape.
  @Test
  public void tableMeterExactLabelSetAndValue() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.addMeteredTableValue("myTable_OFFLINE", ServerMeter.NUM_DOCS_SCANNED, 7L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertEquals(seriesValue(scrape, "pinot_server_numDocsScanned_total"), 7.0, 0.0,
        "meter must export the exact mark count: " + scrape);
    assertTrue(scrape.contains("table=\"myTable\""), "table label must be present: " + scrape);
    assertTrue(scrape.contains("tableType=\"OFFLINE\""), "tableType label must be present: " + scrape);
    assertFalse(scrape.contains("myTable_OFFLINE"), "myTable_OFFLINE must not appear anywhere in scrape: " + scrape);
  }

  /// Partition gauge uses REALTIME table type: asserts labels and that the embedded form is absent.
  @Test
  public void partitionGaugeUsesRealtimeTableType() {
    MicrometerMetricsRegistry registry = initWiredFactory();
    ServerMetrics serverMetrics = new ServerMetrics(registry);

    serverMetrics.setValueOfPartitionGauge("myTable_REALTIME", 7, ServerGauge.LLC_PARTITION_CONSUMING, 1L);

    String scrape = registry.getPrometheusLeg().scrape();
    assertTrue(scrape.contains("tableType=\"REALTIME\""), "tableType must be REALTIME: " + scrape);
    assertTrue(scrape.contains("partition=\"7\""), "partition label must be present: " + scrape);
    assertFalse(hasSeriesContaining(scrape, "myTable_REALTIME"),
        "myTable_REALTIME must not be embedded in any series name: " + scrape);
  }

  /// Custom namer used by {@link #pluggableNamerIsHonored()}; must be public with a no-arg ctor for PluginManager.
  public static class FixedNamer implements MicrometerMetricNamer {
    @Override
    public String meterName(Class<?> metricsClass, String dottedName) {
      return "custom." + dottedName;
    }
  }

  private static int findFreePort()
      throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /// Returns the value of the first non-comment Prometheus exposition line whose series name contains {@code
  /// nameContains}. The trailing whitespace-separated token of an exposition line is the sample value.
  private static double seriesValue(String scrape, String nameContains) {
    for (String line : scrape.split("\n")) {
      if (!line.startsWith("#") && line.contains(nameContains)) {
        String[] parts = line.trim().split("\\s+");
        return Double.parseDouble(parts[parts.length - 1]);
      }
    }
    throw new AssertionError("No exposition series containing '" + nameContains + "' in:\n" + scrape);
  }
}
