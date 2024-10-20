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

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import io.prometheus.jmx.shaded.io.prometheus.client.exporter.HTTPServer;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class BrokerPrometheusMetricsTest extends PinotPrometheusMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_broker_";

  private static final String EXPORTED_METRIC_PREFIX_EXCEPTIONS = "exceptions";

  private static final List<BrokerMeter> GLOBAL_METERS_WITH_EXCEPTIONS_PREFIX =
      List.of(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, BrokerMeter.UNCAUGHT_POST_EXCEPTIONS,
          BrokerMeter.QUERY_REJECTED_EXCEPTIONS, BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS,
          BrokerMeter.RESOURCE_MISSING_EXCEPTIONS);

  private static final List<BrokerMeter> METERS_ACCEPTING_RAW_TABLENAME =
      List.of(BrokerMeter.QUERIES, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, BrokerMeter.DOCUMENTS_SCANNED,
          BrokerMeter.ENTRIES_SCANNED_IN_FILTER, BrokerMeter.BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS,
          BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED,
          BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS,
          BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS,
          BrokerMeter.ENTRIES_SCANNED_POST_FILTER, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
          BrokerMeter.QUERY_QUOTA_EXCEEDED);

  private BrokerMetrics _brokerMetrics;

  private HTTPServer _httpServer;

  @BeforeClass
  public void setup()
      throws Exception {
    _httpServer = startExporter(PinotComponent.BROKER);
    _brokerMetrics = new BrokerMetrics(_pinotMetricsFactory.getPinotMetricsRegistry());
  }

  @Test(dataProvider = "brokerTimers")
  public void timerTest(BrokerTimer timer) {
    if (timer.isGlobal()) {
      _brokerMetrics.addTimedValue(timer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _brokerMetrics.addTimedTableValue(ExportedLabelValues.TABLENAME, timer, 30_000L, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "brokerGauges")
  public void gaugeTest(BrokerGauge gauge) {
    if (gauge.isGlobal()) {
      _brokerMetrics.setOrUpdateGlobalGauge(gauge, () -> 5L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      if (gauge == BrokerGauge.REQUEST_SIZE) {
        _brokerMetrics.setOrUpdateTableGauge(ExportedLabelValues.TABLENAME, gauge, 5L);
        assertGaugeExportedCorrectly(gauge.getGaugeName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      } else {
        _brokerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, gauge, 5L);
        assertGaugeExportedCorrectly(gauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
      }
    }
  }

  @Test(dataProvider = "brokerMeters")
  public void meterTest(BrokerMeter meter) {
    if (meter.isGlobal()) {
      _brokerMetrics.addMeteredGlobalValue(meter, 5L);
      if (GLOBAL_METERS_WITH_EXCEPTIONS_PREFIX.contains(meter)) {
        String exportedMeterPrefix = String.format("%s_%s", EXPORTED_METRIC_PREFIX_EXCEPTIONS,
            StringUtils.remove(meter.getMeterName(), "Exceptions"));
        assertMeterExportedCorrectly(exportedMeterPrefix, EXPORTED_METRIC_PREFIX);
      } else {
        assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
      }
    } else {
      if (METERS_ACCEPTING_RAW_TABLENAME.contains(meter)) {
        _brokerMetrics.addMeteredTableValue(ExportedLabelValues.TABLENAME, meter, 5L);
        assertMeterExportedCorrectly(meter.getMeterName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      } else {
        _brokerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 5L);
        assertMeterExportedCorrectly(meter.getMeterName(), ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
      }
    }
  }

  @Override
  protected SimpleHttpResponse getExportedPromMetrics() {
    try {
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _httpServer.getPort() + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @DataProvider(name = "brokerTimers")
  public Object[] brokerTimers() {
    return BrokerTimer.values();
  }

  @DataProvider(name = "brokerMeters")
  public Object[] brokerMeters() {
    return BrokerMeter.values();
  }

  @DataProvider(name = "brokerGauges")
  public Object[] brokerGauges() {
    return BrokerGauge.values();
  }
}
