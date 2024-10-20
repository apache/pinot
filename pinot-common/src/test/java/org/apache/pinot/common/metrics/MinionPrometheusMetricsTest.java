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
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class MinionPrometheusMetricsTest extends PinotPrometheusMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_minion_";
  private static final String LABEL_KEY_TASKTYPE = "taskType";
  private static final String LABEL_VAL_TASKTYPE = "SegmentImportTask";
  private static final String LABEL_KEY_ID = "id";
  private static final String METER_PREFIX_NO_TASKS = "numberTasks";

  private MinionMetrics _minionMetrics;
  private HTTPServer _httpServer;

  private static final List<String> EXPORTED_LABELS_TABLENAME_TYPE_TASKTYPE =
      List.of(ExportedLabelKeys.TABLE, ExportedLabelValues.TABLENAME,
          ExportedLabelKeys.TABLETYPE, ExportedLabelValues.TABLETYPE_REALTIME, LABEL_KEY_TASKTYPE,
          LABEL_VAL_TASKTYPE);

  @BeforeClass
  public void setup() {

    _httpServer = startExporter(PinotComponent.MINION);

    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ControllerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _minionMetrics = new MinionMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  @Test(dataProvider = "minionTimers")
  public void timerTest(MinionTimer timer) {

    _minionMetrics.addTimedValue(LABEL_VAL_TASKTYPE, timer, 30L, TimeUnit.MILLISECONDS);
    assertTimerExportedCorrectly(timer.getTimerName(), List.of(LABEL_KEY_ID, LABEL_VAL_TASKTYPE),
        EXPORTED_METRIC_PREFIX);

    _minionMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, LABEL_VAL_TASKTYPE, timer, 30L, TimeUnit.MILLISECONDS);

    if (timer == MinionTimer.TASK_THREAD_CPU_TIME_NS) {
      assertTimerExportedCorrectly(timer.getTimerName(),
          List.of(ExportedLabelKeys.DATABASE, ExportedLabelValues.TABLENAME_WITH_TYPE_REALTIME,
              ExportedLabelKeys.TABLE, "myTable_REALTIME.SegmentImportTask"), EXPORTED_METRIC_PREFIX);
    } else {
      assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_LABELS_TABLENAME_TYPE_TASKTYPE,
          EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "minionMeters")
  public void meterTest(MinionMeter meter) {
    if (meter.isGlobal()) {
      validateGlobalMeters(meter);
    } else {
      validateMetersWithLabels(meter);
    }
  }

  private void validateMetersWithLabels(MinionMeter meter) {
    if (meter.getMeterName().startsWith(METER_PREFIX_NO_TASKS)) {
      _minionMetrics.addMeteredTableValue(ExportedLabelValues.TABLENAME, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(),
          List.of(LABEL_KEY_ID, ExportedLabelValues.TABLENAME), EXPORTED_METRIC_PREFIX);

      _minionMetrics.addMeteredValue(LABEL_VAL_TASKTYPE, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(), List.of(LABEL_KEY_ID, LABEL_VAL_TASKTYPE),
          EXPORTED_METRIC_PREFIX);
    } else if (meter == MinionMeter.SEGMENT_UPLOAD_FAIL_COUNT || meter == MinionMeter.SEGMENT_DOWNLOAD_FAIL_COUNT) {

      _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(), List.of(LABEL_KEY_ID, TABLE_NAME_WITH_TYPE),
          EXPORTED_METRIC_PREFIX);
    } else {
      //all remaining meters are also being used as global meters, check their usage
      _minionMetrics.addMeteredGlobalValue(meter, 1L);
      _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, LABEL_VAL_TASKTYPE, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
      assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_LABELS_TABLENAME_TYPE_TASKTYPE,
          EXPORTED_METRIC_PREFIX);
    }
  }

  private void validateGlobalMeters(MinionMeter meter) {
    _minionMetrics.addMeteredGlobalValue(meter, 5L);
    assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
  }

  @Test(dataProvider = "minionGauges")
  public void gaugeTest(MinionGauge gauge) {
    if (gauge.isGlobal()) {
      _minionMetrics.setValueOfGlobalGauge(gauge, 1L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      _minionMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, gauge, 1L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE,
          EXPORTED_METRIC_PREFIX);
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

  @DataProvider(name = "minionTimers")
  public Object[] minionTimers() {
    return MinionTimer.values();
  }

  @DataProvider(name = "minionMeters")
  public Object[] minionMeters() {
    return MinionMeter.values();
  }

  @DataProvider(name = "minionGauges")
  public Object[] minionGauges() {
    return MinionGauge.values();
  }

  @AfterClass
  public void cleanup() {
    _httpServer.close();
  }
}
