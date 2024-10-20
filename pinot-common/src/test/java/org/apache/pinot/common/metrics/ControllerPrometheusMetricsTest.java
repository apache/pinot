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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
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


public class ControllerPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  private static final String EXPORTED_METRIC_PREFIX = "pinot_controller_";
  private static final String LABEL_KEY_TASK_TYPE = "taskType";
  private static final String TASK_TYPE = "ClusterHealthCheck";
  private ControllerMetrics _controllerMetrics;

  private int _exporterPort;

  private HTTPServer _httpServer;

  @BeforeClass
  public void setup()
      throws Exception {

    _httpServer = startExporter(PinotComponent.CONTROLLER);

    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ControllerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  @Test(dataProvider = "controllerTimers")
  public void timerTest(ControllerTimer controllerTimer) {

    if (controllerTimer.isGlobal()) {
      _controllerMetrics.addTimedValue(controllerTimer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _controllerMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
      _controllerMetrics.addTimedTableValue(ExportedLabelValues.TABLENAME, controllerTimer, 30_000L,
          TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(),
          ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TABLENAME,
          EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "controllerMeters")
  public void meterTest(ControllerMeter meter) {
    //global meters
    if (meter.isGlobal()) {
      _controllerMetrics.addMeteredGlobalValue(meter, 5L);
      String meterName = meter.getMeterName();
      //some meters contain a "controller" prefix. For example, controllerInstancePostError. These meters are
      // exported as 'pinot_controller_pinot_controller_InstancePostError'. So we strip the 'controller' from
      // 'controllerInstancePostError'
      String strippedMeterName = StringUtils.remove(meterName, "controller");
      assertMeterExportedCorrectly(strippedMeterName, EXPORTED_METRIC_PREFIX);
    } else {
      if (meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR
          || meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
        _controllerMetrics.addMeteredTableValue(TASK_TYPE, meter, 1L);
      } else if (meter == ControllerMeter.PERIODIC_TASK_ERROR) {
        _controllerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE + "." + TASK_TYPE, meter, 1L);
      } else {
        _controllerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 5L);
        _controllerMetrics.addMeteredTableValue(ExportedLabelValues.TABLENAME, meter, 5L);
      }
      String meterName = meter.getMeterName();
      String strippedMeterName = StringUtils.remove(meterName, "controller");
      if (meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR) {
        assertMeterExportedCorrectly(meterName, List.of(ExportedLabelKeys.TABLE, TASK_TYPE),
            EXPORTED_METRIC_PREFIX);
      } else if (meter == ControllerMeter.PERIODIC_TASK_ERROR) {
        assertMeterExportedCorrectly(meterName, ExportedLabels.PERIODIC_TASK_TABLE_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else if (meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
        assertMeterExportedCorrectly(String.format("%s_%s", strippedMeterName, TASK_TYPE), EXPORTED_METRIC_PREFIX);
      } else if (meter == ControllerMeter.CONTROLLER_TABLE_SEGMENT_UPLOAD_ERROR) {
        assertMeterExportedCorrectly(meterName, ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
        assertMeterExportedCorrectly(meterName, ExportedLabels.TABLENAME,
            EXPORTED_METRIC_PREFIX);
      } else {
        assertMeterExportedCorrectly(strippedMeterName, ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
        assertMeterExportedCorrectly(strippedMeterName, ExportedLabels.TABLENAME,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  @Test(dataProvider = "controllerGauges")
  public void gaugeTest(ControllerGauge controllerGauge) {
    //that accept global gauge with suffix
    List<ControllerGauge> globalGaugesWithTaskType =
        List.of(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING,
            ControllerGauge.NUM_MINION_SUBTASKS_WAITING, ControllerGauge.NUM_MINION_SUBTASKS_ERROR,
            ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);

    //local gauges that accept partition
    List<ControllerGauge> gaugesAcceptingPartition =
        List.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS);

    //these accept task type
    List<ControllerGauge> gaugesAcceptingTaskType =
        List.of(ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
            ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
            ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR);

    List<ControllerGauge> gaugesAcceptingRawTableName = List.of(ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE);

    if (controllerGauge.isGlobal()) {
      _controllerMetrics.setValueOfGlobalGauge(controllerGauge, TASK_TYPE, 1L);
      if (globalGaugesWithTaskType.contains(controllerGauge)) {
        _controllerMetrics.setValueOfGlobalGauge(controllerGauge, TASK_TYPE, 1L);
        String strippedMetricName = getStrippedMetricName(controllerGauge);
        assertGaugeExportedCorrectly(strippedMetricName, List.of(LABEL_KEY_TASK_TYPE, TASK_TYPE),
            EXPORTED_METRIC_PREFIX);
      } else {
        _controllerMetrics.setValueOfGlobalGauge(controllerGauge, 1L);
        String strippedMetricName = getStrippedMetricName(controllerGauge);
        assertGaugeExportedCorrectly(strippedMetricName, EXPORTED_METRIC_PREFIX);
      }
    } else {
      if (gaugesAcceptingPartition.contains(controllerGauge)) {
        _controllerMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, controllerGauge, 10L);
        String strippedGaugeName = getStrippedMetricName(controllerGauge);
        ArrayList<String> exportedLabels =
            new ArrayList<>(ExportedLabels.PARTITION_TABLE_NAME_AND_TYPE);
        assertGaugeExportedCorrectly(strippedGaugeName, exportedLabels, EXPORTED_METRIC_PREFIX);
      } else if (gaugesAcceptingTaskType.contains(controllerGauge)) {
        _controllerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, TASK_TYPE, controllerGauge, () -> 50L);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(),
            ExportedLabels.TABLENAME_TABLETYPE_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else if (gaugesAcceptingRawTableName.contains(controllerGauge)) {
        _controllerMetrics.setValueOfTableGauge(ExportedLabelValues.TABLENAME, controllerGauge, 5L);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(), ExportedLabels.TABLENAME,
            EXPORTED_METRIC_PREFIX);
      } else if (controllerGauge == ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED) {
        _controllerMetrics.setValueOfTableGauge(String.format("%s.%s", TABLE_NAME_WITH_TYPE, TASK_TYPE),
            ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED, 5L);
        assertGaugeExportedCorrectly(ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED.getGaugeName(),
            ExportedLabels.TABLENAME_WITHTYPE_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else if (controllerGauge == ControllerGauge.TASK_STATUS) {
        _controllerMetrics.setValueOfTableGauge(String.format("%s.%s", TASK_TYPE, TaskState.IN_PROGRESS),
            ControllerGauge.TASK_STATUS, 5);
        assertGaugeExportedCorrectly(ControllerGauge.TASK_STATUS.getGaugeName(),
            ExportedLabels.STATUS_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else {
        _controllerMetrics.setValueOfTableGauge(TABLE_NAME_WITH_TYPE, controllerGauge, 5L);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(),
            ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
      }
    }
  }

  private static String getStrippedMetricName(ControllerGauge controllerGauge) {
    return StringUtils.remove(controllerGauge.getGaugeName(), "controller");
  }

  @Override
  protected SimpleHttpResponse getExportedPromMetrics() {
    try {
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _httpServer.getPort() + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @DataProvider(name = "controllerTimers")
  public Object[] controllerTimers() {
    return ControllerTimer.values();
  }

  @DataProvider(name = "controllerMeters")
  public Object[] controllerMeters() {
    return ControllerMeter.values();
  }

  @DataProvider(name = "controllerGauges")
  public Object[] controllerGauges() {
    return ControllerGauge.values();
  }

  @AfterClass
  public void cleanup() {
    _httpServer.close();
  }
}
