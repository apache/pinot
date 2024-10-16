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
import io.prometheus.jmx.BuildInfoCollector;
import io.prometheus.jmx.JavaAgent;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.jmx.common.http.ConfigurationException;
import io.prometheus.jmx.common.http.HTTPServerFactory;
import io.prometheus.jmx.shaded.io.prometheus.client.CollectorRegistry;
import io.prometheus.jmx.shaded.io.prometheus.client.exporter.HTTPServer;
import io.prometheus.jmx.shaded.io.prometheus.client.hotspot.DefaultExports;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ControllerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {
  private static final String EXPORTED_METRIC_PREFIX = "pinot_controller_";
  private static final String TASK_TYPE_KEY = "taskType";
  private static final String TASK_TYPE = "ClusterHealthCheck";
  private ControllerMetrics _controllerMetrics;

  private int _exporterPort;

  private HTTPServer _httpServer;

  @BeforeClass
  public void setup()
      throws Exception {

    _exporterPort = 9000 + new Random().nextInt(1000);
    String agentArgs = String.format("%s:%s", _exporterPort,
        "../docker/images/pinot/etc/jmx_prometheus_javaagent/configs/controller.yml");

    String host = "0.0.0.0";

    try {
      JMXExporterConfig config = parseConfig(agentArgs, host);
      CollectorRegistry registry = new CollectorRegistry();
      (new JmxCollector(new File(config.file), JmxCollector.Mode.AGENT)).register(registry);
      DefaultExports.register(registry);
      _httpServer = (new HTTPServerFactory()).createHTTPServer(config.socket, registry, true, new File(config.file));
    } catch (ConfigurationException var4) {
      System.err.println("Configuration Exception : " + var4.getMessage());
      System.exit(1);
    } catch (IllegalArgumentException var5) {
      System.err.println("Usage: -javaagent:/path/to/JavaAgent.jar=[host:]<port>:<yaml configuration file> " + var5.getMessage());
      System.exit(1);
    }

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

  @Test
  public void controllerTimerTest() {
    //global timers
    Stream.of(ControllerTimer.values()).filter(ControllerTimer::isGlobal)
        .peek(controllerTimer -> _controllerMetrics.addTimedValue(controllerTimer, 30_000, TimeUnit.MILLISECONDS))
        .forEach(
            controllerTimer -> assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_METRIC_PREFIX));

    //local timers
    Stream.of(ControllerTimer.values()).filter(controllerTimer -> !controllerTimer.isGlobal()).peek(controllerTimer -> {
      _controllerMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
      _controllerMetrics.addTimedTableValue(RAW_TABLE_NAME, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
    }).forEach(controllerTimer -> {
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
          EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
          EXPORTED_METRIC_PREFIX);
    });
  }

  @Test
  public void controllerMeterTest() {
    //global meters
    Arrays.stream(ControllerMeter.values()).filter(ControllerMeter::isGlobal)
        .peek(controllerMeter -> _controllerMetrics.addMeteredGlobalValue(controllerMeter, 5L))
        .forEach(controllerMeter -> {
          String meterName = controllerMeter.getMeterName();
          //some meters contain a "controller" prefix. For example, controllerInstancePostError. These meters are
          // exported as 'pinot_controller_pinot_controller_InstancePostError'. So we strip the 'controller' from
          // 'controllerInstancePostError'
          String strippedMeterName = StringUtils.remove(meterName, "controller");
          assertMeterExportedCorrectly(strippedMeterName, EXPORTED_METRIC_PREFIX);
        });

    //local meters
    Arrays.stream(ControllerMeter.values()).filter(controllerMeter -> !controllerMeter.isGlobal())
        .peek(controllerMeter -> {
          if (controllerMeter == ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR
              || controllerMeter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
            _controllerMetrics.addMeteredTableValue(TASK_TYPE, controllerMeter, 1L);
          } else if (controllerMeter == ControllerMeter.PERIODIC_TASK_ERROR) {
            _controllerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE + "." + TASK_TYPE, controllerMeter, 1L);
          } else {
            _controllerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, controllerMeter, 5L);
            _controllerMetrics.addMeteredTableValue(RAW_TABLE_NAME, controllerMeter, 5L);
          }
        }).forEach(controllerMeter -> {
          String meterName = controllerMeter.getMeterName();
          String strippedMeterName = StringUtils.remove(meterName, "controller");
          if (controllerMeter == ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR) {
            assertMeterExportedCorrectly(meterName, List.of("table", "ClusterHealthCheck"), EXPORTED_METRIC_PREFIX);
          } else if (controllerMeter == ControllerMeter.PERIODIC_TASK_ERROR) {
            assertMeterExportedCorrectly(meterName, EXPORTED_LABELS_PERIODIC_TASK_TABLE_TABLETYPE,
                EXPORTED_METRIC_PREFIX);
          } else if (controllerMeter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
            assertMeterExportedCorrectly(String.format("%s_%s", strippedMeterName, TASK_TYPE), EXPORTED_METRIC_PREFIX);
          } else if (controllerMeter == ControllerMeter.CONTROLLER_TABLE_SEGMENT_UPLOAD_ERROR) {
            assertMeterExportedCorrectly(meterName, EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE, EXPORTED_METRIC_PREFIX);
            assertMeterExportedCorrectly(meterName, EXPORTED_LABELS_FOR_RAW_TABLE_NAME, EXPORTED_METRIC_PREFIX);
          } else {
            assertMeterExportedCorrectly(strippedMeterName, EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
                EXPORTED_METRIC_PREFIX);
            assertMeterExportedCorrectly(strippedMeterName, EXPORTED_LABELS_FOR_RAW_TABLE_NAME, EXPORTED_METRIC_PREFIX);
          }
        });
  }

  @Test
  public void controllerGaugeTest() {
    //that accept global gauge with suffix
    List<ControllerGauge> globalGaugesWithTaskType =
        List.of(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING,
            ControllerGauge.NUM_MINION_SUBTASKS_WAITING, ControllerGauge.NUM_MINION_SUBTASKS_ERROR,
            ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);

    Arrays.stream(ControllerGauge.values()).filter(ControllerGauge::isGlobal).filter(globalGaugesWithTaskType::contains)
        .peek(controllerGauge -> _controllerMetrics.setValueOfGlobalGauge(controllerGauge, TASK_TYPE, 1L))
        .filter(controllerGauge -> controllerGauge != ControllerGauge.VERSION).forEach(controllerGauge -> {
          String strippedMetricName = getStrippedMetricName(controllerGauge);
          assertGaugeExportedCorrectly(strippedMetricName, List.of(TASK_TYPE_KEY, TASK_TYPE), EXPORTED_METRIC_PREFIX);
        });

    //remaining guages are set without any suffix
    Arrays.stream(ControllerGauge.values()).filter(ControllerGauge::isGlobal)
        .filter(controllerGauge -> !globalGaugesWithTaskType.contains(controllerGauge))
        .peek(controllerGauge -> _controllerMetrics.setValueOfGlobalGauge(controllerGauge, 1L))
        .forEach(controllerGauge -> {
          String strippedMetricName = getStrippedMetricName(controllerGauge);
          assertGaugeExportedCorrectly(strippedMetricName, EXPORTED_METRIC_PREFIX);
        });

    //local gauges that accept partition
    List<ControllerGauge> gaugesAcceptingPartition =
        List.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS);

    gaugesAcceptingPartition.stream().peek(
            controllerGauge -> _controllerMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, controllerGauge,
                10L))
        .forEach(controllerGauge -> {
          String strippedGaugeName = getStrippedMetricName(controllerGauge);
          ArrayList<String> exportedLabels = new ArrayList<>(EXPORTED_LABELS_FOR_PARTITION_TABLE_NAME_AND_TYPE);
          assertGaugeExportedCorrectly(strippedGaugeName, exportedLabels, EXPORTED_METRIC_PREFIX);
        });

    //these accept task type
    List<ControllerGauge> gaugesAcceptingTaskType =
        List.of(ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
            ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
            ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR);

    gaugesAcceptingTaskType.stream()
        .peek(gauge -> _controllerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, TASK_TYPE, gauge, () -> 50L))
        .forEach(
            gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_TABLE_TABLETYPE_TASKTYPE,
                EXPORTED_METRIC_PREFIX));

    List<ControllerGauge> gaugesAcceptingRawTableName = List.of(ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE);
    gaugesAcceptingRawTableName.stream()
        .peek(gauge -> _controllerMetrics.setValueOfTableGauge(RAW_TABLE_NAME, gauge, 5L)).forEach(
            gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
                EXPORTED_METRIC_PREFIX));

    //ad-hoc
    _controllerMetrics.setValueOfTableGauge(String.format("%s.%s", TABLE_NAME_WITH_TYPE, TASK_TYPE),
        ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED, 5L);
    assertGaugeExportedCorrectly(ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED.getGaugeName(),
        EXPORTED_LABELS_FOR_TABLENAMEANDTYPE_AND_TASKTYPE, EXPORTED_METRIC_PREFIX);

    _controllerMetrics.setValueOfTableGauge(String.format("%s.%s", TASK_TYPE, TaskState.IN_PROGRESS),
        ControllerGauge.TASK_STATUS, 5);
    assertGaugeExportedCorrectly(ControllerGauge.TASK_STATUS.getGaugeName(), EXPORTED_LABELS_FOR_TASK_TYPE_AND_STATUS,
        EXPORTED_METRIC_PREFIX);

    //all remaining gauges
    Stream.of(ControllerGauge.values()).filter(
            gauge -> isRemaining(gauge, gaugesAcceptingPartition, gaugesAcceptingTaskType, gaugesAcceptingRawTableName))
        .filter(gauge -> gauge != ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED && gauge != ControllerGauge.TASK_STATUS)
        .peek(gauge -> _controllerMetrics.setValueOfTableGauge(TABLE_NAME_WITH_TYPE, gauge, 5L)).forEach(gauge -> {
          assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
              EXPORTED_METRIC_PREFIX);
        });
  }

  private boolean isRemaining(ControllerGauge gauge, List<ControllerGauge> gaugesAcceptingPartition,
      List<ControllerGauge> gaugesAcceptingTaskType, List<ControllerGauge> gaugesAcceptingRawTableName) {
    return !gauge.isGlobal() && !gaugesAcceptingPartition.contains(gauge) && !gaugesAcceptingTaskType.contains(gauge)
        && !gaugesAcceptingRawTableName.contains(gauge);
  }

  private static String getStrippedMetricName(ControllerGauge controllerGauge) {
    return StringUtils.remove(controllerGauge.getGaugeName(), "controller");
  }

  @Override
  protected SimpleHttpResponse getExportedPromMetrics() {
    try {
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _exporterPort + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
