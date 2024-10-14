package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.version.PinotVersion;
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

  @BeforeClass
  public void setup() {
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
    Stream.of(ControllerTimer.values()).filter(ControllerTimer::isGlobal)
        .peek(controllerTimer -> _controllerMetrics.addTimedValue(controllerTimer, 30_000, TimeUnit.MILLISECONDS))
        .forEach(
            controllerTimer -> assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_METRIC_PREFIX));

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
          String strippedMeterName = StringUtils.remove(meterName, "controller");
          assertMeterExportedCorrectly(strippedMeterName, EXPORTED_METRIC_PREFIX);
        });

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
            assertMeterExportedCorrectly(meterName,
                List.of("periodicTask", "ClusterHealthCheck", "table", "myTable", "tableType", "REALTIME"),
                EXPORTED_METRIC_PREFIX);
          } else if (controllerMeter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
            assertMeterExportedCorrectly(strippedMeterName + "_" + "ClusterHealthCheck", EXPORTED_METRIC_PREFIX);
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
    List<ControllerGauge> globalGaugesWithTaskTypeSuffix =
        List.of(ControllerGauge.VERSION, ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS,
            ControllerGauge.NUM_MINION_SUBTASKS_RUNNING, ControllerGauge.NUM_MINION_SUBTASKS_WAITING,
            ControllerGauge.NUM_MINION_SUBTASKS_ERROR, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE,
            ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);

    Arrays.stream(ControllerGauge.values()).filter(ControllerGauge::isGlobal)
        .filter(globalGaugesWithTaskTypeSuffix::contains).peek(controllerGauge -> {
          if (controllerGauge == ControllerGauge.VERSION) {
            _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1L);
          } else {
            _controllerMetrics.setValueOfGlobalGauge(controllerGauge, TASK_TYPE, 1L);
          }
        }).filter(controllerGauge -> controllerGauge != ControllerGauge.VERSION).forEach(controllerGauge -> {
          try {
            String strippedMetricName = getStrippedMetricName(controllerGauge);
            assertGaugeExportedCorrectly(strippedMetricName, List.of(TASK_TYPE_KEY, TASK_TYPE), EXPORTED_METRIC_PREFIX);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    //remaining guages are set without any suffix
    Arrays.stream(ControllerGauge.values()).filter(ControllerGauge::isGlobal)
        .filter(controllerGauge -> !globalGaugesWithTaskTypeSuffix.contains(controllerGauge))
        .peek(controllerGauge -> _controllerMetrics.setValueOfGlobalGauge(controllerGauge, 1L))
        .forEach(controllerGauge -> {
          try {
            String strippedMetricName = getStrippedMetricName(controllerGauge);
            assertGaugeExportedCorrectly(strippedMetricName, EXPORTED_METRIC_PREFIX);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    //non-global gauges with accept partition
    List<ControllerGauge> nonGlobalGaugesThatAcceptPartition =
        List.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS);

    //that accept partition gauge
    Stream.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS).peek(controllerGauge -> {
      _controllerMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, controllerGauge, 10_000_000);
    }).forEach(controllerGauge -> {
      String strippedGaugeName = getStrippedMetricName(controllerGauge);
      try {
        ArrayList<String> exportedLabels = new ArrayList<>(EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
        exportedLabels.add("partition");
        exportedLabels.add("3");
        assertGaugeExportedCorrectly(strippedGaugeName, exportedLabels, EXPORTED_METRIC_PREFIX);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    //setOrUpdateTableGauge
//    TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE("TimeMsSinceLastMinionTaskMetadataUpdate", false),
//        TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION("TimeMsSinceLastSuccessfulMinionTaskGeneration", false),
//        LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR("LastMinionTaskGenerationEncountersError", false),

    Stream.of(ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
        ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
        ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR).peek(controllerGauge -> {
//      _controllerMetrics.setOrUpdateTableGauge();
    });

//    setOrUpdateGauge
//    LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_QUEUE_SIZE
  }

  private static String getStrippedMetricName(ControllerGauge controllerGauge) {
    return StringUtils.remove(controllerGauge.getGaugeName(), "controller");
  }
}

