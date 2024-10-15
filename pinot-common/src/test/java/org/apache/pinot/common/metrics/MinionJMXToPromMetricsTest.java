package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.util.List;
import java.util.stream.Stream;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class MinionJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_minion_";
  private static final String TASK_TYPE_KEY = "taskType";
  private static final String TASK_TYPE = "SegmentImportTask";
  private MinionMetrics _minionMetrics;

  @BeforeClass
  public void setup() {
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

  @Test
  public void timerTest() {
  }

  @Test
  public void meterTest() {
    //global meters
    Stream.of(MinionMeter.values()).filter(MinionMeter::isGlobal)
        .peek(meter -> _minionMetrics.addMeteredGlobalValue(meter, 5L))
        .forEach(meter -> assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX));
    //local meters
    Stream.of(MinionMeter.values()).filter(meter -> !meter.isGlobal())
        .filter(meter -> meter.getMeterName().startsWith("numberTasks")).peek(meter -> {
          _minionMetrics.addMeteredTableValue(RAW_TABLE_NAME, meter, 1L);
          _minionMetrics.addMeteredValue(TASK_TYPE, meter, 1L);
        }).forEach(meter -> {
          assertMeterExportedCorrectly(meter.getMeterName(), List.of("id", RAW_TABLE_NAME), EXPORTED_METRIC_PREFIX);
          assertMeterExportedCorrectly(meter.getMeterName(), List.of("id", TASK_TYPE), EXPORTED_METRIC_PREFIX);
        });

    List<MinionMeter> metersAcceptingTableNameWithType =
        List.of(MinionMeter.SEGMENT_UPLOAD_FAIL_COUNT, MinionMeter.SEGMENT_DOWNLOAD_FAIL_COUNT);

    metersAcceptingTableNameWithType.stream()
        .peek(meter -> _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 1L)).forEach(
            meter -> assertMeterExportedCorrectly(meter.getMeterName(), List.of("id", TABLE_NAME_WITH_TYPE),
                EXPORTED_METRIC_PREFIX));

    Stream.of(MinionMeter.values()).filter(meter -> !meter.isGlobal())
        .filter(meter -> !meter.getMeterName().startsWith("numberTasks"))
        .filter(meter -> !metersAcceptingTableNameWithType.contains(meter)).peek(meter -> {
          _minionMetrics.addMeteredGlobalValue(meter, 1L);
          _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, TASK_TYPE, meter, 1L);
        }).forEach(meter -> {
          assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
          assertMeterExportedCorrectly(meter.getMeterName(),
              List.of("table", RAW_TABLE_NAME, "tableType", "REALTIME", "taskType", TASK_TYPE), EXPORTED_METRIC_PREFIX);
        });
  }

  @Test
  public void gaugeTest() {
  }
}
