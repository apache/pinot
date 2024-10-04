package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ControllerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {
  private static final String EXPORTED_METRIC_PREFIX = "pinot_controller_";
  private ControllerMetrics _controllerMetrics;
  @BeforeClass
  public void setup() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  @Test
  public void controllerTimerTest()
      throws IOException, URISyntaxException {

    for (ControllerTimer controllerTimer : ControllerTimer.values()) {
      if (controllerTimer.isGlobal()) {
        _controllerMetrics.addTimedValue(controllerTimer, 30_000, TimeUnit.MILLISECONDS);
      } else {
        _controllerMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
        _controllerMetrics.addTimedTableValue(RAW_TABLE_NAME, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
      }
    }

    for (ControllerTimer controllerTimer : ControllerTimer.values()) {
      if (controllerTimer.isGlobal()) {
        assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
      } else {
        assertMeterExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
            EXPORTED_METRIC_PREFIX);
        assertMeterExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  @Test
  public void controllerMeterTest()
      throws IOException, URISyntaxException {
    for (ControllerMeter controllerMeter : ControllerMeter.values()) {
      if (controllerMeter.isGlobal()) {
        _controllerMetrics.addMeteredGlobalValue(controllerMeter, 5L);
      }
    }
    for (ControllerMeter controllerMeter : ControllerMeter.values()) {
      if (controllerMeter.isGlobal()) {
        if (controllerMeter == ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR) {
          assertMeterExportedCorrectly("InstancePostError", EXPORTED_METRIC_PREFIX);
        } else if (controllerMeter == ControllerMeter.CONTROLLER_INSTANCE_DELETE_ERROR) {
          assertMeterExportedCorrectly("InstanceDeleteError", EXPORTED_METRIC_PREFIX);
        } else if (controllerMeter == ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR) {
          assertMeterExportedCorrectly("SegmentUploadError", EXPORTED_METRIC_PREFIX);
        } else {
          assertMeterExportedCorrectly(controllerMeter.getMeterName(), EXPORTED_METRIC_PREFIX);
        }
      }
    }
  }
}
