package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class BrokerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_broker_";
  private BrokerMetrics _brokerMetrics;

  @BeforeClass
  public void setup() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _brokerMetrics = new BrokerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  @Test
  public void brokerTimerTest() {
    //first assert on global timers
    Stream.of(BrokerTimer.values()).filter(BrokerTimer::isGlobal)
        .peek(timer -> _brokerMetrics.addTimedValue(timer, 30_000, TimeUnit.MILLISECONDS))
        .forEach(timer -> assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_METRIC_PREFIX));
    //Assert on local timers
    Stream.of(BrokerTimer.values()).filter(timer -> !timer.isGlobal()).peek(timer -> {
      _brokerMetrics.addTimedTableValue(RAW_TABLE_NAME, timer, 30_000L, TimeUnit.MILLISECONDS);
    }).forEach(timer -> assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
        EXPORTED_METRIC_PREFIX));
  }

  @Test
  public void brokerGaugeTest() {
    //global gauges
    Stream.of(BrokerGauge.values()).filter(BrokerGauge::isGlobal)
        .peek(gauge -> _brokerMetrics.setOrUpdateGlobalGauge(gauge, () -> 5L))
        .forEach(gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX));
    //local gauges
    Stream.of(BrokerGauge.values()).filter(gauge -> !gauge.isGlobal()).peek(gauge -> {
      if (gauge == BrokerGauge.REQUEST_SIZE) {
        _brokerMetrics.setOrUpdateTableGauge(RAW_TABLE_NAME, gauge, 5L);
      } else {
        _brokerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, gauge, 5L);
      }
    }).forEach(gauge -> {
      if (gauge == BrokerGauge.REQUEST_SIZE) {
        assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME, EXPORTED_METRIC_PREFIX);
      } else {
        assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
            EXPORTED_METRIC_PREFIX);
      }
    });
  }

  @Test
  public void brokerMeterTest() {
    Stream.of(BrokerMeter.values()).filter(BrokerMeter::isGlobal)
        .peek(meter -> _brokerMetrics.addMeteredGlobalValue(meter, 5L)).forEach(meter -> {
          try {
            if (meter == BrokerMeter.UNCAUGHT_GET_EXCEPTIONS || meter == BrokerMeter.UNCAUGHT_POST_EXCEPTIONS
                || meter == BrokerMeter.QUERY_REJECTED_EXCEPTIONS || meter == BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS
                || meter == BrokerMeter.RESOURCE_MISSING_EXCEPTIONS) {
              assertMeterExportedCorrectly("exceptions" + "_" + StringUtils.remove(meter.getMeterName(), "Exceptions"),
                  EXPORTED_METRIC_PREFIX);
            } else {
              assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    Stream.of(BrokerMeter.values()).filter(meter -> !meter.isGlobal())
        .peek(meter -> _brokerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 5L)).forEach(meter -> {
          assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
              EXPORTED_METRIC_PREFIX);
        });
  }
}
