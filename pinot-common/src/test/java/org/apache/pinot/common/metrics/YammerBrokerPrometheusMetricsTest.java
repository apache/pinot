package org.apache.pinot.common.metrics;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class YammerBrokerPrometheusMetricsTest extends BrokerPrometheusMetricsTest {

  private BrokerMetrics _brokerMetrics;

  @BeforeClass
  public void setup()
      throws Exception {
    _brokerMetrics = new BrokerMetrics(getPinotMetricsFactory().getPinotMetricsRegistry());
  }

  @Test(dataProvider = "brokerTimers")
  public void timerTest(BrokerTimer timer) {
    if (timer.isGlobal()) {
      _brokerMetrics.addTimedValue(timer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _brokerMetrics.addTimedTableValue(PinotPrometheusMetricsTest.ExportedLabelValues.TABLENAME, timer, 30_000L,
          TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(), PinotPrometheusMetricsTest.ExportedLabels.TABLENAME,
          EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "brokerGauges")
  public void gaugeTest(BrokerGauge gauge) {
    if (gauge.isGlobal()) {
      _brokerMetrics.setOrUpdateGlobalGauge(gauge, () -> 5L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      if (gauge == BrokerGauge.REQUEST_SIZE) {
        _brokerMetrics.setOrUpdateTableGauge(PinotPrometheusMetricsTest.ExportedLabelValues.TABLENAME, gauge, 5L);
        assertGaugeExportedCorrectly(gauge.getGaugeName(), PinotPrometheusMetricsTest.ExportedLabels.TABLENAME,
            EXPORTED_METRIC_PREFIX);
      } else {
        _brokerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, gauge, 5L);
        assertGaugeExportedCorrectly(gauge.getGaugeName(),
            PinotPrometheusMetricsTest.ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
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
        _brokerMetrics.addMeteredTableValue(PinotPrometheusMetricsTest.ExportedLabelValues.TABLENAME, meter, 5L);
        assertMeterExportedCorrectly(meter.getMeterName(), PinotPrometheusMetricsTest.ExportedLabels.TABLENAME,
            EXPORTED_METRIC_PREFIX);
      } else {
        _brokerMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 5L);
        assertMeterExportedCorrectly(meter.getMeterName(),
            PinotPrometheusMetricsTest.ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
      }
    }
  }

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
