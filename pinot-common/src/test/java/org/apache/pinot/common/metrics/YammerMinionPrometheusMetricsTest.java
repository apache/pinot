package org.apache.pinot.common.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class YammerMinionPrometheusMetricsTest extends MinionPrometheusMetricsTest {

  private MinionMetrics _minionMetrics;

  @BeforeClass
  public void setup() {
    _minionMetrics = new MinionMetrics(getPinotMetricsFactory().getPinotMetricsRegistry());
  }

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }

  @Test(dataProvider = "minionTimers")
  public void timerTest(MinionTimer timer) {

    _minionMetrics.addTimedValue(ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, timer, 30L, TimeUnit.MILLISECONDS);
    assertTimerExportedCorrectly(timer.getTimerName(),
        List.of(ExportedLabelKeys.ID, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT), EXPORTED_METRIC_PREFIX);

    _minionMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, timer, 30L,
        TimeUnit.MILLISECONDS);

    if (timer == MinionTimer.TASK_THREAD_CPU_TIME_NS) {
      assertTimerExportedCorrectly(timer.getTimerName(),
          List.of(ExportedLabelKeys.DATABASE, ExportedLabelValues.TABLENAME_WITH_TYPE_REALTIME, ExportedLabelKeys.TABLE,
              "myTable_REALTIME.SegmentImportTask"), EXPORTED_METRIC_PREFIX);
    } else {
      assertTimerExportedCorrectly(timer.getTimerName(), ExportedLabels.TABLENAME_TABLETYPE_MINION_TASKTYPE,
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

  private void validateGlobalMeters(MinionMeter meter) {
    _minionMetrics.addMeteredGlobalValue(meter, 5L);
    assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
  }

  private void validateMetersWithLabels(MinionMeter meter) {
    if (meter.getMeterName().startsWith(METER_PREFIX_NO_TASKS)) {
      _minionMetrics.addMeteredTableValue(ExportedLabelValues.TABLENAME, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(), List.of(ExportedLabelKeys.ID, ExportedLabelValues.TABLENAME),
          EXPORTED_METRIC_PREFIX);

      _minionMetrics.addMeteredValue(ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(),
          List.of(ExportedLabelKeys.ID, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT), EXPORTED_METRIC_PREFIX);
    } else if (meter == MinionMeter.SEGMENT_UPLOAD_FAIL_COUNT || meter == MinionMeter.SEGMENT_DOWNLOAD_FAIL_COUNT) {

      _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, meter, 1L);
      assertMeterExportedCorrectly(meter.getMeterName(), List.of(ExportedLabelKeys.ID, TABLE_NAME_WITH_TYPE),
          EXPORTED_METRIC_PREFIX);
    } else {
      //all remaining meters are also being used as global meters, check their usage
      _minionMetrics.addMeteredGlobalValue(meter, 1L);
      _minionMetrics.addMeteredTableValue(TABLE_NAME_WITH_TYPE, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, meter,
          1L);
      assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_METRIC_PREFIX);
      assertMeterExportedCorrectly(meter.getMeterName(), ExportedLabels.TABLENAME_TABLETYPE_MINION_TASKTYPE,
          EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "minionGauges")
  public void gaugeTest(MinionGauge gauge) {
    if (gauge.isGlobal()) {
      _minionMetrics.setValueOfGlobalGauge(gauge, 1L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      _minionMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, gauge, 1L);
      assertGaugeExportedCorrectly(gauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
    }
  }
}
