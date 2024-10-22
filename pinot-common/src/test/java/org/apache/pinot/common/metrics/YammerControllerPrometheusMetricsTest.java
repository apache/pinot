package org.apache.pinot.common.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class YammerControllerPrometheusMetricsTest extends ControllerPrometheusMetricsTest {

  private ControllerMetrics _controllerMetrics;

  @BeforeClass
  public void setup()
      throws Exception {
    _controllerMetrics = new ControllerMetrics(getPinotMetricsFactory().getPinotMetricsRegistry());
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

      assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TABLENAME_TABLETYPE,
          EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "controllerMeters")
  public void meterTest(ControllerMeter meter) {
    if (meter.isGlobal()) {
      _controllerMetrics.addMeteredGlobalValue(meter, 5L);
      String meterName = meter.getMeterName();
      //some meters contain a "controller" prefix. For example, controllerInstancePostError. These meters are
      // exported as 'pinot_controller_pinot_controller_InstancePostError'. So we strip the 'controller' from
      // 'controllerInstancePostError'
      String strippedMeterName = StringUtils.remove(meterName, "controller");
      assertMeterExportedCorrectly(strippedMeterName, EXPORTED_METRIC_PREFIX);
    } else {

      String meterName = meter.getMeterName();
      String strippedMeterName = StringUtils.remove(meterName, "controller");

      if (meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR) {
        addMeterWithLabels(meter, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC);
        assertMeterExportedCorrectly(meterName,
            List.of(ExportedLabelKeys.TABLE, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC), EXPORTED_METRIC_PREFIX);
      } else if (meter == ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN) {
        addMeterWithLabels(meter, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC);
        assertMeterExportedCorrectly(
            String.format("%s_%s", strippedMeterName, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC),
            EXPORTED_METRIC_PREFIX);
      } else if (meter == ControllerMeter.PERIODIC_TASK_ERROR) {
        addMeterWithLabels(meter, TABLE_NAME_WITH_TYPE + "." + ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC);
        assertMeterExportedCorrectly(meterName, ExportedLabels.CONTROLLER_TASKTYPE_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else {
        addMeterWithLabels(meter, TABLE_NAME_WITH_TYPE);
        addMeterWithLabels(meter, ExportedLabelValues.TABLENAME);
        if (meter == ControllerMeter.CONTROLLER_TABLE_SEGMENT_UPLOAD_ERROR) {
          assertMeterExportedCorrectly(meterName, ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
          assertMeterExportedCorrectly(meterName, ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
        } else {
          assertMeterExportedCorrectly(strippedMeterName, ExportedLabels.TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
          assertMeterExportedCorrectly(strippedMeterName, ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
        }
      }
    }
  }

  @Test(dataProvider = "controllerGauges")
  public void gaugeTest(ControllerGauge controllerGauge) {
    if (controllerGauge.isGlobal()) {
      _controllerMetrics.setValueOfGlobalGauge(controllerGauge, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC, 1L);
      //some global gauges also accept the taskType (which should not be). todo: this should be fixed
      if (GLOBAL_GAUGES_ACCEPTING_TASKTYPE.contains(controllerGauge)) {
        _controllerMetrics.setValueOfGlobalGauge(controllerGauge, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC, 1L);
        String strippedMetricName = getStrippedMetricName(controllerGauge);
        assertGaugeExportedCorrectly(strippedMetricName,
            List.of(LABEL_KEY_TASK_TYPE, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC), EXPORTED_METRIC_PREFIX);
      } else {
        _controllerMetrics.setValueOfGlobalGauge(controllerGauge, 1L);
        String strippedMetricName = getStrippedMetricName(controllerGauge);
        assertGaugeExportedCorrectly(strippedMetricName, EXPORTED_METRIC_PREFIX);
      }
    } else {
      if (GAUGES_ACCEPTING_PARTITION.contains(controllerGauge)) {
        _controllerMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, controllerGauge, 10L);
        String strippedGaugeName = getStrippedMetricName(controllerGauge);
        assertGaugeExportedCorrectly(strippedGaugeName, ExportedLabels.PARTITION_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_TASKTYPE.contains(controllerGauge)) {
        _controllerMetrics.setOrUpdateTableGauge(TABLE_NAME_WITH_TYPE, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC,
            controllerGauge, () -> 50L);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(),
            ExportedLabels.TABLENAME_TABLETYPE_CONTROLLER_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_RAW_TABLENAME.contains(controllerGauge)) {
        addGaugeWithLabels(controllerGauge, ExportedLabelValues.TABLENAME);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      } else if (controllerGauge == ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED) {
        addGaugeWithLabels(controllerGauge,
            String.format("%s.%s", TABLE_NAME_WITH_TYPE, ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC));
        assertGaugeExportedCorrectly(ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED.getGaugeName(),
            ExportedLabels.TABLENAMEWITHTYPE_CONTROLLER_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else if (controllerGauge == ControllerGauge.TASK_STATUS) {
        addGaugeWithLabels(controllerGauge,
            String.format("%s.%s", ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC, TaskState.IN_PROGRESS));
        assertGaugeExportedCorrectly(ControllerGauge.TASK_STATUS.getGaugeName(),
            ExportedLabels.JOBSTATUS_CONTROLLER_TASKTYPE, EXPORTED_METRIC_PREFIX);
      } else {
        addGaugeWithLabels(controllerGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  private void addGaugeWithLabels(ControllerGauge gauge, String labels) {
    _controllerMetrics.setValueOfTableGauge(labels, gauge, 5L);
  }

  private static String getStrippedMetricName(ControllerGauge controllerGauge) {
    return StringUtils.remove(controllerGauge.getGaugeName(), "controller");
  }

  private void addMeterWithLabels(ControllerMeter meter, String labels) {
    _controllerMetrics.addMeteredTableValue(labels, meter, 1L);
  }

  @Override
  protected PinotMetricsFactory getPinotMetricsFactory() {
    return new YammerMetricsFactory();
  }
}
