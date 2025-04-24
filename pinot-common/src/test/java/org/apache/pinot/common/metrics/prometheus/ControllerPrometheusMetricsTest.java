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
package org.apache.pinot.common.metrics.prometheus;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class ControllerPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported controller metrics have this prefix
  private static final String EXPORTED_METRIC_PREFIX = "pinot_controller_";
  private static final String LABEL_KEY_TASK_TYPE = "taskType";

  //that accept global gauge with suffix
  private static final List<ControllerGauge> GLOBAL_GAUGES_ACCEPTING_TASKTYPE =
      List.of(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING,
          ControllerGauge.NUM_MINION_SUBTASKS_WAITING, ControllerGauge.NUM_MINION_SUBTASKS_ERROR,
          ControllerGauge.NUM_MINION_SUBTASKS_UNKNOWN,
          ControllerGauge.NUM_MINION_SUBTASKS_DROPPED,
          ControllerGauge.NUM_MINION_SUBTASKS_TIMED_OUT,
          ControllerGauge.NUM_MINION_SUBTASKS_ABORTED,
          ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);

  //local gauges that accept partition
  private static final List<ControllerGauge> GAUGES_ACCEPTING_PARTITION =
      List.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS);

  //these accept task type
  private static final List<ControllerGauge> GAUGES_ACCEPTING_TASKTYPE =
      List.of(ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
          ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
          ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR);

  private static final List<ControllerGauge> GAUGES_ACCEPTING_RAW_TABLENAME = List.of();

  private ControllerMetrics _controllerMetrics;

  @BeforeClass
  public void setup()
      throws Exception {
    _controllerMetrics = new ControllerMetrics(_pinotMetricsFactory.getPinotMetricsRegistry());
  }

  @Test(dataProvider = "controllerTimers")
  public void timerTest(ControllerTimer controllerTimer) {
    if (controllerTimer.isGlobal()) {
      _controllerMetrics.addTimedValue(controllerTimer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(controllerTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      if (controllerTimer == ControllerTimer.TABLE_REBALANCE_EXECUTION_TIME_MS) {
        _controllerMetrics.addTimedTableValue(String.format("%s.%s", TABLE_NAME_WITH_TYPE, ExportedLabelValues.DONE

            ),
            ControllerTimer.TABLE_REBALANCE_EXECUTION_TIME_MS, 100_000, TimeUnit.MILLISECONDS);
        assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.JOBSTATUS_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else if (controllerTimer == ControllerTimer.CRON_SCHEDULER_JOB_EXECUTION_TIME_MS) {
        _controllerMetrics.addTimedTableValue(String.format("%s.%s", TABLE_NAME_WITH_TYPE,
                ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT),
            ControllerTimer.CRON_SCHEDULER_JOB_EXECUTION_TIME_MS, 100_000, TimeUnit.MILLISECONDS);
        assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TASKTYPE_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else {
        _controllerMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, controllerTimer, 30_000L, TimeUnit.MILLISECONDS);
        _controllerMetrics.addTimedTableValue(ExportedLabelValues.TABLENAME, controllerTimer, 30_000L,
            TimeUnit.MILLISECONDS);

        assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
        assertTimerExportedCorrectly(controllerTimer.getTimerName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      }
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
      } else if (controllerGauge == ControllerGauge.TABLE_REBALANCE_JOB_PROGRESS_PERCENT) {
        addGaugeWithLabels(controllerGauge,
            String.format("%s.%s", TABLE_NAME_WITH_TYPE, REBALANCE_JOB_ID));
        assertGaugeExportedCorrectly(controllerGauge.getGaugeName(),
            ExportedLabels.JOBID_TABLENAME_TABLETYPE, EXPORTED_METRIC_PREFIX);
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
}
