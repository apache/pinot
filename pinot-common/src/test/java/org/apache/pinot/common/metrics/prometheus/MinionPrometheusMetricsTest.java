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
import org.apache.pinot.common.metrics.MinionGauge;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.MinionTimer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class MinionPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported minion metrics have this prefix
  private static final String EXPORTED_METRIC_PREFIX = "pinot_minion_";
  private static final String METER_PREFIX_NO_TASKS = "numberTasks";

  private MinionMetrics _minionMetrics;

  @BeforeClass
  public void setup() {
    _minionMetrics = new MinionMetrics(_pinotMetricsFactory.getPinotMetricsRegistry());
  }

  @Test(dataProvider = "minionTimers")
  public void timerTest(MinionTimer timer) {
    if (timer.isGlobal()) {
      _minionMetrics.addTimedValue(timer, 30L, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _minionMetrics.addTimedValue(ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, timer, 30L, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(timer.getTimerName(),
          List.of(ExportedLabelKeys.ID, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT), EXPORTED_METRIC_PREFIX);
      _minionMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, timer,
          30L, TimeUnit.MILLISECONDS);
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

  @DataProvider(name = "minionTimers")
  public Object[] minionTimers() {
    return MinionTimer.values();
  }

  @DataProvider(name = "minionMeters")
  public Object[] minionMeters() {
    return MinionMeter.values();
  }

  @DataProvider(name = "minionGauges")
  public Object[] minionGauges() {
    return MinionGauge.values();
  }
}
