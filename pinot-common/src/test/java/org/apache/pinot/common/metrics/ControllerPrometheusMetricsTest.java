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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class ControllerPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported controller metrics have this prefix
  protected static final String EXPORTED_METRIC_PREFIX = "pinot_controller_";
  protected static final String LABEL_KEY_TASK_TYPE = "taskType";

  //that accept global gauge with suffix
  protected static final List<ControllerGauge> GLOBAL_GAUGES_ACCEPTING_TASKTYPE =
      List.of(ControllerGauge.NUM_MINION_TASKS_IN_PROGRESS, ControllerGauge.NUM_MINION_SUBTASKS_RUNNING,
          ControllerGauge.NUM_MINION_SUBTASKS_WAITING, ControllerGauge.NUM_MINION_SUBTASKS_ERROR,
          ControllerGauge.PERCENT_MINION_SUBTASKS_IN_QUEUE, ControllerGauge.PERCENT_MINION_SUBTASKS_IN_ERROR);

  //local gauges that accept partition
  protected static final List<ControllerGauge> GAUGES_ACCEPTING_PARTITION =
      List.of(ControllerGauge.MAX_RECORDS_LAG, ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS);

  //these accept task type
  protected static final List<ControllerGauge> GAUGES_ACCEPTING_TASKTYPE =
      List.of(ControllerGauge.TIME_MS_SINCE_LAST_MINION_TASK_METADATA_UPDATE,
          ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
          ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR);

  protected static final List<ControllerGauge> GAUGES_ACCEPTING_RAW_TABLENAME =
      List.of(ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE);

  @Override
  protected PinotComponent getPinotComponent() {
    return PinotComponent.CONTROLLER;
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
