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
package org.apache.pinot.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.*;


public class ControllerConfTest {

  private static final List<String> UNIT_CONFIGS = Arrays
      .asList(RETENTION_MANAGER_FREQUENCY_IN_SECONDS, OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
          OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS, REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
          REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS, BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
          BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS, STATUS_CHECKER_FREQUENCY_IN_SECONDS,
          STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS, TASK_MANAGER_FREQUENCY_IN_SECONDS,
          MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS, MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
          MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
          TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS, SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS,
          SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS, STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS,
          OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
          DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS, SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS);

  private static final List<String> PERIOD_CONFIGS =
      Arrays.asList(RETENTION_MANAGER_FREQUENCY_PERIOD, OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD,
          REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_PERIOD,
          BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD, BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_PERIOD,
          STATUS_CHECKER_FREQUENCY_PERIOD, STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD, TASK_MANAGER_FREQUENCY_PERIOD,
          MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD, MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_PERIOD,
          MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD, TASK_METRICS_EMITTER_FREQUENCY_PERIOD,
          SEGMENT_RELOCATOR_FREQUENCY_PERIOD, SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD,
          STATUS_CHECKER_INITIAL_DELAY_PERIOD, OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_PERIOD,
          SEGMENT_RELOCATOR_INITIAL_DELAY_PERIOD);

  private static final int DURATION_IN_SECONDS = 20;
  private static final String DURATION_IN_PERIOD_VALID = "2m";
  private static final int PERIOD_DURATION_IN_SECONDS = 120;
  private static final String DURATION_IN_PERIOD_INVALID = "2m2m";

  /**
   * When both type of configs are present, then the human-readable period config overrides the unit config
   */
  @Test
  public void periodConfigOverridesUnitConfig() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    UNIT_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_SECONDS));
    PERIOD_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_PERIOD_VALID));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, PERIOD_DURATION_IN_SECONDS);
  }

  /**
   * When only unit configs are supplied, then the correct converted value is returned.
   */
  @Test
  public void suppliedUnitConfigsShouldReturnCorrectValues() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    UNIT_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_SECONDS));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, DURATION_IN_SECONDS);
  }

  /**
   * When only period configs are supplied, then the correct converted value is returned.
   */
  @Test
  public void suppliedPeriodConfigsShouldReturnCorrectValues() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    PERIOD_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_PERIOD_VALID));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, PERIOD_DURATION_IN_SECONDS);
  }

  /**
   * When valid unit config and invalid period config are specified, then an {@link IllegalArgumentException} is thrown (valid unit
   * config does not override invalid period config)
   */
  @Test
  public void validUnitConfigInvalidPeriodConfigExceptionShouldBeThrown() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    UNIT_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_SECONDS));
    PERIOD_CONFIGS.forEach(config -> controllerConfig.put(config, DURATION_IN_PERIOD_INVALID));
    ControllerConf conf = new ControllerConf(controllerConfig);
    Assert.assertThrows(IllegalArgumentException.class, conf::getSegmentRelocatorInitialDelayInSeconds);
  }

  private void assertOnDurations(ControllerConf conf, int expectedDuration) {
    long segmentRelocatorInitialDelayInSeconds = conf.getSegmentRelocatorInitialDelayInSeconds();
    long offlineSegmentIntervalCheckerInitialDelayInSeconds =
        conf.getOfflineSegmentIntervalCheckerInitialDelayInSeconds();
    long statusCheckerInitialDelayInSeconds = conf.getStatusCheckerInitialDelayInSeconds();
    int segmentLevelValidationIntervalInSeconds = conf.getSegmentLevelValidationIntervalInSeconds();
    int segmentRelocatorFrequencyInSeconds = conf.getSegmentRelocatorFrequencyInSeconds();
    int taskMetricsEmitterFrequencyInSeconds = conf.getTaskMetricsEmitterFrequencyInSeconds();
    int minionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds =
        conf.getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds();
    long minionInstancesCleanupTaskInitialDelaySeconds = conf.getMinionInstancesCleanupTaskInitialDelaySeconds();
    long minionInstancesCleanupTaskFrequencyInSeconds = conf.getMinionInstancesCleanupTaskFrequencyInSeconds();
    int taskManagerFrequencyInSeconds = conf.getTaskManagerFrequencyInSeconds();
    int statusCheckerWaitForPushTimeInSeconds = conf.getStatusCheckerWaitForPushTimeInSeconds();
    int statusCheckerFrequencyInSeconds = conf.getStatusCheckerFrequencyInSeconds();
    long brokerResourceValidationInitialDelayInSeconds = conf.getBrokerResourceValidationInitialDelayInSeconds();
    int brokerResourceValidationFrequencyInSeconds = conf.getBrokerResourceValidationFrequencyInSeconds();
    long realtimeSegmentValidationManagerInitialDelaySeconds =
        conf.getRealtimeSegmentValidationManagerInitialDelaySeconds();
    int realtimeSegmentValidationFrequencyInSeconds = conf.getRealtimeSegmentValidationFrequencyInSeconds();
    int offlineSegmentIntervalCheckerFrequencyInSeconds = conf.getOfflineSegmentIntervalCheckerFrequencyInSeconds();
    int retentionControllerFrequencyInSeconds = conf.getRetentionControllerFrequencyInSeconds();
    //then
    Assert.assertEquals(segmentRelocatorInitialDelayInSeconds, expectedDuration);
    Assert.assertEquals(offlineSegmentIntervalCheckerInitialDelayInSeconds, expectedDuration);
    Assert.assertEquals(statusCheckerInitialDelayInSeconds, expectedDuration);
    Assert.assertEquals(segmentLevelValidationIntervalInSeconds, expectedDuration);
    Assert.assertEquals(segmentRelocatorFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(taskMetricsEmitterFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(minionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds, expectedDuration);
    Assert.assertEquals(minionInstancesCleanupTaskInitialDelaySeconds, expectedDuration);
    Assert.assertEquals(minionInstancesCleanupTaskFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(taskManagerFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(statusCheckerWaitForPushTimeInSeconds, expectedDuration);
    Assert.assertEquals(statusCheckerFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(brokerResourceValidationInitialDelayInSeconds, expectedDuration);
    Assert.assertEquals(brokerResourceValidationFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(realtimeSegmentValidationManagerInitialDelaySeconds, expectedDuration);
    Assert.assertEquals(realtimeSegmentValidationFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(offlineSegmentIntervalCheckerFrequencyInSeconds, expectedDuration);
    Assert.assertEquals(retentionControllerFrequencyInSeconds, expectedDuration);
  }
}
