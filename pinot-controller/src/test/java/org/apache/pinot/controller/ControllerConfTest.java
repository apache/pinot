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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.TimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.*;


public class ControllerConfTest {

  // Deprecated configs have been removed - no configs to test for fallback behavior

  private static final List<String> NEW_CONFIGS = List.of(
      RETENTION_MANAGER_FREQUENCY_PERIOD,
      OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD,
      REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD,
      BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD,
      STATUS_CHECKER_FREQUENCY_PERIOD,
      STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD,
      TASK_MANAGER_FREQUENCY_PERIOD,
      TASK_METRICS_EMITTER_FREQUENCY_PERIOD,
      SEGMENT_RELOCATOR_FREQUENCY_PERIOD,
      SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD
  );

  private static final Random RAND = new Random();

  /**
   * When only new configs are supplied (deprecated configs have been removed), then the correct
   * converted value is returned.
   */
  @Test
  public void supplyingNewConfigsShouldReturnCorrectlyConvertedValue() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    String period = getRandomPeriodInMinutes();
    NEW_CONFIGS.forEach(config -> controllerConfig.put(config, period));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS),
        controllerConfig);
  }

  @Test
  public void validateSegmentRelocatorRebalanceDefaultConfigs() {
    ControllerConf conf = new ControllerConf(Map.of());
    Assert.assertFalse(conf.getSegmentRelocatorReassignInstances());
    Assert.assertFalse(conf.getSegmentRelocatorBootstrap());
    Assert.assertFalse(conf.getSegmentRelocatorDowntime());
    Assert.assertEquals(conf.getSegmentRelocatorMinAvailableReplicas(), -1);
    Assert.assertTrue(conf.getSegmentRelocatorBestEfforts());
    Assert.assertFalse(conf.isSegmentRelocatorIncludingConsuming());
    Assert.assertEquals(conf.getSegmentRelocatorMinimizeDataMovement(), Enablement.ENABLE);
    Assert.assertEquals(conf.getSegmentRelocatorBatchSizePerServer(), RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
  }

  @Test
  public void validateSegmentRelocatorRebalanceConfigs() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(SEGMENT_RELOCATOR_REASSIGN_INSTANCES, true);
    properties.put(SEGMENT_RELOCATOR_BOOTSTRAP, false);
    properties.put(SEGMENT_RELOCATOR_DOWNTIME, true);
    properties.put(SEGMENT_RELOCATOR_MIN_AVAILABLE_REPLICAS, -2);
    properties.put(SEGMENT_RELOCATOR_BEST_EFFORTS, true);
    properties.put(SEGMENT_RELOCATOR_INCLUDE_CONSUMING, true);
    properties.put(SEGMENT_RELOCATOR_MINIMIZE_DATA_MOVEMENT, "DISABLE");
    properties.put(SEGMENT_RELOCATOR_BATCH_SIZE_PER_SERVER, 42);

    ControllerConf conf = new ControllerConf(properties);
    Assert.assertTrue(conf.getSegmentRelocatorReassignInstances());
    Assert.assertFalse(conf.getSegmentRelocatorBootstrap());
    Assert.assertTrue(conf.getSegmentRelocatorDowntime());
    Assert.assertEquals(conf.getSegmentRelocatorMinAvailableReplicas(), -2);
    Assert.assertTrue(conf.getSegmentRelocatorBestEfforts());
    Assert.assertTrue(conf.isSegmentRelocatorIncludingConsuming());
    Assert.assertEquals(conf.getSegmentRelocatorMinimizeDataMovement(), Enablement.DISABLE);
    Assert.assertEquals(conf.getSegmentRelocatorBatchSizePerServer(), 42);
  }

  @Test
  public void testGetDisasterRecoveryMode() {
    Map<String, Object> controllerConfig = new HashMap<>();
    ControllerConf conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getDisasterRecoveryMode(), DisasterRecoveryMode.DEFAULT);

    controllerConfig = new HashMap<>();
    controllerConfig.put(DISASTER_RECOVERY_MODE_CONFIG_KEY, "ALWAYS");
    conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getDisasterRecoveryMode(), DisasterRecoveryMode.ALWAYS);
  }

  @Test
  public void shouldBeAbleToDisableUsingNewConfig() {
    Map<String, Object> controllerConfig = new HashMap<>();
    ControllerConf conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getTaskManagerFrequencyInSeconds(), -1);

    controllerConfig = new HashMap<>();
    controllerConfig.put(TASK_MANAGER_FREQUENCY_PERIOD, "0s");
    conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getTaskManagerFrequencyInSeconds(), 0);

    controllerConfig = new HashMap<>();
    controllerConfig.put(TASK_MANAGER_FREQUENCY_PERIOD, "-1s");
    conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getTaskManagerFrequencyInSeconds(), -1);
  }

  @Test
  public void shouldBeAbleToSetDataDir() {
    Map<String, Object> controllerConfig = new HashMap<>();
    ControllerConf conf = new ControllerConf(controllerConfig);
    Assert.assertEquals(conf.getDataDir(), null);

    // test for the dataDir s3 value with ending slash
    conf.setDataDir("s3://<bucket_name>/controller/");
    Assert.assertEquals(conf.getDataDir(), "s3://<bucket_name>/controller");

    // test for the dataDir s3 value without ending slash
    conf.setDataDir("s3://<bucket_name>/controller");
    Assert.assertEquals(conf.getDataDir(), "s3://<bucket_name>/controller");

    // test for the dataDir non-s3 value without ending slash
    conf.setDataDir("/tmp/PinotController");
    Assert.assertEquals(conf.getDataDir(), "/tmp/PinotController");

    // test for the dataDir non-s3 value with ending slash
    conf.setDataDir("/tmp/PinotController/");
    Assert.assertEquals(conf.getDataDir(), "/tmp/PinotController");
  }

  private void assertOnDurations(ControllerConf conf, long expectedDuration, Map<String, Object> controllerConfig) {
    int segmentLevelValidationIntervalInSeconds = conf.getSegmentLevelValidationIntervalInSeconds();
    int segmentRelocatorFrequencyInSeconds = conf.getSegmentRelocatorFrequencyInSeconds();
    int taskMetricsEmitterFrequencyInSeconds = conf.getTaskMetricsEmitterFrequencyInSeconds();
    int taskManagerFrequencyInSeconds = conf.getTaskManagerFrequencyInSeconds();
    int statusCheckerFrequencyInSeconds = conf.getStatusCheckerFrequencyInSeconds();
    int brokerResourceValidationFrequencyInSeconds = conf.getBrokerResourceValidationFrequencyInSeconds();
    int realtimeSegmentValidationFrequencyInSeconds = conf.getRealtimeSegmentValidationFrequencyInSeconds();
    int offlineSegmentIntervalCheckerFrequencyInSeconds = conf.getOfflineSegmentIntervalCheckerFrequencyInSeconds();
    int retentionControllerFrequencyInSeconds = conf.getRetentionControllerFrequencyInSeconds();
    int statusCheckerWaitForPushTimeInSeconds = conf.getStatusCheckerWaitForPushTimeInSeconds();
    //then
    String confAsString = controllerConfig.toString();
    Assert.assertEquals(segmentLevelValidationIntervalInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(segmentRelocatorFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(taskMetricsEmitterFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(taskManagerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(statusCheckerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(brokerResourceValidationFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(realtimeSegmentValidationFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(offlineSegmentIntervalCheckerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(retentionControllerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(statusCheckerWaitForPushTimeInSeconds, expectedDuration, confAsString);
  }

  private String getRandomPeriodInMinutes() {
    return getRandomMinutes() + "m";
  }

  private int getRandomMinutes() {
    return 1 + RAND.nextInt(10);
  }

  @Test
  public void invalidFrequencyPeriodShouldFallBackToDefault() {
    Map<String, Object> controllerConfig = new HashMap<>();
    controllerConfig.put(RETENTION_MANAGER_FREQUENCY_PERIOD, "notAValidPeriod");
    ControllerConf conf = new ControllerConf(controllerConfig);
    // Invalid period should fall back to the default value
    Assert.assertEquals(conf.getRetentionControllerFrequencyInSeconds(),
        (int) TimeUnit.SECONDS.convert(
            TimeUtils.convertPeriodToMillis(DEFAULT_RETENTION_MANAGER_FREQUENCY_PERIOD), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testTaskQueueBoundingConfigDefaults() {
    ControllerConf conf = new ControllerConf();

    // Verify default values
    Assert.assertEquals(conf.getPinotTaskExpireTimeInMs(), 24 * 60 * 60 * 1000L);
    Assert.assertEquals(conf.getPinotTaskTerminalStateExpireTimeInMs(), 72 * 60 * 60 * 1000L);
    Assert.assertEquals(conf.getPinotTaskQueueMaxSize(), -1);
    Assert.assertEquals(conf.getPinotTaskQueueMaxDeletesPerCycle(), 100);
    Assert.assertEquals(conf.getPinotTaskQueueCapacity(), -1);
    Assert.assertEquals(conf.getPinotTaskQueueWarningThreshold(), 5000);
  }

  @Test
  public void testTaskQueueBoundingConfigCustomValues() {
    ControllerConf conf = new ControllerConf();
    conf.setProperty(PINOT_TASK_TERMINAL_STATE_EXPIRE_TIME_MS, 48 * 60 * 60 * 1000L);
    conf.setProperty(PINOT_TASK_QUEUE_MAX_SIZE, 5000);
    conf.setProperty(PINOT_TASK_QUEUE_MAX_DELETES_PER_CYCLE, 100);
    conf.setProperty(PINOT_TASK_QUEUE_CAPACITY, 10000);
    conf.setProperty(PINOT_TASK_QUEUE_WARNING_THRESHOLD, 8000);

    Assert.assertEquals(conf.getPinotTaskTerminalStateExpireTimeInMs(), 48 * 60 * 60 * 1000L);
    Assert.assertEquals(conf.getPinotTaskQueueMaxSize(), 5000);
    Assert.assertEquals(conf.getPinotTaskQueueMaxDeletesPerCycle(), 100);
    Assert.assertEquals(conf.getPinotTaskQueueCapacity(), 10000);
    Assert.assertEquals(conf.getPinotTaskQueueWarningThreshold(), 8000);
  }
}
