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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.*;


public class ControllerConfTest {

  private static final List<String> DEPRECATED_CONFIGS = Arrays
      .asList(DEPRECATED_RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
          DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
          DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
          DEPRECATED_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
          DEPRECATED_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS, DEPRECATED_STATUS_CHECKER_FREQUENCY_IN_SECONDS,
          DEPRECATED_TASK_MANAGER_FREQUENCY_IN_SECONDS, DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS,
          DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
          DEPRECATED_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS, DEPRECATED_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS,
          DEPRECATED_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS,
          DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS,
          DEPRECATED_STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS);

  private static final List<String> NEW_CONFIGS = Arrays
      .asList(RETENTION_MANAGER_FREQUENCY_PERIOD, OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD,
          REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD,
          STATUS_CHECKER_FREQUENCY_PERIOD, TASK_MANAGER_FREQUENCY_PERIOD,
          MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD,
          MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD, TASK_METRICS_EMITTER_FREQUENCY_PERIOD,
          SEGMENT_RELOCATOR_FREQUENCY_PERIOD, SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD,
          STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD);

  private static final Random RAND = new Random();

  /**
   * When config contains: 1. Both deprecated config and the corresponding new config. 2. All new
   * configurations are valid. 3. Some deprecated configurations are invalid, then new configs
   * override deprecated configs (invalid deprecated configs do not throw exceptions when
   * corresponding valid new configs are supplied as well)
   */
  @Test
  public void validNewConfigOverridesCorrespondingValidOrInvalidOldConfigOnRead() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    int durationInSeconds = getRandomDurationInSeconds();
    DEPRECATED_CONFIGS.forEach(config -> controllerConfig.put(config, durationInSeconds));
    //put some invalid deprecated configs
    controllerConfig.put(DEPRECATED_RETENTION_MANAGER_FREQUENCY_IN_SECONDS, getRandomString());
    controllerConfig.put(DEPRECATED_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS, getRandomString());
    //override all deprecated configs with valid new configs
    String period = getRandomPeriodInMinutes();
    NEW_CONFIGS.forEach(config -> controllerConfig.put(config, period));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS),
        controllerConfig);
  }

  /**
   * When config contains: 1. Both deprecated config and the corresponding new config. 2. All
   * deprecated configurations are valid. 3. Some new configurations are invalid, then exceptions
   * are thrown when invalid new configurations are read (there is no fall-back to the corresponding
   * valid deprecated configuration). For all valid new configurations, they override the
   * corresponding deprecated configuration.
   */
  @Test
  public void invalidNewConfigShouldThrowExceptionOnReadWithoutFallbackToCorrespondingValidDeprecatedConfig() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    int durationInSeconds = getRandomDurationInSeconds();
    //all deprecated configs should be valid
    DEPRECATED_CONFIGS.forEach(config -> controllerConfig.put(config, durationInSeconds));
    String randomPeriodInMinutes = getRandomPeriodInMinutes();
    NEW_CONFIGS.forEach(config -> controllerConfig.put(config, randomPeriodInMinutes));
    //put some invalid new configs
    controllerConfig.put(RETENTION_MANAGER_FREQUENCY_PERIOD, getRandomString());
    ControllerConf conf = new ControllerConf(controllerConfig);
    Assert.assertThrows(IllegalArgumentException.class, conf::getRetentionControllerFrequencyInSeconds);
  }

  /**
   * When only deprecated configs are supplied (new configs are not supplied), then the correct
   * converted value is returned.
   */
  @Test
  public void supplyingOnlyDeprecatedConfigsShouldReturnCorrectlyConvertedValue() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    int durationInSeconds = getRandomDurationInSeconds();
    DEPRECATED_CONFIGS.forEach(config -> {
      controllerConfig.put(config, durationInSeconds);
    });
    //pre-conditions: config should not contain any new config
    NEW_CONFIGS.forEach(config -> Assert.assertFalse(controllerConfig.containsKey(config)));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, durationInSeconds, controllerConfig);
  }

  /**
   * When only new configs are supplied (deprecated configs are not supplied), then the correct
   * converted value is returned.
   */
  @Test
  public void supplyingOnlyNewConfigsShouldReturnCorrectlyConvertedValue() {
    //setup
    Map<String, Object> controllerConfig = new HashMap<>();
    String period = getRandomPeriodInMinutes();
    NEW_CONFIGS.forEach(config -> controllerConfig.put(config, period));
    //pre-conditions: controller config should not contain any deprecated config
    DEPRECATED_CONFIGS.forEach(config -> Assert.assertFalse(controllerConfig.containsKey(config)));
    ControllerConf conf = new ControllerConf(controllerConfig);
    //execution and assertion
    assertOnDurations(conf, TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS),
        controllerConfig);
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
    int minionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds =
        conf.getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds();
    long minionInstancesCleanupTaskFrequencyInSeconds = conf.getMinionInstancesCleanupTaskFrequencyInSeconds();
    int taskManagerFrequencyInSeconds = conf.getTaskManagerFrequencyInSeconds();
    int statusCheckerFrequencyInSeconds = conf.getStatusCheckerFrequencyInSeconds();
    int brokerResourceValidationFrequencyInSeconds = conf.getBrokerResourceValidationFrequencyInSeconds();
    int realtimeSegmentValidationFrequencyInSeconds = conf.getRealtimeSegmentValidationFrequencyInSeconds();
    int offlineSegmentIntervalCheckerFrequencyInSeconds = conf.getOfflineSegmentIntervalCheckerFrequencyInSeconds();
    int retentionControllerFrequencyInSeconds = conf.getRetentionControllerFrequencyInSeconds();
    //then
    String confAsString = controllerConfig.toString();
    Assert.assertEquals(segmentLevelValidationIntervalInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(segmentRelocatorFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(taskMetricsEmitterFrequencyInSeconds, expectedDuration, confAsString);
    Assert
        .assertEquals(minionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(minionInstancesCleanupTaskFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(taskManagerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(statusCheckerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(brokerResourceValidationFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(realtimeSegmentValidationFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(offlineSegmentIntervalCheckerFrequencyInSeconds, expectedDuration, confAsString);
    Assert.assertEquals(retentionControllerFrequencyInSeconds, expectedDuration, confAsString);
  }

  private int getRandomDurationInSeconds() {
    return RAND.nextInt(50);
  }

  private String getRandomPeriodInMinutes() {
    return getRandomMinutes() + "m";
  }

  private int getRandomMinutes() {
    return 1 + RAND.nextInt(10);
  }

  private String getRandomString() {
    return RandomStringUtils.randomAlphanumeric(5);
  }
}
