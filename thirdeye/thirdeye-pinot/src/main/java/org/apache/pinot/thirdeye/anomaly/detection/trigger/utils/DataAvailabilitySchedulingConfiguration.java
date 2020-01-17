/*
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

package org.apache.pinot.thirdeye.anomaly.detection.trigger.utils;

import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Configuration class for DataAvailabilityListener.
 */
public class DataAvailabilitySchedulingConfiguration {
  private String consumerClass;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String kafkaConsumerGroupId;
  private String kafkaConsumerPropPath;
  private int numParallelConsumer = 1; // run one consumer by default
  private int datasetWhitelistUpdateFreqInMin = 30; // update every 30 minutes by default
  private long sleepTimeWhenNoEventInMilli = 30_000; // sleep for 30 secs when no event by default
  private long consumerPollTimeInMilli = 5_000; // consumer wait 5 secs by default for the buffer to be filled
  private List<String> dataSourceWhitelist;
  private List<String> filterClassList;
  // delay time after each run for the scheduler to reduce DB polling
  private long schedulerDelayInSec = TimeUnit.MINUTES.toSeconds(5);
  // default threshold if detection level threshold is not set
  private long taskTriggerFallBackTimeInSec = TimeUnit.DAYS.toSeconds(1);
  // scheduling window for data availability scheduling to avoid over-scheduling if watermarks do not move forward
  private long schedulingWindowInSec = TimeUnit.MINUTES.toSeconds(15);

  public String getConsumerClass() {
    return consumerClass;
  }

  public void setConsumerClass(String consumerClass) {
    this.consumerClass = consumerClass;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public void setKafkaTopic(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  public String getKafkaConsumerGroupId() {
    return kafkaConsumerGroupId;
  }

  public void setKafkaConsumerGroupId(String kafkaConsumerGroupId) {
    this.kafkaConsumerGroupId = kafkaConsumerGroupId;
  }

  public String getKafkaConsumerPropPath() {
    return kafkaConsumerPropPath;
  }

  public void setKafkaConsumerPropPath(String kafkaConsumerPropPath) {
    this.kafkaConsumerPropPath = kafkaConsumerPropPath;
  }

  public int getNumParallelConsumer() {
    return numParallelConsumer;
  }

  public void setNumParallelConsumer(int numParallelConsumer) {
    this.numParallelConsumer = numParallelConsumer;
  }

  public int getDatasetWhitelistUpdateFreqInMin() {
    return datasetWhitelistUpdateFreqInMin;
  }

  public void setDatasetWhitelistUpdateFreqInMin(int datasetWhitelistUpdateFreqInMin) {
    this.datasetWhitelistUpdateFreqInMin = datasetWhitelistUpdateFreqInMin;
  }

  public long getSleepTimeWhenNoEventInMilli() {
    return sleepTimeWhenNoEventInMilli;
  }

  public void setSleepTimeWhenNoEventInMilli(long sleepTimeWhenNoEventInMilli) {
    this.sleepTimeWhenNoEventInMilli = sleepTimeWhenNoEventInMilli;
  }

  public long getConsumerPollTimeInMilli() {
    return consumerPollTimeInMilli;
  }

  public void setConsumerPollTimeInMilli(long consumerPollTimeInMilli) {
    this.consumerPollTimeInMilli = consumerPollTimeInMilli;
  }

  public List<String> getDataSourceWhitelist() {
    return dataSourceWhitelist;
  }

  public void setDataSourceWhitelist(List<String> dataSourceWhitelist) {
    this.dataSourceWhitelist = dataSourceWhitelist;
  }

  public List<String> getFilterClassList() {
    return filterClassList;
  }

  public void setFilterClassList(List<String> filterClassList) {
    this.filterClassList = filterClassList;
  }

  public long getSchedulerDelayInSec() {
    return schedulerDelayInSec;
  }

  public void setSchedulerDelayInSec(long schedulerDelayInSec) {
    this.schedulerDelayInSec = schedulerDelayInSec;
  }

  public long getTaskTriggerFallBackTimeInSec() {
    return taskTriggerFallBackTimeInSec;
  }

  public void setTaskTriggerFallBackTimeInSec(long taskTriggerFallBackTimeInSec) {
    this.taskTriggerFallBackTimeInSec = taskTriggerFallBackTimeInSec;
  }

  public long getSchedulingWindowInSec() {
    return schedulingWindowInSec;
  }

  public void setSchedulingWindowInSec(long schedulingWindowInSec) {
    this.schedulingWindowInSec = schedulingWindowInSec;
  }
}
