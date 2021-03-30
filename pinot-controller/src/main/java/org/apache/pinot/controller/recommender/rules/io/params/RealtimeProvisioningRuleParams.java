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

package org.apache.pinot.controller.recommender.rules.io.params;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


/**
 * Parameters used in RealtimeProvisioningRule
 */
public class RealtimeProvisioningRuleParams {

  // Number of partitions for the topic
  private int numPartitions;

  // Number of replicas
  private int numReplicas;

  // Retention hours for the segment of realtime table
  private int realtimeTableRetentionHours =
      RecommenderConstants.RealtimeProvisioningRule.DEFAULT_REAL_TIME_TABLE_RETENTION_HOURS;

  // Available memory of the host
  private String maxUsableHostMemory =
      RecommenderConstants.RealtimeProvisioningRule.DEFAULT_MAX_USABLE_HOST_MEMORY;

  // Different values for `number of hosts` parameter
  private int[] numHosts = RecommenderConstants.RealtimeProvisioningRule.DEFAULT_NUM_HOSTS;

  // Acceptable values for `number of hours` parameter. NumHours represents consumption duration.
  private int[] numHours = RecommenderConstants.RealtimeProvisioningRule.DEFAULT_NUM_HOURS;


  // Getters & Setters

  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  public void setNumReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
  }

  public int getRealtimeTableRetentionHours() {
    return realtimeTableRetentionHours;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRealtimeTableRetentionHours(int realtimeTableRetentionHours) {
    this.realtimeTableRetentionHours = realtimeTableRetentionHours;
  }

  public String getMaxUsableHostMemory() {
    return maxUsableHostMemory;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setMaxUsableHostMemory(String maxUsableHostMemory) {
    this.maxUsableHostMemory = maxUsableHostMemory;
  }

  public int[] getNumHosts() {
    return numHosts;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumHosts(int[] numHosts) {
    this.numHosts = numHosts;
  }

  public int[] getNumHours() {
    return numHours;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumHours(int[] numHours) {
    this.numHours = numHours;
  }
}
