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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


/**
 * Parameters used in RealtimeProvisioningRule
 */
public class RealtimeProvisioningRuleParams {

  // Number of partitions for the topic
  @JsonProperty("numPartitions")
  private int _numPartitions;

  // Number of replicas
  @JsonProperty("numReplicas")
  private int _numReplicas;

  // Retention hours for the segment of realtime table
  @JsonProperty("realtimeTableRetentionHours")
  private int _realtimeTableRetentionHours =
      RecommenderConstants.RealtimeProvisioningRule.DEFAULT_REAL_TIME_TABLE_RETENTION_HOURS;

  // Available memory of the host
  @JsonProperty("maxUsableHostMemory")
  private String _maxUsableHostMemory = RecommenderConstants.RealtimeProvisioningRule.DEFAULT_MAX_USABLE_HOST_MEMORY;

  // Different values for `number of hosts` parameter
  @JsonProperty("numHosts")
  private int[] _numHosts = RecommenderConstants.RealtimeProvisioningRule.DEFAULT_NUM_HOSTS;

  // Acceptable values for `number of hours` parameter. NumHours represents consumption duration.
  @JsonProperty("numHours")
  private int[] _numHours = RecommenderConstants.RealtimeProvisioningRule.DEFAULT_NUM_HOURS;

  // Number of rows for the segment that is going to be generated
  @JsonProperty("numRowsInGeneratedSegment")
  private int _numRowsInGeneratedSegment = RecommenderConstants.DEFAULT_NUM_ROWS_IN_GENERATED_SEGMENT;

  // Getters & Setters

  public int getNumPartitions() {
    return _numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
  }

  public int getNumReplicas() {
    return _numReplicas;
  }

  public void setNumReplicas(int numReplicas) {
    _numReplicas = numReplicas;
  }

  public int getRealtimeTableRetentionHours() {
    return _realtimeTableRetentionHours;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRealtimeTableRetentionHours(int realtimeTableRetentionHours) {
    _realtimeTableRetentionHours = realtimeTableRetentionHours;
  }

  public String getMaxUsableHostMemory() {
    return _maxUsableHostMemory;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setMaxUsableHostMemory(String maxUsableHostMemory) {
    _maxUsableHostMemory = maxUsableHostMemory;
  }

  public int[] getNumHosts() {
    return _numHosts;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumHosts(int[] numHosts) {
    _numHosts = numHosts;
  }

  public int[] getNumHours() {
    return _numHours;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumHours(int[] numHours) {
    _numHours = numHours;
  }

  public int getNumRowsInGeneratedSegment() {
    return _numRowsInGeneratedSegment;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumRowsInGeneratedSegment(int numRowsInGeneratedSegment) {
    _numRowsInGeneratedSegment = numRowsInGeneratedSegment;
  }
}
