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

package org.apache.pinot.thirdeye.anomaly.merge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;


/**
 * Merge Configuration to hint merge module to fetch anomalies by specific group and apply certain
 * merge rule
 * <p/>
 * Check following merge parameters:
 * <p/>
 * mergeStrategy : dictates how anomalies should be grouped
 * <p/>
 * sequentialAllowedGap: allowed gap in milli seconds between sequential anomalies
 * <p/>
 * mergeDuration : length of the merged anomaly in milli seconds
 * <p/>
 * mergeablePropertyKeys : allow anomalies to be compared using function specified mergeablePropertyKeys
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalyMergeConfig {
  private AnomalyMergeStrategy mergeStrategy = AnomalyMergeStrategy.FUNCTION;
  private long sequentialAllowedGap = 30_000; // 30 seconds

  // Look back for 2 days to accommodate merging of anomalies in daily metrics
  private long maxMergeDurationLength = 48 * 60 * 60 * 1000; // 48 hours

  private List<String> mergeablePropertyKeys = new ArrayList<>();

  public AnomalyMergeConfig() {
  }

  public AnomalyMergeConfig(AnomalyMergeStrategy mergeStrategy, long sequentialAllowedGap, long maxMergeDurationLength, List<String> mergeablePropertyKeys) {
    this.mergeStrategy = mergeStrategy;
    this.sequentialAllowedGap = sequentialAllowedGap;
    this.maxMergeDurationLength = maxMergeDurationLength;
    this.mergeablePropertyKeys = mergeablePropertyKeys;
  }

  public AnomalyMergeStrategy getMergeStrategy() {
    return mergeStrategy;
  }

  public void setMergeStrategy(AnomalyMergeStrategy mergeStrategy) {
    this.mergeStrategy = mergeStrategy;
  }

  public long getSequentialAllowedGap() {
    return sequentialAllowedGap;
  }

  public void setSequentialAllowedGap(long sequentialAllowedGap) {
    this.sequentialAllowedGap = sequentialAllowedGap;
  }

  public long getMaxMergeDurationLength() {
    return maxMergeDurationLength;
  }

  public void setMaxMergeDurationLength(long mergeDuration) {
    this.maxMergeDurationLength = mergeDuration;
  }

  public List<String> getMergeablePropertyKeys() {
    return mergeablePropertyKeys;
  }

  public void setMergeablePropertyKeys(List<String> mergeablePropertyKeys) {
    this.mergeablePropertyKeys = mergeablePropertyKeys;
  }
}