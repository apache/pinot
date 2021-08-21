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

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.PartitionRule.*;


/**
 * Thresholds and parameters used in PartitionRule
 */
public class PartitionRuleParams {
  // Above this latency SLA we do not need any partitioning
  public Long _thresholdMaxLatencySlaPartition = DEFAULT_THRESHOLD_MAX_LATENCY_SLA_PARTITION;

  // Below this QPS we do not need any partitioning
  public Long _thresholdMinQpsPartition = DEFAULT_THRESHOLD_MIN_QPS_PARTITION;

  // In the over all recommendation for partitioning, iff the frequency of top N-th candidate is larger than
  // THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES * frequency_of_top_one_candidate,
  // we will pick from [1st, nth] candidates with the largest cardinality as partitioning column
  public Double _thresholdRatioMinDimensionPartitionTopCandidates =
      DEFAULT_THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES;

  // For IN predicate with literals larger than this we will not recommend partitioning on that
  // column because the query will fan out to  a large number of partitions,
  // making partitioning pointless
  public Integer _thresholdMaxInLength = DEFAULT_THRESHOLD_MAX_IN_LENGTH;

  // The desirable msgs/sec for each kafka partition
  public Long _kafkaNumMessagesPerSecPerPartition = DEFAULT_KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION;

  public Integer getThresholdMaxInLength() {
    return _thresholdMaxInLength;
  }

  @JsonSetter(value = "THRESHOLD_MAX_IN_LENGTH", nulls = Nulls.SKIP)
  public void setThresholdMaxInLength(Integer thresholdMaxInLength) {
    _thresholdMaxInLength = thresholdMaxInLength;
  }

  public Long getThresholdMaxLatencySlaPartition() {
    return _thresholdMaxLatencySlaPartition;
  }

  @JsonSetter(value = "THRESHOLD_MAX_LATENCY_SLA_PARTITION", nulls = Nulls.SKIP)
  public void setThresholdMaxLatencySlaPartition(long thresholdMaxLatencySlaPartition) {
    _thresholdMaxLatencySlaPartition = thresholdMaxLatencySlaPartition;
  }

  public Long getThresholdMinQpsPartition() {
    return _thresholdMinQpsPartition;
  }

  @JsonSetter(value = "THRESHOLD_MIN_QPS_PARTITION", nulls = Nulls.SKIP)
  public void setThresholdMinQpsPartition(long thresholdMinQpsPartition) {
    _thresholdMinQpsPartition = thresholdMinQpsPartition;
  }

  public Double getThresholdRatioMinDimensionPartitionTopCandidates() {
    return _thresholdRatioMinDimensionPartitionTopCandidates;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES", nulls = Nulls.SKIP)
  public void setThresholdRatioMinDimensionPartitionTopCandidates(
      Double thresholdRatioMinDimensionPartitionTopCandidates) {
    _thresholdRatioMinDimensionPartitionTopCandidates = thresholdRatioMinDimensionPartitionTopCandidates;
  }

  public Long getKafkaNumMessagesPerSecPerPartition() {
    return _kafkaNumMessagesPerSecPerPartition;
  }

  @JsonSetter(value = "KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION", nulls = Nulls.SKIP)
  public void setKafkaNumMessagesPerSecPerPartition(long kafkaNumMessagesPerSecPerPartition) {
    _kafkaNumMessagesPerSecPerPartition = kafkaNumMessagesPerSecPerPartition;
  }
}
