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
  public Long THRESHOLD_MAX_LATENCY_SLA_PARTITION = DEFAULT_THRESHOLD_MAX_LATENCY_SLA_PARTITION;

  // Below this QPS we do not need any partitioning
  public Long THRESHOLD_MIN_QPS_PARTITION = DEFAULT_THRESHOLD_MIN_QPS_PARTITION;

  // In the over all recommendation for partitioning, iff the frequency of top N-th candidate is larger than
  // THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES * frequency_of_top_one_candidate,
  // we will pick from [1st, nth] candidates with the largest cardinality as partitioning column
  public Double THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES =
      DEFAULT_THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES;

  // For IN predicate with literals larger than this we will not recommend partitioning on that
  // column because the query will fan out to  a large number of partitions,
  // making partitioning pointless
  public Integer THRESHOLD_MAX_IN_LENGTH = DEFAULT_THRESHOLD_MAX_IN_LENGTH;

  // The desirable msgs/sec for each kafka partition
  public Long KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION = DEFAULT_KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION;

  public Integer getTHRESHOLD_MAX_IN_LENGTH() {
    return THRESHOLD_MAX_IN_LENGTH;
  }

  @JsonSetter(value = "THRESHOLD_MAX_IN_LENGTH", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MAX_IN_LENGTH(Integer THRESHOLD_MAX_IN_LENGTH) {
    this.THRESHOLD_MAX_IN_LENGTH = THRESHOLD_MAX_IN_LENGTH;
  }

  public Long getTHRESHOLD_MAX_LATENCY_SLA_PARTITION() {
    return THRESHOLD_MAX_LATENCY_SLA_PARTITION;
  }

  @JsonSetter(value = "THRESHOLD_MAX_LATENCY_SLA_PARTITION", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MAX_LATENCY_SLA_PARTITION(long THRESHOLD_MAX_LATENCY_SLA_PARTITION) {
    this.THRESHOLD_MAX_LATENCY_SLA_PARTITION = THRESHOLD_MAX_LATENCY_SLA_PARTITION;
  }

  public Long getTHRESHOLD_MIN_QPS_PARTITION() {
    return THRESHOLD_MIN_QPS_PARTITION;
  }

  @JsonSetter(value = "THRESHOLD_MIN_QPS_PARTITION", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_QPS_PARTITION(long THRESHOLD_MIN_QPS_PARTITION) {
    this.THRESHOLD_MIN_QPS_PARTITION = THRESHOLD_MIN_QPS_PARTITION;
  }

  public Double getTHRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES() {
    return THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES", nulls = Nulls.SKIP)
  public void setTHRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES(
      Double THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES) {
    this.THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES =
        THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES;
  }

  public Long getKAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION() {
    return KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION;
  }

  @JsonSetter(value = "KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION", nulls = Nulls.SKIP)
  public void setKAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION(long KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION) {
    this.KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION = KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION;
  }
}