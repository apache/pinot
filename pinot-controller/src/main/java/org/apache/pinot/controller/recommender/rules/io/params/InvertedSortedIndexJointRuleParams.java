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

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.InvertedSortedIndexJointRule.*;

/**
 * Thresholds and parameters used in InvertedSortedIndexJointRule
 */
public class InvertedSortedIndexJointRuleParams {

  // When the number of indices we recommend increment 1,
  // the corresponding nESI saved should be > THRESHOLD_GAIN_DIFF_BETWEEN_ITERATION * totalNESI
  // to be consider a valid gain
  public Double THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION = DEFAULT_THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION;

  // When we do not have a valid gain for MAX_NUM_ITERATION_WITHOUT_GAIN of iterations
  // The process will stop because adding more indices does not bring down the nESI
  public Integer MAX_NUM_ITERATION_WITHOUT_GAIN = DEFAULT_MAX_NUM_ITERATION_WITHOUT_GAIN;

  // Algorithm will increase the number of recommended indices for AND predicate if
  // the nESI saved by adding one more index exceeds THRESHOLD_AND_PREDICATE_INCREMENTAL_VOTE
  public Double THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE = DEFAULT_THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE;

  // For AND connected predicates, iff the nESI saved of top N-th candidate is larger than
  // THRESHOLD_AND_PREDICATE_TOP_VOTES * nESI_saved_of_top_one_candidate
  // then candidates [1st, nth] will from a exclusive vote
  // Meaning that during the overall vote counting, only one candidate can be counted
  public Double THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES = DEFAULT_THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES;

  // In the over all recommendation for sorted and inverted indices, iff the nESI saved of top N-th candidate is larger than
  // THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES * nESI_saved_of_top_one_candidate,
  // we will pick from [1st, nth] candidates with the largest cardinality as sorted index
  public Double THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES = DEFAULT_THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES;

  // For the predicates it is impractical to estimate the portion
  // of documents selected out. Thus we use default values.
  public Double PERCENT_SELECT_FOR_FUNCTION = DEFAULT_PERCENT_SELECT_FOR_FUNCTION;
  public Double PERCENT_SELECT_FOR_TEXT_MATCH = DEFAULT_PERCENT_SELECT_FOR_TEXT_MATCH;
  public Double PERCENT_SELECT_FOR_RANGE = DEAFULT_PERCENT_SELECT_FOR_RANGE;
  public Double PERCENT_SELECT_FOR_REGEX = DEAFULT_PERCENT_SELECT_FOR_REGEX;
  public Double PERCENT_SELECT_FOR_ISNULL = DEFAULT_PERCENT_SELECT_FOR_ISNULL;


  public Double getTHRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES() {
    return THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES", nulls = Nulls.SKIP)
  public void setTHRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES(Double THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES) {
    this.THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES = THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES;
  }

  public Double getTHRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION() {
    return THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION", nulls = Nulls.SKIP)
  public void setTHRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION(Double THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION) {
    this.THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION = THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION;
  }

  public Integer getMAX_NUM_ITERATION_WITHOUT_GAIN() {
    return MAX_NUM_ITERATION_WITHOUT_GAIN;
  }

  @JsonSetter(value = "MAX_NUM_ITERATION_WITHOUT_GAIN", nulls = Nulls.SKIP)
  public void setMAX_NUM_ITERATION_WITHOUT_GAIN(Integer MAX_NUM_ITERATION_WITHOUT_GAIN) {
    this.MAX_NUM_ITERATION_WITHOUT_GAIN = MAX_NUM_ITERATION_WITHOUT_GAIN;
  }

  public Double getTHRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE() {
    return THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE;
  }

  @JsonSetter(value = "THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE(Double THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE) {
    this.THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE = THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE;
  }

  public Double getTHRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES() {
    return THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_VOTES", nulls = Nulls.SKIP)
  public void setTHRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES(Double THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES) {
    this.THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES = THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES;
  }

  public Double getPERCENT_SELECT_FOR_FUNCTION() {
    return PERCENT_SELECT_FOR_FUNCTION;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_FUNCTION", nulls = Nulls.SKIP)
  public void setPERCENT_SELECT_FOR_FUNCTION(Double PERCENT_SELECT_FOR_FUNCTION) {
    this.PERCENT_SELECT_FOR_FUNCTION = PERCENT_SELECT_FOR_FUNCTION;
  }

  public Double getPERCENT_SELECT_FOR_TEXT_MATCH() {
    return PERCENT_SELECT_FOR_TEXT_MATCH;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_TEXT_MATCH", nulls = Nulls.SKIP)
  public void setPERCENT_SELECT_FOR_TEXT_MATCH(Double PERCENT_SELECT_FOR_TEXT_MATCH) {
    this.PERCENT_SELECT_FOR_TEXT_MATCH = PERCENT_SELECT_FOR_TEXT_MATCH;
  }

  public Double getPERCENT_SELECT_FOR_RANGE() {
    return PERCENT_SELECT_FOR_RANGE;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_RANGE", nulls = Nulls.SKIP)
  public void setPERCENT_SELECT_FOR_RANGE(Double PERCENT_SELECT_FOR_RANGE) {
    this.PERCENT_SELECT_FOR_RANGE = PERCENT_SELECT_FOR_RANGE;
  }

  public Double getPERCENT_SELECT_FOR_REGEX() {
    return PERCENT_SELECT_FOR_REGEX;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_REGEX", nulls = Nulls.SKIP)
  public void setPERCENT_SELECT_FOR_REGEX(Double PERCENT_SELECT_FOR_REGEX) {
    this.PERCENT_SELECT_FOR_REGEX = PERCENT_SELECT_FOR_REGEX;
  }

  public Double getPERCENT_SELECT_FOR_ISNULL() {
    return PERCENT_SELECT_FOR_ISNULL;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_ISNULL", nulls = Nulls.SKIP)
  public void setPERCENT_SELECT_FOR_ISNULL(Double PERCENT_SELECT_FOR_ISNULL) {
    this.PERCENT_SELECT_FOR_ISNULL = PERCENT_SELECT_FOR_ISNULL;
  }
}