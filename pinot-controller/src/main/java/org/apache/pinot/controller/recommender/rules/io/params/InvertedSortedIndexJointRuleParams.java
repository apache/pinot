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
  public Double _thresholdRatioMinGainDiffBetweenIteration =
      DEFAULT_THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION;

  // When we do not have a valid gain for MAX_NUM_ITERATION_WITHOUT_GAIN of iterations
  // The process will stop because adding more indices does not bring down the nESI
  public Integer _maxNumIterationWithoutGain = DEFAULT_MAX_NUM_ITERATION_WITHOUT_GAIN;

  // Algorithm will increase the number of recommended indices for AND predicate if
  // the nESI saved by adding one more index exceeds THRESHOLD_AND_PREDICATE_INCREMENTAL_VOTE
  public Double _thresholdMinAndPredicateIncrementalVote = DEFAULT_THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE;

  // For AND connected predicates, iff the nESI saved of top N-th candidate is larger than
  // THRESHOLD_AND_PREDICATE_TOP_VOTES * nESI_saved_of_top_one_candidate
  // then candidates [1st, nth] will from a exclusive vote
  // Meaning that during the overall vote counting, only one candidate can be counted
  public Double _thresholdRatioMinAndPredicateTopCandidates =
      DEFAULT_THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES;

  // In the over all recommendation for sorted and inverted indices, iff the nESI saved of top N-th candidate is
  // larger than
  // THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES * nESI_saved_of_top_one_candidate,
  // we will pick from [1st, nth] candidates with the largest cardinality as sorted index
  public Double _thresholdRatioMinNesiForTopCandidates = DEFAULT_THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES;

  // For the predicates it is impractical to estimate the portion
  // of documents selected out. Thus we use default values.
  public Double _percentSelectForFunction = DEFAULT_PERCENT_SELECT_FOR_FUNCTION;
  public Double _percentSelectForTextMatch = DEFAULT_PERCENT_SELECT_FOR_TEXT_MATCH;
  public Double _percentSelectForRange = DEFAULT_PERCENT_SELECT_FOR_RANGE;
  public Double _percentSelectForRegex = DEFAULT_PERCENT_SELECT_FOR_REGEX;
  public Double _percentSelectForIsnull = DEFAULT_PERCENT_SELECT_FOR_ISNULL;

  public Double getThresholdRatioMinNesiForTopCandidates() {
    return _thresholdRatioMinNesiForTopCandidates;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES", nulls = Nulls.SKIP)
  public void setThresholdRatioMinNesiForTopCandidates(Double thresholdRatioMinNesiForTopCandidates) {
    _thresholdRatioMinNesiForTopCandidates = thresholdRatioMinNesiForTopCandidates;
  }

  public Double getThresholdRatioMinGainDiffBetweenIteration() {
    return _thresholdRatioMinGainDiffBetweenIteration;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION", nulls = Nulls.SKIP)
  public void setThresholdRatioMinGainDiffBetweenIteration(
      Double thresholdRatioMinGainDiffBetweenIteration) {
    _thresholdRatioMinGainDiffBetweenIteration = thresholdRatioMinGainDiffBetweenIteration;
  }

  public Integer getMaxNumIterationWithoutGain() {
    return _maxNumIterationWithoutGain;
  }

  @JsonSetter(value = "MAX_NUM_ITERATION_WITHOUT_GAIN", nulls = Nulls.SKIP)
  public void setMaxNumIterationWithoutGain(Integer maxNumIterationWithoutGain) {
    _maxNumIterationWithoutGain = maxNumIterationWithoutGain;
  }

  public Double getThresholdMinAndPredicateIncrementalVote() {
    return _thresholdMinAndPredicateIncrementalVote;
  }

  @JsonSetter(value = "THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE", nulls = Nulls.SKIP)
  public void setThresholdMinAndPredicateIncrementalVote(Double thresholdMinAndPredicateIncrementalVote) {
    _thresholdMinAndPredicateIncrementalVote = thresholdMinAndPredicateIncrementalVote;
  }

  public Double getThresholdRatioMinAndPredicateTopCandidates() {
    return _thresholdRatioMinAndPredicateTopCandidates;
  }

  @JsonSetter(value = "THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES", nulls = Nulls.SKIP)
  public void setThresholdRatioMinAndPredicateTopCandidates(
      Double thresholdRatioMinAndPredicateTopCandidates) {
    _thresholdRatioMinAndPredicateTopCandidates = thresholdRatioMinAndPredicateTopCandidates;
  }

  public Double getPercentSelectForFunction() {
    return _percentSelectForFunction;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_FUNCTION", nulls = Nulls.SKIP)
  public void setPercentSelectForFunction(Double percentSelectForFunction) {
    _percentSelectForFunction = percentSelectForFunction;
  }

  public Double getPercentSelectForTextMatch() {
    return _percentSelectForTextMatch;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_TEXT_MATCH", nulls = Nulls.SKIP)
  public void setPercentSelectForTextMatch(Double percentSelectForTextMatch) {
    _percentSelectForTextMatch = percentSelectForTextMatch;
  }

  public Double getPercentSelectForRange() {
    return _percentSelectForRange;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_RANGE", nulls = Nulls.SKIP)
  public void setPercentSelectForRange(Double percentSelectForRange) {
    _percentSelectForRange = percentSelectForRange;
  }

  public Double getPercentSelectForRegex() {
    return _percentSelectForRegex;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_REGEX", nulls = Nulls.SKIP)
  public void setPercentSelectForRegex(Double percentSelectForRegex) {
    _percentSelectForRegex = percentSelectForRegex;
  }

  public Double getPercentSelectForIsnull() {
    return _percentSelectForIsnull;
  }

  @JsonSetter(value = "PERCENT_SELECT_FOR_ISNULL", nulls = Nulls.SKIP)
  public void setPercentSelectForIsnull(Double percentSelectForIsnull) {
    _percentSelectForIsnull = percentSelectForIsnull;
  }
}
