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
package org.apache.pinot.controller.recommender.rules.utils;

/**
 * Class holding a candidate recommendation
 */
public class PredicateParseResult {
  public static final class PredicateParseResultBuilder {
    // recommended set of dimensions (as a whole) to apply indices on
    FixedLenBitset _candidateDims;
    QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum _iteratorEvalPriorityEnum;
    QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum _recommendationPriorityEnum;
    double _nESI; // num entries scanned in filter without recommended indices
    double _nESIWithIdx; // num entries scanned in filter with recommended indices
    double _percentSelected; // the percentage of document selected by such (sub-)predicate, in fraction

    public static PredicateParseResultBuilder aPredicateParseResult() {
      return new PredicateParseResultBuilder();
    }

    public PredicateParseResultBuilder setCandidateDims(FixedLenBitset candidateDims) {
      _candidateDims = candidateDims;
      return this;
    }

    public PredicateParseResultBuilder setIteratorEvalPriorityEnum(
        QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum iteratorEvalPriorityEnum) {
      _iteratorEvalPriorityEnum = iteratorEvalPriorityEnum;
      return this;
    }

    public PredicateParseResultBuilder setRecommendationPriorityEnum(
        QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum recommendationPriorityEnum) {
      _recommendationPriorityEnum = recommendationPriorityEnum;
      return this;
    }

    public PredicateParseResultBuilder setnESI(double nESI) {
      _nESI = nESI;
      return this;
    }

    public PredicateParseResultBuilder setPercentSelected(double percentSelected) {
      _percentSelected = percentSelected;
      return this;
    }

    public PredicateParseResultBuilder setnESIWithIdx(double nESIWithIdx) {
      _nESIWithIdx = nESIWithIdx;
      return this;
    }

    public PredicateParseResultBuilder setQueryWeight(double queryWeight) {
      _nESI *= queryWeight;
      _nESIWithIdx *= queryWeight;
      return this;
    }

    public PredicateParseResult build() {
      PredicateParseResult predicateParseResult = new PredicateParseResult();
      predicateParseResult._candidateDims = this._candidateDims;
      predicateParseResult._percentSelected = this._percentSelected;
      predicateParseResult._nESI = this._nESI;
      predicateParseResult._iteratorEvalPriorityEnum = this._iteratorEvalPriorityEnum;
      predicateParseResult._recommendationPriorityEnum = this._recommendationPriorityEnum;
      predicateParseResult._nESIWithIdx = this._nESIWithIdx;
      return predicateParseResult;
    }
  }

  public static final double PERCENT_SELECT_ALL = 1;
  public static final double PERCENT_SELECT_ZERO = 0;
  public static final double NESI_ZERO = 0;
  public static final double NESI_ONE = 1;


  FixedLenBitset _candidateDims;
  QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum _iteratorEvalPriorityEnum;
  QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum _recommendationPriorityEnum;
  double _nESI;
  double _nESIWithIdx;
  double _percentSelected;

  @Override
  public String toString() {
    return "PredicateParseResult{" + "dims" + _candidateDims + ", "
        + _iteratorEvalPriorityEnum + ", " + _recommendationPriorityEnum + ", nESI=" + String.format("%.3f", _nESI)
        + ", selected=" +  String.format("%.3f", _percentSelected)+ ", nESIWithIdx=" +  String.format("%.3f", _nESIWithIdx) + '}';
  }

  public boolean hasCandidateDim() {
    return _candidateDims.hasCandidateDim();
  }

  public boolean isBitmapConvertable() {
    return _recommendationPriorityEnum == QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum.BITMAP
        || _recommendationPriorityEnum == QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum.CANDIDATE_SCAN;
  }

  public FixedLenBitset getCandidateDims() {
    return _candidateDims;
  }

  public QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum getIteratorEvalPriority() {
    return _iteratorEvalPriorityEnum;
  }

  public QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum getRecommendationPriorityEnum() {
    return _recommendationPriorityEnum;
  }

  public double getnESI() {
    return _nESI;
  }

  public double getPercentSelected() {
    return _percentSelected;
  }

  public double getnESIWithIdx() {
    return _nESIWithIdx;
  }

  public PredicateParseResult multiplyWeight(double queryWeight) {
    this._nESI *= queryWeight;
    this._nESIWithIdx *= queryWeight;
    return this;
  }

  public static PredicateParseResult EMPTY_PREDICATE_PARSE_RESULT() {
    return PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
        .setIteratorEvalPriorityEnum(QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum.INDEXED)
        .setRecommendationPriorityEnum(QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum.NON_CANDIDATE_SCAN).setnESI(NESI_ZERO)
        .setPercentSelected(PERCENT_SELECT_ZERO).setnESIWithIdx(NESI_ZERO).build();
  }
}