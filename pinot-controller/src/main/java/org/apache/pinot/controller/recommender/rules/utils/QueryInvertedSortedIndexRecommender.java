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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.io.configs.IndexConfig;
import org.apache.pinot.controller.recommender.rules.io.params.InvertedSortedIndexJointRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.recommender.rules.utils.PredicateParseResult.NESI_ZERO;
import static org.apache.pinot.controller.recommender.rules.utils.PredicateParseResult.PERCENT_SELECT_ALL;
import static org.apache.pinot.controller.recommender.rules.utils.PredicateParseResult.PERCENT_SELECT_ZERO;


/**
 * A query parser to simulate the nESI cost of a given query in run-time, recommend an optimal set of dimensions to
 * apply indices on, and calculate the estimated nESI saved by applying such indices
 */
public class QueryInvertedSortedIndexRecommender {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryInvertedSortedIndexRecommender.class);
  private static final int NESTED_TOP_LEVEL = 0;
  private static final int NESTED_SECOND_LEVEL = 1;
  public static final List<List<PredicateParseResult>> EMPTY_PARSE_RESULT =
      Collections.singletonList(Collections.singletonList(PredicateParseResult.emptyPredicateParseResult()));

  private InputManager _inputManager;
  private boolean _useOverwrittenIndices;
  private IndexConfig _indexOverwritten;
  private InvertedSortedIndexJointRuleParams _params;
  private int _numColumnsIndexApplicable;

  /**
   * The plan priority for AND-connected sub-predicates in {@link FilterOperatorUtils}
   */
  public enum IteratorEvalPriorityEnum {
    INDEXED, // sorted, bitmap, range, text
    AND, OR, SCAN, EXPRESSION,
  }

  public enum RecommendationPriorityEnum {
    BITMAP, // already optimized
    CANDIDATE_SCAN, // candidates
    NON_CANDIDATE_SCAN, NESTED, // not applicable
  }

  /**
   *
   * @param queryContext parse query in QueryContext
   * @param queryWeight the weight (frequency) of query in the sample queries
   * @return A list of List<PredicateParseResult>. Each List<PredicateParseResult> is a list of exclusive candidates,
   * meaning that at most one candidate (PredicateParseResult) in the list can contribute to the global recommendation.
   */
  public List<List<PredicateParseResult>> parseQuery(QueryContext queryContext, double queryWeight) {
    FilterContext filter = queryContext.getFilter();
    if (filter == null || filter.isConstant()) {
      return EMPTY_PARSE_RESULT;
    }

    LOGGER.trace("Parsing Where Clause: {}", filter);
    return parseTopLevel(filter, queryWeight);
  }

  /**
   *  Reorder the list according to the plan priority for AND-connected sub-predicates in {@link FilterOperatorUtils}
   *  and iterator evaluation priority in {@link AndDocIdSet#iterator()}
   * @param childResults input child predicates
   * @return reordered AND-connected sub-predicates based on priorities
   */
  private List<PredicateParseResult> reorderAndPredicateList(List<PredicateParseResult> childResults) {
    // With any indexed columns, the order will be first intersecting (indexed columns + all scan based columns)
    // then make the resulting bitmap and the nested iterators a nested iterator
    if (childResults.stream().anyMatch(
        predicateParseResult -> predicateParseResult.getIteratorEvalPriority() == IteratorEvalPriorityEnum.INDEXED)) {
      Map<IteratorEvalPriorityEnum, List<PredicateParseResult>> groupedPredicates =
          childResults.stream().collect(Collectors.groupingBy(PredicateParseResult::getIteratorEvalPriority));
      List<PredicateParseResult> ret =
          groupedPredicates.getOrDefault(IteratorEvalPriorityEnum.INDEXED, new ArrayList<>());
      ret.addAll(groupedPredicates.getOrDefault(IteratorEvalPriorityEnum.SCAN, Collections.emptyList()));
      ret.addAll(groupedPredicates.getOrDefault(IteratorEvalPriorityEnum.AND, Collections.emptyList()));
      ret.addAll(groupedPredicates.getOrDefault(IteratorEvalPriorityEnum.OR, Collections.emptyList()));
      ret.addAll(groupedPredicates.getOrDefault(IteratorEvalPriorityEnum.EXPRESSION, Collections.emptyList()));
      return ret;
    } else {
      // Else the evaluation priority is simply AND, OR, SCAN, EXPRESSION
      childResults.sort(Comparator.comparing(PredicateParseResult::getIteratorEvalPriority));
      return childResults;
    }
  }

  /**
   * Simulates the execution of top level predicate list
   * top level: a = 3 and b = 4 and (...) is top level in "select ... where a = 3 and b = 4 and (c=5 or d=6)"
   * whereas (c=5 or d=6) is the second level
   *
   * Recommend inverted index for:
   * Case AND: The dimension(s) which minimizes the total nESI for AND clause, given the
   *           iterator reordering and intersection process in {@link AndDocIdSet}. The result is a list
   *           containing ONE List<PredicateParseResult>. The List<PredicateParseResult> is a set of exclusive
   *           candidates, meaning that any one but only one candidate in the list can
   *           contribute to the global recommendation.
   *
   * Case OR:  Recursively run parseTopLevel on each of the child predicates. The result is a list
   *           containing MULTIPLE List<PredicateParseResult>, which are the return value of parsing each of the child
   *           clauses of or. Each List<PredicateParseResult> will contribute to the global recommendation equally.
   *           This is due to the nature of evaluating or-connected predicates is essentially evaluating each of them
   *           individually
   *
   * @param filterContextTopLevel The tree representing the top level "where" clause
   * @return see {@link QueryInvertedSortedIndexRecommender#parseQuery(QueryContext, double)}
   */
  private List<List<PredicateParseResult>> parseTopLevel(FilterContext filterContextTopLevel, double queryWeight) {
    LOGGER.debug("parseTopLevel: Parsing top level predicate list: {}", filterContextTopLevel.toString());
    FilterContext.Type type = filterContextTopLevel.getType();
    // case: AND connected top level predicates.
    if (type == FilterContext.Type.AND) {
      /**** Run parsePredicateList on its children, and yielding PredicateParseResult p1, p2, p3 ..., pn ****/
      List<PredicateParseResult> childResults = new ArrayList<>();
      for (int i = 0; i < filterContextTopLevel.getChildren().size(); i++) {
        PredicateParseResult childResult =
            parsePredicateList(filterContextTopLevel.getChildren().get(i), NESTED_SECOND_LEVEL);
        if (childResult != null) {
          childResults.add(childResult);
        }
      }
      if (childResults.isEmpty()) {
        return EMPTY_PARSE_RESULT;
      }

      /**** Reorder the list and calculate the nESI WITHOUT any recommendation ****/
      // Also initiate a cache to fast calculate the all possible reordering of this AND predicate
      childResults = reorderAndPredicateList(childResults);
      LOGGER.debug("parseTopLevel: AND: Reordered child results: {}", childResults);
      List<PredicateParseResult> ret = new ArrayList<>();
      double[] cache = new double[childResults.size()];
      double percentSelected = 1;
      double totalNESI = 0;

      // the calculation process is nESI(p1) + portion(p1) * ( nESI(p2) + portion(p2) * ( nESI(p3) + portion(p3) * (
      // ... )))
      for (int i = 0; i < childResults.size(); i++) {
        cache[i] = childResults.get(i).getnESI() * percentSelected;
        totalNESI += cache[i];
        percentSelected *= childResults.get(i).getPercentSelected();
      }
      LOGGER.debug("parseTopLevel: AND: Cache: {} perQuerytTotalNESI:{} percentSelected:{}", cache, totalNESI,
          percentSelected);

      /**** chose the optimal PredicateParseResult(s) and pack them as exclusive candidates ****/
      List<Pair<Double, PredicateParseResult>> totalNESIWithIdxSorted = new LinkedList<>();

      // Pick the candidate recommended by a given child predicate, reorder the iterator evaluate sequence accordingly
      // (the picked child predicate as a whole will be evaluated to a bitmap, thus prioritized)
      // select the optimal indices (PredicateParseResult) that minimize the overall nESI after reordering
      for (int i = 0; i < childResults.size(); i++) {
        if (childResults.get(i).hasCandidateDim()) {
          double tmpNESIWithIdx = 0;
          int j = 0;
          for (; j < i; j++) {
            tmpNESIWithIdx += childResults.get(i)._percentSelected * cache[j];
          }
          j = j + 1;
          for (; j < childResults.size(); j++) {
            tmpNESIWithIdx += cache[j];
          }
          tmpNESIWithIdx += childResults.get(i)._nESIWithIdx;
          totalNESIWithIdxSorted.add(Pair.of(tmpNESIWithIdx,
              childResults.get(i))); // pair of totalNESIWithIdx and corresponding recommended indices
        }
      }

      // Pick the top N candidates whose nESI saved are > ratio * top_nESI_saved. Forming a list of candidates.
      // However only one of these candidates will be counted towards the global recommendation, this is called
      // exclusive candidates
      // This is to ensure the the per-query near-optimal recommendations not be overshadowed by the optimal
      // recommendations
      // Which will make the global recommendation optimal
      if (totalNESIWithIdxSorted.isEmpty()) { // If no applicable PredicateParseResult
        ret.add(PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.NESTED).setnESI(totalNESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(totalNESI).setQueryWeight(queryWeight).build());
      } else {
        totalNESIWithIdxSorted.sort(Comparator.comparing(Pair::getLeft));
        LOGGER.debug("parseTopLevel: AND: totalNESIWithIdxSorted {}", totalNESIWithIdxSorted);
        double threshold =
            _params._thresholdRatioMinAndPredicateTopCandidates * (totalNESI - totalNESIWithIdxSorted.get(0).getLeft());
        LOGGER.debug("threshold {}", threshold);
        for (Pair<Double, PredicateParseResult> pair : totalNESIWithIdxSorted) {
          if ((totalNESI - pair.getLeft()) >= threshold) { // TOP candidates
            ret.add(PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
                .setCandidateDims(pair.getRight().getCandidateDims())
                .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
                .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(totalNESI)
                .setPercentSelected(percentSelected).setnESIWithIdx(pair.getLeft()).setQueryWeight(queryWeight)
                .build());
          }
        }

        // If picking more candidates all together is beneficial, pick two or more at the same time. They will be
        // unioned together as one
        // candidate and added to the exclusive recommendations List<PredicateParseResult> . This is to optimize the
        // recommendation
        // when we have a large number of dimensions to apply index on.
        Pair<Double, PredicateParseResult> previousPair = totalNESIWithIdxSorted.remove(0);
        childResults.remove(previousPair.getRight());
        double previousTotalNESIWithIdx = previousPair.getLeft();
        double percentForCandidates = previousPair.getRight().getPercentSelected();
        double nESIWithIdx = previousPair.getRight().getnESIWithIdx();
        FixedLenBitset candidateDims = mutableEmptySet().union(previousPair.getRight().getCandidateDims());

        while (!totalNESIWithIdxSorted.isEmpty()) {
          previousPair = totalNESIWithIdxSorted.remove(0);
          childResults.remove(previousPair.getRight());
          candidateDims.union(previousPair.getRight().getCandidateDims());

          nESIWithIdx += previousPair.getRight().getnESIWithIdx();
          percentForCandidates *= previousPair.getRight().getPercentSelected();
          LOGGER.debug("childResults {}", childResults);
          LOGGER.debug("nESIWithIdx {}", nESIWithIdx);

          double tmpTotalNESIWithIdx = nESIWithIdx;
          double tmpPercentSelected = percentForCandidates;
          for (PredicateParseResult childResult : childResults) {
            tmpTotalNESIWithIdx += childResult.getnESI() * tmpPercentSelected;
            tmpPercentSelected *= childResult.getPercentSelected();
          }
          LOGGER.debug("tmpTotalNESIWithIdx {}", tmpTotalNESIWithIdx);
          if (previousTotalNESIWithIdx - tmpTotalNESIWithIdx > _params._thresholdMinAndPredicateIncrementalVote) {
            ret.add(
                PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
                    .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
                    .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(totalNESI)
                    .setPercentSelected(tmpPercentSelected).setnESIWithIdx(tmpTotalNESIWithIdx)
                    .setQueryWeight(queryWeight).build());
            previousTotalNESIWithIdx = tmpTotalNESIWithIdx;
          } else {
            break;
          }
        }
      }

      // Always add an empty recommendation (nESI with no recommended index)
      // so that the nESI can be correctly calculate when doing recommendation globally
      ret.add(PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.NESTED).setnESI(totalNESI)
          .setPercentSelected(percentSelected).setnESIWithIdx(totalNESI).setQueryWeight(queryWeight).build());
      return Collections.singletonList(ret);
    } else if (type == FilterContext.Type.OR) {
      // case: OR connected top level predicates, recursively run parseTopLevel on each on its children and
      // simply return all the results. Each result will contribute to the global recommendation equally
      List<List<PredicateParseResult>> childResults = new ArrayList<>();
      for (FilterContext child : filterContextTopLevel.getChildren()) {
        List<List<PredicateParseResult>> childResult = parseTopLevel(child, queryWeight);
        if (childResult != null) {
          childResults.addAll(childResult);
        }
      }
      LOGGER.debug("parseTopLevel: OR: Child results: {}", childResults);
      if (childResults.isEmpty()) {
        return EMPTY_PARSE_RESULT;
      } else {
        return childResults;
      }
    } else if (type == FilterContext.Type.NOT) {
      assert filterContextTopLevel.getChildren().size() == 1;
      return parseTopLevel(filterContextTopLevel.getChildren().get(0), queryWeight);
    } else {
      // case: Return result directly.
      PredicateParseResult predicateParseResult = parseLeafPredicate(filterContextTopLevel, NESTED_TOP_LEVEL);
      ArrayList<PredicateParseResult> ret = new ArrayList<>();
      if (predicateParseResult == null) {
        return EMPTY_PARSE_RESULT;
      } else {
        ret.add(PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(predicateParseResult._iteratorEvalPriorityEnum)
            .setRecommendationPriorityEnum(predicateParseResult._recommendationPriorityEnum)
            .setnESI(predicateParseResult._nESI).setPercentSelected(predicateParseResult._percentSelected)
            .setnESIWithIdx(predicateParseResult._nESI).setQueryWeight(queryWeight).build());
        ret.add(predicateParseResult.multiplyWeight(queryWeight));
        LOGGER.debug("parseTopLevel: LEAF: Child results: {}", ret);
        return Collections.singletonList(ret);
      }
    }
  }

  /**
   * Recursively simulates the execution of nested predicates, whose nested level > top level
   * e.g. (c=5 or d=6) in select ... where a = 3 and b = 4 and (c=5 or d=6)
   * Recommend inverted index for:
   * Case AND: The dimension which selects the lowest percentage of rows.
   * Case OR:  All the recommended dimensions from evaluating all its child predicates.
   * Case NOT: Same as the underlying predicate
   * Case Leaf: See {@link QueryInvertedSortedIndexRecommender#parseLeafPredicate(FilterContext, int)}
   * @param predicateList Single or nested predicates.
   * @param depth         The depth of current AST tree. >= Second level in this function. Top level is handled in
   * {@link QueryInvertedSortedIndexRecommender#parseTopLevel(FilterContext, double)} ()}.
   * @return A {@link PredicateParseResult} holding the metrics of simulated execution.
   */
  private PredicateParseResult parsePredicateList(FilterContext predicateList, int depth) {
    LOGGER.debug("parsePredicateList: Parsing predicate list: {}", predicateList.toString());
    FilterContext.Type type = predicateList.getType();
    // case:AND connected predicates
    if (type == FilterContext.Type.AND) {
      LOGGER.trace("parsePredicateList: Parsing AND list {}", predicateList.toString());
      FixedLenBitset candidateDims = mutableEmptySet();
      double nESI = NESI_ZERO;
      double percentSelected = PERCENT_SELECT_ALL;
      List<PredicateParseResult> childResults = new ArrayList<>();
      boolean isPureBitmap = false;

      // Recursively eval child predicates
      for (int i = 0; i < predicateList.getChildren().size(); i++) {
        PredicateParseResult childResult = parsePredicateList(predicateList.getChildren().get(i), depth + 1);
        if (childResult != null) {
          childResults.add(childResult);
        }
      }

      // Sort child candidates based on the priority
      childResults = reorderAndPredicateList(childResults);
      LOGGER.debug("parsePredicateList: AND: Reordered child results: {}", childResults);
      if (childResults.get(childResults.size() - 1).getIteratorEvalPriority()
          .compareTo(IteratorEvalPriorityEnum.INDEXED) <= 0) {
        isPureBitmap = true;
      }

      // Calculate the estimated columns scanned with no indices applied
      for (PredicateParseResult childResult : childResults) {
        nESI += childResult.getnESI() * percentSelected;
        percentSelected *= childResult.getPercentSelected();
      }

      // Recommend nothing if all child predicates already have indices
      if (isPureBitmap) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(nESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(nESI).build();
      }

      Map<RecommendationPriorityEnum, List<PredicateParseResult>> groupedPredicates = childResults.stream()
          .collect(Collectors.groupingBy(PredicateParseResult::getRecommendationPriorityEnum, Collectors.toList()));

      // Recommend nothing if any nested predicate list cannot be converted to a bitmap by applying sorted/inverted
      // indices
      if (groupedPredicates.containsKey(RecommendationPriorityEnum.NESTED) || (
          !groupedPredicates.containsKey(RecommendationPriorityEnum.BITMAP) && !groupedPredicates
              .containsKey(RecommendationPriorityEnum.CANDIDATE_SCAN))) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.NESTED).setnESI(nESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(nESI).build();
      }

      // Recommend one predicate having min percent docs selected to apply indices on,
      // from the scanning based predicates
      Optional<PredicateParseResult> newCandidateOptional =
          groupedPredicates.getOrDefault(RecommendationPriorityEnum.CANDIDATE_SCAN, Collections.emptyList()).stream()
              .filter(
                  PredicateParseResult::hasCandidateDim) // The predicate should be index applicable to be recommended
              .min(Comparator.comparing(PredicateParseResult::getPercentSelected));

      double bitmapPercentSelected = PERCENT_SELECT_ALL;
      double bitmapNESIWithIdx = NESI_ZERO;
      for (PredicateParseResult predicateParseResult : groupedPredicates
          .getOrDefault(RecommendationPriorityEnum.BITMAP, Collections.emptyList())) {
        bitmapPercentSelected *= predicateParseResult.getPercentSelected();
        bitmapNESIWithIdx += predicateParseResult.getnESIWithIdx();
        candidateDims.union(predicateParseResult.getCandidateDims());
      }

      if (depth > NESTED_SECOND_LEVEL) {
        if (groupedPredicates.containsKey(RecommendationPriorityEnum.BITMAP)) {
          newCandidateOptional = Optional.empty();
        }
      } else {
        if (newCandidateOptional.isPresent()
            && newCandidateOptional.get().getPercentSelected() > bitmapPercentSelected) {
          newCandidateOptional = Optional.empty();
        }
      }
      newCandidateOptional
          .ifPresent(predicateParseResult -> candidateDims.union(predicateParseResult.getCandidateDims()));

      // Calculate the estimated columns scanned with recommended indices applied
      double percentSelectedWithIdx =
          newCandidateOptional.map(PredicateParseResult::getPercentSelected).orElse(PERCENT_SELECT_ALL);
      double nESIWithIdx = newCandidateOptional.map(PredicateParseResult::getnESIWithIdx).orElse(NESI_ZERO);

      percentSelectedWithIdx *= bitmapPercentSelected;
      nESIWithIdx += bitmapNESIWithIdx;

      for (PredicateParseResult predicateParseResult : groupedPredicates
          .getOrDefault(RecommendationPriorityEnum.CANDIDATE_SCAN, Collections.emptyList())) {
        if (predicateParseResult != newCandidateOptional.orElse(null)) {
          nESIWithIdx += predicateParseResult.getnESI() * percentSelectedWithIdx;
          percentSelectedWithIdx *= predicateParseResult.getPercentSelected();
        }
      }

      for (PredicateParseResult predicateParseResult : groupedPredicates
          .getOrDefault(RecommendationPriorityEnum.NON_CANDIDATE_SCAN, Collections.emptyList())) {
        nESIWithIdx += predicateParseResult.getnESI() * percentSelectedWithIdx;
        percentSelectedWithIdx *= predicateParseResult.getPercentSelected();
      }

      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.AND)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(nESI)
          .setPercentSelected(percentSelected).setnESIWithIdx(nESIWithIdx).build();
    } else if (type == FilterContext.Type.OR) {
      // case:OR connected predicates
      LOGGER.trace("parsePredicateList: OR Parsing  list: {}", predicateList.toString());
      List<PredicateParseResult> childResults = new ArrayList<>();
      FixedLenBitset candidateDims = mutableEmptySet();
      double nESI = NESI_ZERO;
      double nESIWithIdx = NESI_ZERO;
      double percentSelected = PERCENT_SELECT_ZERO;
      boolean isPureBitmap = true;
      boolean isBitmapConvertable = true;

      // Recursively eval child predicates
      for (int i = 0; i < predicateList.getChildren().size(); i++) {
        PredicateParseResult childResult = parsePredicateList(predicateList.getChildren().get(i), depth + 1);
        if (childResult != null) {
          childResults.add(childResult);
        }
      }
      LOGGER.debug("parsePredicateList: OR: childResults {}", childResults);
      // Recommend all child candidates if by doing this the or predicate can be reduced to a bitmap iterator
      for (PredicateParseResult childResult : childResults) {
        nESI += childResult.getnESI();
        percentSelected += childResult.getPercentSelected();
        nESIWithIdx += childResult.getnESIWithIdx();
        candidateDims.union(childResult.getCandidateDims());
        if (!childResult.isBitmapConvertable()) { //Contains column that can not be converted to a Bitmap iterator
          isBitmapConvertable = false;
        }
        if (childResult.getIteratorEvalPriority().compareTo(IteratorEvalPriorityEnum.INDEXED) > 0) {
          isPureBitmap = false;
        }
      }
      LOGGER.trace("parsePredicateList: OR: parsePredicateList(): isPureBitmap {} hasCandidateDim {}", isPureBitmap,
          isBitmapConvertable);
      percentSelected = Math.min(percentSelected, PERCENT_SELECT_ALL);

      if (isPureBitmap) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(nESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(nESIWithIdx).build();
      } else if (isBitmapConvertable) { // The current and predicate cannot be converted to a bitmap by applying indices
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.OR)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(nESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(nESIWithIdx).build();
      } else {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.OR)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.NESTED).setnESI(nESI)
            .setPercentSelected(percentSelected).setnESIWithIdx(nESI).build();
      }
    } else if (type == FilterContext.Type.NOT) {
      assert predicateList.getChildren().size() == 1;
      return parsePredicateList(predicateList.getChildren().get(0), depth);
    } else {
      // case:Leaf predicate
      PredicateParseResult predicateParseResult = parseLeafPredicate(predicateList, depth);
      LOGGER.debug("parsePredicateList: Parsed a leaf predicate: {}", predicateParseResult);
      return predicateParseResult;
    }
  }

  /**
   * Simulate the execution of leaf predicates
   *
   * @param leafPredicate the leaf predicate context where the score are generated from selectivity
   * @return A {@link PredicateParseResult} holding the metrics of simulated execution.
   */
  private PredicateParseResult parseLeafPredicate(FilterContext leafPredicate, int depth) {
    LOGGER.trace("parseLeafPredicate: Parsing predicate: {}", leafPredicate.toString());
    Predicate predicate = leafPredicate.getPredicate();
    Predicate.Type type = predicate.getType();
    ExpressionContext lhs = predicate.getLhs();
    String colName = predicate.getLhs().toString();
    double numValuesPerEntry = _inputManager.getNumValuesPerEntry(colName);

    // e.g. max(a, b) > 10 : requires transformation on multiple dimensions, scan only.
    if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
      double nESI = flattenFunction(lhs);
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.NON_CANDIDATE_SCAN) // won't recommend index
          .setnESI(nESI).setPercentSelected(_params._percentSelectForFunction).setnESIWithIdx(nESI).build();
    } else if (type == Predicate.Type.RANGE) {
      // e.g. a > 10 / b between 1 and 10
      LOGGER.trace("Entering RANGE clause: {}", leafPredicate);
      if (_useOverwrittenIndices && (_indexOverwritten.hasSortedIndex(colName) || _indexOverwritten
          .hasRangeIndex(colName))) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(NESI_ZERO)
            .setPercentSelected(_params._percentSelectForRange).setnESIWithIdx(NESI_ZERO).build();
      } else {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.NON_CANDIDATE_SCAN).setnESI(numValuesPerEntry)
            .setPercentSelected(_params._percentSelectForRange).setnESIWithIdx(numValuesPerEntry).build();
      }
    } else if (_inputManager.getColNamesNoDictionary().contains(colName)) {
      // For overwritten no dictionary columns we do not recommend indices
      // In the future no dictionary columns might have range indices
      // so do not swap this with type == Predicate.Type.RANGE
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.NON_CANDIDATE_SCAN).setnESI(numValuesPerEntry)
          .setPercentSelected(_params._percentSelectForRange).setnESIWithIdx(numValuesPerEntry).build();
    } else if (type == Predicate.Type.IN || type == Predicate.Type.NOT_IN) {
      // e.g. a IN (1,2,3)
      LOGGER.trace("Entering IN clause: {}", leafPredicate);

      double cardinality = _inputManager.getCardinality(colName);
      LOGGER.trace("Cardinality: {} {}", colName, cardinality);

      FixedLenBitset candidateDims = mutableEmptySet();
      candidateDims.add(_inputManager.colNameToInt(colName));
      int numValuesSelected;

      boolean isFirst = false;
      List<String> values = (type == Predicate.Type.IN) ? ((InPredicate) predicate).getValues()
          : ((NotInPredicate) predicate).getValues();
      if (values.size() == 1) {
        numValuesSelected = 1;
      } else if (values.get(RecommenderConstants.FIRST).equals(RecommenderConstants.IN_PREDICATE_ESTIMATE_LEN_FLAG)) {
        numValuesSelected = Integer.parseInt(values.get(RecommenderConstants.SECOND));
      } else if (values.get(RecommenderConstants.SECOND).equals(RecommenderConstants.IN_PREDICATE_ESTIMATE_LEN_FLAG)) {
        numValuesSelected = Integer.parseInt(values.get(RecommenderConstants.FIRST));
      } else {
        numValuesSelected = values.size();
      }
      Boolean isExclusive = leafPredicate.getPredicate().getType().isExclusive();
      LOGGER.debug("Length of in clause: {}", numValuesSelected);

      if (_useOverwrittenIndices && (_indexOverwritten.hasInvertedIndex(colName) || _indexOverwritten
          .hasSortedIndex(colName))) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(NESI_ZERO)
            .setPercentSelected(percentSelected(isExclusive, cardinality, numValuesSelected, numValuesPerEntry))
            .setnESIWithIdx(NESI_ZERO).build();
      } else {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.CANDIDATE_SCAN).setnESI(numValuesPerEntry)
            .setPercentSelected(percentSelected(isExclusive, cardinality, numValuesSelected, numValuesPerEntry))
            .setnESIWithIdx(NESI_ZERO).build();
      }
    } else if (type == Predicate.Type.EQ || type == Predicate.Type.NOT_EQ) {
      // e.g. a <> 1
      LOGGER.trace("Entering COMP clause: {}", leafPredicate);

      double cardinality = _inputManager.getCardinality(colName);
      LOGGER.trace("Cardinality: {} {}", colName, cardinality);

      FixedLenBitset candidateDims = mutableEmptySet();
      candidateDims.add(_inputManager.colNameToInt(colName));
      Boolean isExclusive = leafPredicate.getPredicate().getType().isExclusive();

      if (_useOverwrittenIndices && (_indexOverwritten.hasInvertedIndex(colName) || _indexOverwritten
          .hasSortedIndex(colName))) {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
            .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(NESI_ZERO)
            .setPercentSelected(percentSelected(isExclusive, cardinality, 1, numValuesPerEntry))
            .setnESIWithIdx(NESI_ZERO).build();
      } else {
        return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult().setCandidateDims(candidateDims)
            .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
            .setRecommendationPriorityEnum(RecommendationPriorityEnum.CANDIDATE_SCAN).setnESI(numValuesPerEntry)
            .setPercentSelected(percentSelected(isExclusive, cardinality, 1, numValuesPerEntry))
            .setnESIWithIdx(NESI_ZERO).build();
      }
    } else if (type == Predicate.Type.TEXT_MATCH) {
      // e.g. TEXT_MATCH(a, "...")
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(NESI_ZERO)
          .setPercentSelected(_params._percentSelectForTextMatch).setnESIWithIdx(NESI_ZERO).build();
    } else if (type == Predicate.Type.REGEXP_LIKE) {
      //  e.g. REGEXP_LIKE(a, "...")
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.NON_CANDIDATE_SCAN).setnESI(numValuesPerEntry)
          .setPercentSelected(_params._percentSelectForRegex).setnESIWithIdx(NESI_ZERO).build();
    } else if (type == Predicate.Type.IS_NOT_NULL || type == Predicate.Type.IS_NULL) {
      // a IS NULL
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.INDEXED)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.BITMAP).setnESI(NESI_ZERO)
          .setPercentSelected(_params._percentSelectForIsnull).setnESIWithIdx(NESI_ZERO).build();
    } else {
      // DEFAULT for unrecognized operators
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(FixedLenBitset.IMMUTABLE_EMPTY_SET)
          .setIteratorEvalPriorityEnum(IteratorEvalPriorityEnum.SCAN)
          .setRecommendationPriorityEnum(RecommendationPriorityEnum.NON_CANDIDATE_SCAN).setnESI(numValuesPerEntry)
          .setPercentSelected(_params._percentSelectForRange).setnESIWithIdx(numValuesPerEntry).build();
    }
  }

  public static double percentSelected(Boolean invertSelection, double cardinality, int numSelectedValues,
      double numEntriesPerDoc) {
    double n = cardinality;
    double r = numEntriesPerDoc;
    int k = numSelectedValues;
    double pInverted;
    if (k + r <= n) {
      if (r != 1 && k != 1) {
        // The possibility of selecting x values out of n different values,
        // s.t. *NONE* of x values falls into a subset of k given values
        // pInverted = (n-k)Cr / nCr = (n-r-k+1) * (n-r-k+2) * ... * (n - r)/{(n-k+1)*(n-k+2)*...*n}
        // Then the possibility of
        pInverted = 1;
        for (int i = 0; i < k; i++) {
          pInverted *= (n - r - i) / (n - i);
        }
      } else {
        pInverted = 1 - Math.max(k, r) / n;
      }
    } else {
      pInverted = 0;
    }

    if (!invertSelection) { // not invertSelection
      return 1 - pInverted;
    } else { // invertSelection®®
      return pInverted;
    }
  }

  private double flattenFunction(ExpressionContext expression) {
    double ret = 0;
    HashSet<String> dimNames = new HashSet<>();
    expression.getColumns(dimNames);
    for (String dimName : dimNames) {
      if (_inputManager.isDim(dimName)) {
        ret += _inputManager.getNumValuesPerEntry(dimName);
      }
    }
    return ret;
  }

  public static final class QueryInvertedSortedIndexRecommenderBuilder {
    private InputManager _inputManager;
    private boolean _useOverwrittenIndices = true;
    private InvertedSortedIndexJointRuleParams _invertedSortedIndexJointRuleParams;

    private QueryInvertedSortedIndexRecommenderBuilder() {
    }

    public static QueryInvertedSortedIndexRecommenderBuilder aQueryInvertedSortedIndexRecommender() {
      return new QueryInvertedSortedIndexRecommenderBuilder();
    }

    public QueryInvertedSortedIndexRecommenderBuilder setInputManager(InputManager inputManager) {
      _inputManager = inputManager;
      return this;
    }

    public QueryInvertedSortedIndexRecommenderBuilder setUseOverwrittenIndices(boolean useOverwrittenIndices) {
      _useOverwrittenIndices = useOverwrittenIndices;
      return this;
    }

    public QueryInvertedSortedIndexRecommenderBuilder setInvertedSortedIndexJointRuleParams(
        InvertedSortedIndexJointRuleParams invertedSortedIndexJointRuleParams) {
      _invertedSortedIndexJointRuleParams = invertedSortedIndexJointRuleParams;
      return this;
    }

    public QueryInvertedSortedIndexRecommender build() {
      QueryInvertedSortedIndexRecommender queryInvertedSortedIndexRecommender =
          new QueryInvertedSortedIndexRecommender();
      queryInvertedSortedIndexRecommender._params = _invertedSortedIndexJointRuleParams;
      queryInvertedSortedIndexRecommender._useOverwrittenIndices = _useOverwrittenIndices;
      queryInvertedSortedIndexRecommender._inputManager = _inputManager;
      queryInvertedSortedIndexRecommender._numColumnsIndexApplicable =
          _inputManager.getNumColumnsInvertedSortedApplicable();
      queryInvertedSortedIndexRecommender._indexOverwritten = _inputManager.getOverWrittenConfigs().getIndexConfig();

      return queryInvertedSortedIndexRecommender;
    }
  }

  private FixedLenBitset mutableEmptySet() {
    return new FixedLenBitset(_numColumnsIndexApplicable);
  }
}
