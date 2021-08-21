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
package org.apache.pinot.controller.recommender.rules.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.InvertedSortedIndexJointRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.controller.recommender.rules.utils.PredicateParseResult;
import org.apache.pinot.controller.recommender.rules.utils.QueryInvertedSortedIndexRecommender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recommend one sorted index and inverted indices
 * See details in
 * https://cwiki.apache.org/confluence/display/PINOT/Automated+Inverted+Index+Recommendation+for+Pinot
 * #AutomatedInvertedIndexRecommendationforPinot-3.Parser-basedEstimation:
 */
public class InvertedSortedIndexJointRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedSortedIndexJointRule.class);
  InvertedSortedIndexJointRuleParams _params;
  Double _totalNESI;

  public InvertedSortedIndexJointRule(InputManager input, ConfigManager config) {
    super(input, config);
    _params = input.getInvertedSortedIndexJointRuleParams();
  }

  public void run() {

    // count total nESI for all queries without any indices
    QueryInvertedSortedIndexRecommender totalNESICounter =
        QueryInvertedSortedIndexRecommender.QueryInvertedSortedIndexRecommenderBuilder
            .aQueryInvertedSortedIndexRecommender().setInputManager(_input)
            .setInvertedSortedIndexJointRuleParams(_params)
            .setUseOverwrittenIndices(false) // nESI when not using any overwritten indices
            .build();

    List<Double> perQueryNESI = _input.getParsedQueries().stream().flatMap(
        query -> totalNESICounter.parseQuery(_input.getQueryContext(query), _input.getQueryWeight(query))
            .stream()) // List<PredicateParseResult>
        .map(exclusiveRecommendations -> exclusiveRecommendations.get(0).getnESI()).collect(Collectors.toList());
    _totalNESI = perQueryNESI.stream().reduce(Double::sum).orElse(0d);

    LOGGER.debug("totalNESI without any indices {}", _totalNESI);
    LOGGER.debug("perQueryNESI {}", perQueryNESI);

    // recommend indices for each query
    QueryInvertedSortedIndexRecommender perQueryRecommender =
        QueryInvertedSortedIndexRecommender.QueryInvertedSortedIndexRecommenderBuilder
            .aQueryInvertedSortedIndexRecommender().setInputManager(_input)
            .setInvertedSortedIndexJointRuleParams(_params).setUseOverwrittenIndices(true).build();

    List<List<List<PredicateParseResult>>> parsedQueries = _input.getParsedQueries().stream()
        .map(query -> perQueryRecommender.parseQuery(_input.getQueryContext(query), _input.getQueryWeight(query)))
        .collect(Collectors.toList());

    // based on per-query result, recommend indices for entire query set
    // this process is essentially brute-forcing all the combinations of dimensions to apply indices on
    // start from using only one dimensions, increment the number gradually, till a point adding more indices will
    // not be rewarding
    List<List<PredicateParseResult>> flattenedResults =
        parsedQueries.stream().flatMap(Collection::stream).collect(Collectors.toList());
    PredicateParseResult optimalCombination = findOptimalCombination(flattenedResults);
    LOGGER.debug("flattenedResults: {}", flattenedResults);
    LOGGER.debug("optimalCombination: {}", optimalCombination);

    // under the optimal set of indices, calculate nESI saved for each qeury
    List<PredicateParseResult> perQuerySelectedCandidate =
        flattenedResults.stream()// stream of List<PredicateParseResult> (exclusive recommendations)
            .map(exclusiveRecommendations -> exclusiveRecommendations.stream() // stream of PredicateParseResult
                .filter(predicateParseResult // PredicateParseResult whose index set is in optimal index set
                    -> optimalCombination.getCandidateDims().contains(predicateParseResult.getCandidateDims()))
                .min(Comparator.comparing(PredicateParseResult::getnESIWithIdx))
                .get()) // The selected result in findOptimalCombination process
            .collect(Collectors.toList());

    LOGGER.debug("perQuerySelectedCandidate: {}", perQuerySelectedCandidate);

    int numColumnsIndexApplicable = _input.getNumColumnsInvertedSortedApplicable();
    double[] weights = new double[numColumnsIndexApplicable];
    for (int i = 0; i < perQueryNESI.size(); i++) {
      double savedNESI = perQueryNESI.get(i) - perQuerySelectedCandidate.get(i).getnESIWithIdx();
      // split per query nESI saved equally among the columns used in this query
      double share = savedNESI / perQuerySelectedCandidate.get(i).getCandidateDims().getCardinality();

      LOGGER.trace("getOffsets: {}", perQuerySelectedCandidate.get(i).getCandidateDims());
      for (Integer j : perQuerySelectedCandidate.get(i).getCandidateDims().getOffsets()) {
        weights[j] += share;
      }
    }

    List<Pair<String, Double>> colNameWeightPairRank = new ArrayList<>(numColumnsIndexApplicable);

    for (int i = 0; i < numColumnsIndexApplicable; i++) {
      if (weights[i] > 0) {
        colNameWeightPairRank.add(Pair.of(_input.intToColName(i), weights[i]));
      }
    }

    if (!colNameWeightPairRank.isEmpty()) {
      if (_input.getOverWrittenConfigs().getIndexConfig().isSortedColumnOverwritten()) {
        // if an overwritten sorted index presents
        colNameWeightPairRank.forEach(pair -> {
          _output.getIndexConfig().getInvertedIndexColumns()
              .add(pair.getLeft()); // Add recommendations to inverted index set
        });
      } else {
        // sort dimensions based on the overall nESI saved by each dimension
        // for top dimensions whose nESI saved exceeds
        // nESI_saved_by_top_dimension * THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES
        // pick the top dimension with highest Cardinality as sorted index
        // and others will be recommended as inverted index
        // Our study shows that: [Regardless of the situation, a inverted index can be
        // replaced with a sorted index (given no prior sorted index), and the performance
        // boost will be significant, the binary search cost of sorted index is minor]
        colNameWeightPairRank.sort((a, b) -> b.getRight().compareTo(a.getRight()));
        LOGGER.debug("colWeightsRank: {}", colNameWeightPairRank);
        Double weightThresholdForTopCandidates =
            colNameWeightPairRank.get(0).getRight() * _params._thresholdRatioMinNesiForTopCandidates;
        Optional<Pair<String, Double>> sortedColumn = colNameWeightPairRank.stream()
            .filter(pair -> pair.getRight() >= weightThresholdForTopCandidates) // nESI saved > threshold
            .filter(pair -> _input.isSingleValueColumn(pair.getLeft())) // sorted index on single valued column
            .max(Comparator.comparing(pair -> // pick dimension with highest Cardinality as sorted index
                _input.getCardinality(pair.getLeft())));
        if (sortedColumn.isPresent()) {
          _output.getIndexConfig().setSortedColumn(sortedColumn.get().getLeft());
          colNameWeightPairRank.stream().filter(pair -> pair != sortedColumn.get())
              .forEach(pair -> _output.getIndexConfig().getInvertedIndexColumns().add(pair.getLeft()));
        } else {
          colNameWeightPairRank.forEach(pair -> _output.getIndexConfig().getInvertedIndexColumns().add(pair.getLeft()));
        }
      }
    }
  }

  /**
   * Gradually increment the number of indices to apply index on
   * @param parsedQuery a list of query parsed and scored using {@link QueryInvertedSortedIndexRecommender}
   * @return the optimal combination of indices and the nESI with such combination
   */
  public PredicateParseResult findOptimalCombination(List<List<PredicateParseResult>> parsedQuery) {
    int n = _input.getNumColumnsInvertedSortedApplicable();
    int iterationsWithoutGain = 0;
    PredicateParseResult optimalCombinationResult = evaluateCombination(n, 1, parsedQuery);
    LOGGER.debug("findOptimalCombination: currentOptimalCombinationResult: {}", optimalCombinationResult);
    for (int r = 2; r <= n; r++) {
      PredicateParseResult currentCombination = evaluateCombination(n, r, parsedQuery);
      LOGGER.debug("findOptimalCombination: currentCombination: {}", currentCombination);
      double ratio = (optimalCombinationResult.getnESIWithIdx() - currentCombination.getnESIWithIdx()) / _totalNESI;
      LOGGER.debug("ratio {}", ratio);
      if (ratio > _params._thresholdRatioMinGainDiffBetweenIteration) {
        optimalCombinationResult = currentCombination;
        iterationsWithoutGain = 0;
      } else {
        iterationsWithoutGain += 1;
        if (iterationsWithoutGain >= _params._maxNumIterationWithoutGain) {
          break;
        }
      }
      LOGGER.debug("findOptimalCombination: currentOptimalCombinationResult: {}", optimalCombinationResult);
    }
    return optimalCombinationResult;
  }

  /**
   * @param n total number of dimensions in this table
   * @param r number of dimensions to apply index on
   * @param parsedQuery a list of query parsed and scored using {@link QueryInvertedSortedIndexRecommender}
   * @return {@link PredicateParseResult}
   * {@link PredicateParseResult#getCandidateDims()} the optimal r-sized set of dimensions to apply index on
   * {@link PredicateParseResult#getnESI()} the nESI before applying any index
   * {@link PredicateParseResult#getnESIWithIdx()} the estimated nESI after applying optimal indices
   */
  public PredicateParseResult evaluateCombination(final int n, final int r,
      List<List<PredicateParseResult>> parsedQuery) {
    FixedLenBitset usedCols = new FixedLenBitset(n);
    parsedQuery.forEach(list -> list.stream()
        .filter(predicateParseResult -> (predicateParseResult.getCandidateDims().getCardinality() <= r))
        .forEach(predicateParseResult -> usedCols.union(predicateParseResult.getCandidateDims())));
    LOGGER.debug("totalUsed {}", usedCols.getCardinality());

    List<Integer> usedColIDs = usedCols.getOffsets();
    int nCapped = usedColIDs.size();
    int rCapped = Math.min(r, nCapped);

    int[] idToColID = new int[nCapped];
    for (int i = 0; i < nCapped; i++) {
      idToColID[i] = usedColIDs.get(i);
    }

    // Enumerate all possible combinations of r-sized set, which will be applied indices on
    List<int[]> combinationIntArrays = generateCombinations(nCapped, rCapped);

    // Calculate the total nESIWithIdx after applying each r-sized indices
    // and pick the top r-sized indices that minimize total nESIWithIdx
    Optional<Pair<Double, FixedLenBitset>> optimal = combinationIntArrays.parallelStream().map(combinationIntArray -> {
      FixedLenBitset fixedLenBitset = new FixedLenBitset(n);
      for (int j = 0; j < rCapped; j++) {
        fixedLenBitset.add(idToColID[combinationIntArray[j]]);
      }
      double nESIWithIdx = 0;
      for (List<PredicateParseResult> exclusiveRecommendations : parsedQuery) {
        double bestNESIWithIdx = exclusiveRecommendations.get(0).getnESI();
        for (PredicateParseResult predicateParseResult : exclusiveRecommendations) {
          if (fixedLenBitset.contains(predicateParseResult.getCandidateDims())) {
            // If the dimensions of a candidate is a subset of the r-sized set
            bestNESIWithIdx = Math.min(bestNESIWithIdx, predicateParseResult.getnESIWithIdx());
          }
        }
        nESIWithIdx += bestNESIWithIdx;
      }
      return Pair.of(nESIWithIdx, fixedLenBitset);
    }).min(Comparator.comparing(Pair::getLeft));
    if (optimal.isPresent()) {
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(optimal.get().getRight())
          .setIteratorEvalPriorityEnum(QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum.INDEXED)
          .setRecommendationPriorityEnum(QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum.BITMAP)
          .setnESI(_totalNESI).setPercentSelected(0).setnESIWithIdx(optimal.get().getLeft()).build();
    } else {
      return PredicateParseResult.PredicateParseResultBuilder.aPredicateParseResult()
          .setCandidateDims(new FixedLenBitset(n))
          .setIteratorEvalPriorityEnum(QueryInvertedSortedIndexRecommender.IteratorEvalPriorityEnum.INDEXED)
          .setRecommendationPriorityEnum(QueryInvertedSortedIndexRecommender.RecommendationPriorityEnum.BITMAP)
          .setnESI(_totalNESI).setPercentSelected(0).setnESIWithIdx(_totalNESI).build();
    }
  }

  /**
   * @param n the size of a set N={0, 2, 3, 4, ..., n-1}
   * @param r size of subset to select from N
   * @return enumeration of all r-sized subsets
   */
  public static List<int[]> generateCombinations(int n, int r) {
    List<int[]> combinations = new ArrayList<>();
    if (r == 0) {
      return combinations;
    }
    int[] combination = new int[r];

    // initialize with lowest lexicographic combination
    for (int i = 0; i < r; i++) {
      combination[i] = i;
    }

    while (combination[r - 1] < n) {
      combinations.add(combination.clone());

      // generate next combination in lexicographic order
      int t = r - 1;
      while (t != 0 && combination[t] == n - r + t) {
        t--;
      }
      combination[t]++;
      for (int i = t + 1; i < r; i++) {
        combination[i] = combination[i - 1] + 1;
      }
    }

    return combinations;
  }
}
