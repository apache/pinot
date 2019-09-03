/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.detection.components;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.EvaluationBean;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.spec.MapeAveragePercentageChangeModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.ModelEvaluationResult;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.joda.time.Instant;


/**
 *  Monitor the recent mean MAPE in last 7 days, and compare that with the mean MAPE for the last 30 days.
 *  If the percentage change dropped to a certain threshold for a metric urn, return a bad model status to trigger
 *  auto configuration.
 */
public class MapeAveragePercentageChangeModelEvaluator implements ModelEvaluator<MapeAveragePercentageChangeModelEvaluatorSpec> {
  private static final int MAPE_LOOK_BACK_DAYS_RECENT = 7;
  private static final int MAPE_LOOK_BACK_DAYS_BASELINE = 30;

  private InputDataFetcher dataFetcher;
  private double threshold;

  @Override
  public ModelEvaluationResult evaluateModel(Instant evaluationTimeStamp) {
    EvaluationSlice evaluationSlice =
        new EvaluationSlice().withStartTime(evaluationTimeStamp.toDateTime().minusDays(MAPE_LOOK_BACK_DAYS_BASELINE).getMillis())
            .withEndTime(evaluationTimeStamp.getMillis());
    // fetch evaluations
    Collection<EvaluationDTO> evaluations =
        this.dataFetcher.fetchData(new InputDataSpec().withEvaluationSlices(Collections.singleton(evaluationSlice)))
            .getEvaluations()
            .get(evaluationSlice);

    Collection<EvaluationDTO> recentEvaluations = getEvaluationsWithinDays(evaluations, evaluationTimeStamp,
        MAPE_LOOK_BACK_DAYS_RECENT);
    Collection<EvaluationDTO> baselineEvaluations = getEvaluationsWithinDays(evaluations, evaluationTimeStamp,
        MAPE_LOOK_BACK_DAYS_BASELINE);

    if (recentEvaluations.isEmpty() || recentEvaluations.containsAll(baselineEvaluations)) {
      // data is insufficient for performing evaluations
      return new ModelEvaluationResult(ModelStatus.UNKNOWN);
    }

    // calculate past 7 day mean MAPE for each metric urn and rules
    Map<String, Double> recentMeanMapeForMetricUrnsAndRules = getMeanMapeForEachMetricUrnAndRule(recentEvaluations);

    // calculate past 30 day mean MAPE for each metric urn and rules
    Map<String, Double> baselineMeanMapeForMetricUrnsAndRules = getMeanMapeForEachMetricUrnAndRule(baselineEvaluations);

    // evaluate for each metric urn
    Map<String, Boolean> evaluationResultForMetricUrnsAndRules = recentMeanMapeForMetricUrnsAndRules.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            // compare the MAPE percentage change to threshold
            recentMeanMape -> recentMeanMape.getValue() / baselineMeanMapeForMetricUrnsAndRules.get(recentMeanMape.getKey()) - 1 <= threshold));

    if (evaluationResultForMetricUrnsAndRules.values().stream().allMatch(result -> result)) {
      // if all metric urn's status is good, return overall good status
      return new ModelEvaluationResult(ModelStatus.GOOD);
    }
    return new ModelEvaluationResult(ModelStatus.BAD);
  }

  /**
   * Filter the evaluations to return only the past number days.
   * @param evaluations evaluations
   * @param evaluationTimeStamp the time stamp for evaluations
   * @param days look back number of days
   * @return the filtered collection of evaluationDTOs
   */
  private Collection<EvaluationDTO> getEvaluationsWithinDays(Collection<EvaluationDTO> evaluations,
      Instant evaluationTimeStamp, int days) {
    return evaluations.stream()
        .filter(eval -> Objects.nonNull(eval.getMape()))
        .filter(eval -> evaluationTimeStamp.toDateTime().minusDays(days).getMillis() < eval.getStartTime())
        .collect(Collectors.toSet());
  }

  /**
   * calculate the mean MAPE for each metric urn based on the available evaluations over the past number of days
   * @param evaluations the available evaluations
   * @return the mean MAPE keyed by metric urns
   */
  private Map<String, Double> getMeanMapeForEachMetricUrnAndRule(Collection<EvaluationDTO> evaluations) {
    return
        evaluations.stream().collect(
            Collectors.groupingBy(e -> String.format("%s:%s", e.getMetricUrn(), e.getDetectorName()), Collectors.averagingDouble(EvaluationBean::getMape)));
  }

  @Override
  public void init(MapeAveragePercentageChangeModelEvaluatorSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.threshold = spec.getThreshold();
  }
}
