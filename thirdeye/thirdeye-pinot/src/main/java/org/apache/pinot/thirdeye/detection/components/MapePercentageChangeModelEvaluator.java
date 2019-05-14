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
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.EvaluationBean;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.spec.MapePercentageChangeModelEvaluatorSpec;
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
public class MapePercentageChangeModelEvaluator implements ModelEvaluator<MapePercentageChangeModelEvaluatorSpec> {
  private InputDataFetcher dataFetcher;
  private double threshold;

  @Override
  public ModelEvaluationResult evaluateModel(Instant evaluationTimeStamp) {
    EvaluationSlice evaluationSlice =
        new EvaluationSlice().withStartTime(evaluationTimeStamp.toDateTime().minusDays(30).getMillis())
            .withEndTime(evaluationTimeStamp.getMillis());
    // fetch evaluations
    Collection<EvaluationDTO> evaluations =
        this.dataFetcher.fetchData(new InputDataSpec().withEvaluationSlices(Collections.singleton(evaluationSlice)))
            .getEvaluations()
            .get(evaluationSlice);
    // calculate past 7 day mean MAPE for each metric urn
    Map<String, Double> sevenDaysMeanMapeForMetricUrns =
        getMeanMapeForEachMetricUrn(evaluations, evaluationTimeStamp, 7);

    // calculate past 30 day mean MAPE for each metric urn
    Map<String, Double> thirtyDaysMeanMapeForMetricUrns =
        getMeanMapeForEachMetricUrn(evaluations, evaluationTimeStamp, 30);

    // evaluate for each metric urn
    Map<String, Boolean> evaluationResultForMetricUrns = sevenDaysMeanMapeForMetricUrns.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            // compare the MAPE percentage change to threshold
            e -> e.getValue() / thirtyDaysMeanMapeForMetricUrns.get(e.getKey()) - 1 <= threshold));

    if (evaluationResultForMetricUrns.values().stream().allMatch(result -> result)) {
      // if all metric urn's status is good, return overall good status
      return new ModelEvaluationResult(ModelStatus.GOOD);
    }
    return new ModelEvaluationResult(ModelStatus.BAD);
  }

  /**
   * calculate the mean MAPE for each metric urn based on the available evaluations over the past numbe of days
   * @param evaluations the available evaluations
   * @param evaluationTimeStamp the time stamp for this evaluation
   * @param days number of days
   * @return the mean MAPE keyed by metric urns
   */
  private Map<String, Double> getMeanMapeForEachMetricUrn(Collection<EvaluationDTO> evaluations,
      Instant evaluationTimeStamp, int days) {
    return evaluations.stream()
        .filter(eval -> evaluationTimeStamp.toDateTime().minusDays(days).getMillis() < eval.getStartTime())
        .collect(
            Collectors.groupingBy(EvaluationBean::getMetricUrn, Collectors.averagingDouble(EvaluationBean::getMape)));
  }

  @Override
  public void init(MapePercentageChangeModelEvaluatorSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.threshold = spec.getThreshold();
  }
}
