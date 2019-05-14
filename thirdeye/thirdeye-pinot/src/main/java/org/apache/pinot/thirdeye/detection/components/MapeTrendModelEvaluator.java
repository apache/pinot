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
import org.apache.pinot.thirdeye.detection.spec.MapeTrendModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.ModelEvaluationResult;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.joda.time.Instant;


/**
 *  Monitor the recent mean MAPE in last 7 days, and compare that with the mean MAPE for the last 30 days.
 *  If it's dropped to a certain threshold, return bad model status
 */
public class MapeTrendModelEvaluator implements ModelEvaluator<MapeTrendModelEvaluatorSpec> {
  private InputDataFetcher dataFetcher;
  private double threshold;

  @Override
  public ModelEvaluationResult evaluateModel(Instant evaluationTimeStamp) {
    EvaluationSlice evaluationSlice =
        new EvaluationSlice().withStartTime(evaluationTimeStamp.toDateTime().minusDays(30).getMillis())
            .withEndTime(evaluationTimeStamp.getMillis());
    Collection<EvaluationDTO> evaluations =
        this.dataFetcher.fetchData(new InputDataSpec().withEvaluationSlices(Collections.singleton(evaluationSlice)))
            .getEvaluations()
            .get(evaluationSlice);
    Map<String, Double> sevenDaysMeanMapeForMetricUrns =
        getMeanMapeForEachMetricUrn(evaluations, evaluationTimeStamp, 7);
    Map<String, Double> thirtyDaysMeanMapeForMetricUrns =
        getMeanMapeForEachMetricUrn(evaluations, evaluationTimeStamp, 30);
    Map<String, Boolean> evaluationResultForMetricUrns = sevenDaysMeanMapeForMetricUrns.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> (e.getValue() - thirtyDaysMeanMapeForMetricUrns.get(e.getKey())) <= threshold));
    if (evaluationResultForMetricUrns.values().stream().anyMatch(v -> v.equals(false))) {
      return new ModelEvaluationResult(ModelStatus.GOOD);
    }
    return new ModelEvaluationResult(ModelStatus.BAD);
  }

  private Map<String, Double> getMeanMapeForEachMetricUrn(Collection<EvaluationDTO> evaluations,
      Instant evaluationTimeStamp, int days) {
    return evaluations.stream()
        .filter(eval -> evaluationTimeStamp.toDateTime().minusDays(days).getMillis() < eval.getStartTime())
        .collect(
            Collectors.groupingBy(EvaluationBean::getMetricUrn, Collectors.averagingDouble(EvaluationBean::getMape)));
  }

  @Override
  public void init(MapeTrendModelEvaluatorSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.threshold = spec.getThreshold();
  }
}
