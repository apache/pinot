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

import java.util.Arrays;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.MapeAveragePercentageChangeModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.model.ModelEvaluationResult;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.joda.time.Instant;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MapePercentageChangeModelEvaluatorTest {
  private InputDataFetcher dataFetcher;

  @BeforeMethod
  public void setUp() {
    MockDataProvider dataProvider = new MockDataProvider();
    long mockDetectionConfigId = 100L;
    String mockMetricUrn = "thirdeye:metric:1";
    EvaluationDTO eval1 = makeMockEvaluationDTO(mockDetectionConfigId, mockMetricUrn, 1557187200000L,1557273600000L, 0.06);
    EvaluationDTO eval2 = makeMockEvaluationDTO(mockDetectionConfigId, mockMetricUrn, 1555368321000L,1555454721000L, 0.055);
    dataProvider.setEvaluations(Arrays.asList(eval1, eval2));
    dataFetcher = new DefaultInputDataFetcher(dataProvider, mockDetectionConfigId);
  }

  private EvaluationDTO makeMockEvaluationDTO(long mockDetectionConfigId, String mockMetricUrn, long start, long end, Double mape) {
    EvaluationDTO eval = new EvaluationDTO();
    eval.setStartTime(start);
    eval.setEndTime(end);
    eval.setMetricUrn(mockMetricUrn);
    eval.setMape(mape);
    eval.setDetectionConfigId(mockDetectionConfigId);
    return eval;
  }

  @Test
  public void testEvaluateModelGood() {
    MapeAveragePercentageChangeModelEvaluatorSpec spec = new MapeAveragePercentageChangeModelEvaluatorSpec();
    spec.setThreshold(0.1);
    MapeAveragePercentageChangeModelEvaluator evaluator = new MapeAveragePercentageChangeModelEvaluator();
    evaluator.init(spec, dataFetcher);
    ModelEvaluationResult result = evaluator.evaluateModel(Instant.parse("2019-05-08T20:00:00.000Z"));
    Assert.assertEquals(result.getStatus(), ModelStatus.GOOD);
  }

  @Test
  public void testEvaluateModelBad() {
    MapeAveragePercentageChangeModelEvaluatorSpec spec = new MapeAveragePercentageChangeModelEvaluatorSpec();
    spec.setThreshold(0.01);
    MapeAveragePercentageChangeModelEvaluator evaluator = new MapeAveragePercentageChangeModelEvaluator();
    evaluator.init(spec, dataFetcher);
    ModelEvaluationResult result = evaluator.evaluateModel(Instant.parse("2019-05-08T20:00:00.000Z"));
    Assert.assertEquals(result.getStatus(), ModelStatus.BAD);
  }
}
