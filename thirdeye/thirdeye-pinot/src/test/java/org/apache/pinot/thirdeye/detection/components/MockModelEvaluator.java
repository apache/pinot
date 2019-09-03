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

import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.spec.MockModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.ModelEvaluationResult;
import org.apache.pinot.thirdeye.detection.spi.model.ModelStatus;
import org.joda.time.Instant;


public class MockModelEvaluator implements ModelEvaluator<MockModelEvaluatorSpec> {
  private ModelStatus status;

  @Override
  public ModelEvaluationResult evaluateModel(Instant evaluationTimeStamp) {
    return new ModelEvaluationResult(status);
  }

  @Override
  public void init(MockModelEvaluatorSpec spec, InputDataFetcher dataFetcher) {
    this.status = spec.getMockModelStatus();
  }
}
