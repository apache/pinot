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
import org.apache.pinot.thirdeye.detection.spec.MapeTrendModelEvaluatorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.ModelEvaluator;
import org.apache.pinot.thirdeye.detection.spi.model.ModelEvaluationResult;
import org.joda.time.Instant;


/**
 *  Monitor the recent mean MAPE in last 7 days, and compare that with the mean MAPE for the last 30 days.
 *  If it's dropped to a certain percentage threshold, re-tune the model
 */
public class MapeTrendModelEvaluator implements ModelEvaluator<MapeTrendModelEvaluatorSpec> {
  private InputDataFetcher dataFetcher;

  @Override
  public ModelEvaluationResult evaluateModel(Instant evaluationTimeStamp) {
    return null;
  }

  @Override
  public void init(MapeTrendModelEvaluatorSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
  }
}
