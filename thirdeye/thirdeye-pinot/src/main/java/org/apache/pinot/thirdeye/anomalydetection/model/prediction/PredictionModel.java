/*
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

package org.apache.pinot.thirdeye.anomalydetection.model.prediction;

import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import org.apache.pinot.thirdeye.anomalydetection.context.TimeSeries;
import java.util.List;
import java.util.Properties;

public interface PredictionModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns the properties of this model.
   */
  Properties getProperties();

  /**
   * Compute parameters for this model using the given time series. The parameters could be a
   * dynamic threshold that is computed using the given baselines.
   *
   * @param historicalTimeSeries the history time series for training this model.
   * @param anomalyDetectionContext
   */
  void train(List<TimeSeries> historicalTimeSeries, AnomalyDetectionContext anomalyDetectionContext);
}
