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

package org.apache.pinot.thirdeye.anomaly.task;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AnomalyClassifierFactory anomalyClassifierFactory;

  public ThirdEyeAnomalyConfiguration getThirdEyeAnomalyConfiguration() {
    return thirdEyeAnomalyConfiguration;
  }

  public void setThirdEyeAnomalyConfiguration(
      ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration) {
    this.thirdEyeAnomalyConfiguration = thirdEyeAnomalyConfiguration;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public AlertFilterFactory getAlertFilterFactory(){ return  alertFilterFactory; }

  public void setAlertFilterFactory(AlertFilterFactory alertFilterFactory){
    this.alertFilterFactory = alertFilterFactory;
  }

  public AnomalyClassifierFactory getAnomalyClassifierFactory() {
    return anomalyClassifierFactory;
  }

  public void setAnomalyClassifierFactory(AnomalyClassifierFactory anomalyClassifierFactory) {
    this.anomalyClassifierFactory = anomalyClassifierFactory;
  }
}
