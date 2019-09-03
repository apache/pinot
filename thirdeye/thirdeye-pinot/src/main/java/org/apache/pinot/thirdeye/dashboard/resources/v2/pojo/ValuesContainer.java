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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

public class ValuesContainer {
  // TODO: add percentage change, cummulative values etc here
  double[] currentValues;
  double[] baselineValues;
  String[] percentageChange;

  double[] cumulativeCurrentValues;
  double[] cumulativeBaselineValues;
  String[] cumulativePercentageChange;

  public double[] getBaselineValues() {
    return baselineValues;
  }

  public void setBaselineValues(double[] baselineValues) {
    this.baselineValues = baselineValues;
  }

  public double[] getCurrentValues() {
    return currentValues;
  }

  public void setCurrentValues(double[] currentValues) {
    this.currentValues = currentValues;
  }

  public String[] getPercentageChange() {
    return percentageChange;
  }

  public void setPercentageChange(String[] percentageChange) {
    this.percentageChange = percentageChange;
  }

  public double[] getCumulativeBaselineValues() {
    return cumulativeBaselineValues;
  }

  public void setCumulativeBaselineValues(double[] cumulativeBaselineValues) {
    this.cumulativeBaselineValues = cumulativeBaselineValues;
  }

  public double[] getCumulativeCurrentValues() {
    return cumulativeCurrentValues;
  }

  public void setCumulativeCurrentValues(double[] cumulativeCurrentValues) {
    this.cumulativeCurrentValues = cumulativeCurrentValues;
  }

  public String[] getCumulativePercentageChange() {
    return cumulativePercentageChange;
  }

  public void setCumulativePercentageChange(String[] cumulativePercentageChange) {
    this.cumulativePercentageChange = cumulativePercentageChange;
  }
}
