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

package org.apache.pinot.thirdeye.dashboard.views.heatmap;

import java.text.DecimalFormat;

public class HeatMapCell {

  private static String[] columns;
  private static DecimalFormat decimalFormat;
  private double contributionToOverallChange;

  public HeatMapCell(String dimensionValue, double baselineValue, double currentValue,
      double numeratorBaseline, double denominatorBaseline, double numeratorCurrent, double denominatorCurrent,
      double baselineContribution, double currentContribution, double baselineCDFValue,
      double currentCDFValue, double percentageChange, double absoluteChange,
      double contributionDifference, double contributionToOverallChange, double deltaColor,
      double deltaSize, double contributionColor, double contributionSize,
      double contributionToOverallColor, double contributionToOverallSize, String cellSizeExpression) {
    super();
    this.dimensionValue = dimensionValue;
    this.baselineValue = baselineValue;
    this.numeratorBaseline = numeratorBaseline;
    this.denominatorBaseline = denominatorBaseline;
    this.baselineCDFValue = baselineCDFValue;
    this.baselineContribution = baselineContribution;
    this.currentValue = currentValue;
    this.numeratorCurrent = numeratorCurrent;
    this.denominatorCurrent = denominatorCurrent;
    this.currentCDFValue = currentCDFValue;
    this.currentContribution = currentContribution;
    this.percentageChange = percentageChange;
    this.absoluteChange = absoluteChange;
    this.contributionDifference = contributionDifference;
    this.contributionToOverallChange = contributionToOverallChange;
    this.deltaColor = deltaColor;
    this.deltaSize = deltaSize;
    this.contributionColor = contributionColor;
    this.contributionSize = contributionSize;
    this.contributionToOverallColor = contributionToOverallColor;
    this.contributionToOverallSize = contributionToOverallSize;
    this.cellSizeExpression = cellSizeExpression;
  }

  String dimensionValue;

  // baseline
  double baselineValue;

  double baselineCDFValue;

  double baselineContribution;

  double numeratorBaseline;

  double denominatorBaseline;

  // current
  double currentValue;

  double currentCDFValue;

  double currentContribution;

  double numeratorCurrent;

  double denominatorCurrent;

  // comparison fields
  double percentageChange;

  double absoluteChange;

  double contributionDifference;

  double deltaColor;
  double deltaSize;
  double contributionColor;
  double contributionSize;
  double contributionToOverallColor;
  double contributionToOverallSize;

  String cellSizeExpression;

  static {
    columns = new String[] {
        "dimensionValue", "baselineValue", "currentValue", //
        "numeratorBaseline", "denominatorBaseline", "numeratorCurrent", "denominatorCurrent",
        "baselineContribution", "currentContribution", //
        "baselineCDFValue", "currentCDFValue", //
        "percentageChange", "absoluteChange", //
        "contributionDifference", "contributionToOverallChange", //
        "deltaColor", "deltaSize", //
        "contributionColor", "contributionSize", //
        "contributionToOverallColor", "contributionToOverallSize",//
        "cellSizeExpression"
    };
    decimalFormat = new DecimalFormat("0.##");
  }

  public String[] toArray() {
    return new String[] {
        dimensionValue, format(baselineValue), format(currentValue), //
        format(numeratorBaseline), format(denominatorBaseline), format(numeratorCurrent), format(denominatorCurrent),
        format(baselineContribution), format(currentContribution), //
        format(baselineCDFValue), format(currentCDFValue), //
        format(percentageChange), format(absoluteChange), //
        format(contributionDifference), format(contributionToOverallChange), //
        format(deltaColor), format(deltaSize), //
        format(contributionColor), format(contributionSize), //
        format(contributionToOverallColor), format(contributionToOverallSize),
        cellSizeExpression
    };
  }

  // NOTE: hack for display of small numbers
  public static String format(Double number) {
    if (number == 0.0d)
      return "0.0";
    if (Math.abs(number) <= 0.1)
      return String.format("%.3e", number);
    return decimalFormat.format(number);
  }

  public static String[] columns() {
    return columns;
  }

}
