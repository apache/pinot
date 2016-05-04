package com.linkedin.thirdeye.dashboard.views.heatmap;

import java.text.DecimalFormat;

public class HeatMapCell {

  private static String[] columns;
  private static DecimalFormat decimalFormat;
  private double contributionToOverallChange;

  public HeatMapCell(String dimensionValue, double baselineValue, double baselineCDFValue,
      double baselineContribution, double currentValue, double currentCDFValue,
      double currentContribution, double percentageChange, double absoluteChange,
      double contributionDifference, double contributionToOverallChange, double deltaColor,
      double deltaSize, double contributionColor, double contributionSize,
      double contributionToOverallColor, double contributionToOverallSize) {
    super();
    this.dimensionValue = dimensionValue;
    this.baselineValue = baselineValue;
    this.baselineCDFValue = baselineCDFValue;
    this.baselineContribution = baselineContribution;
    this.currentValue = currentValue;
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
  }

  String dimensionValue;

  // baseline
  double baselineValue;

  double baselineCDFValue;

  double baselineContribution;

  // current
  double currentValue;

  double currentCDFValue;

  double currentContribution;

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

  static {
    columns = new String[] {
        "dimensionValue", "baselineValue", "currentValue", //
        "baselineContribution", "currentContribution", //
        "baselineCDFValue", "currentCDFValue", //
        "percentageChange", "absoluteChange", //
        "contributionDifference", "contributionToOverallChange", //
        "deltaColor", "deltaSize", //
        "contributionColor", "contributionSize", //
        "contributionToOverallColor", "contributionToOverallSize"//
    };
    decimalFormat = new DecimalFormat("0.##");
  }

  public String[] toArray() {
    return new String[] {
        dimensionValue, format(baselineValue), format(currentValue), //
        format(baselineContribution), format(currentContribution), //
        format(baselineCDFValue), format(currentCDFValue), //
        format(percentageChange), format(absoluteChange), //
        format(contributionDifference), format(contributionToOverallChange), //
        format(deltaColor), format(deltaSize), //
        format(contributionColor), format(contributionSize), //
        format(contributionToOverallColor), format(contributionToOverallSize)
    };
  }

  public static String format(Double number) {
    return decimalFormat.format(number);
  }

  public static String[] columns() {
    return columns;
  }

}
