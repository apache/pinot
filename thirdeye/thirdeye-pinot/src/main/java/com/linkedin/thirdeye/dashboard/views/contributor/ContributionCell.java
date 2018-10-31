/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.views.contributor;

import java.text.DecimalFormat;

import com.linkedin.thirdeye.dashboard.views.TimeBucket;

public class ContributionCell {
  private static String[] columns;
  private static DecimalFormat decimalFormat;

  private String dimensionValue;
  private TimeBucket timeBucket;

  private double baselineValue;
  private double currentValue;

  private double cumulativeBaselineValue;
  private double cumulativeCurrentValue;

  private double baselineContribution = 0;
  private double currentContribution = 0;

  private double cumulativeBaselineContribution = 0;
  private double cumulativeCurrentContribution = 0;

  private double percentageChange;
  private double contributionDifference;

  private double cumulativeContributionDifference;
  private double cumulativePercentageChange;

  public ContributionCell(String dimensionValue, TimeBucket timeBucket, double baselineValue,
      double currentValue, double cumulativeBaselineValue, double cumulativeCurrentValue) {
    super();
    this.dimensionValue = dimensionValue;
    this.timeBucket = timeBucket;
    this.baselineValue = baselineValue;
    this.currentValue = currentValue;
    this.cumulativeBaselineValue = cumulativeBaselineValue;
    this.cumulativeCurrentValue = cumulativeCurrentValue;
  }

  public void updateContributionStats(double baselineTotal, double currentTotal,
      double cumulativeBaselineTotal, double cumulativeCurrentTotal) {
    if (baselineTotal > 0) {
      baselineContribution = baselineValue * 100 / baselineTotal;
    }
    if (currentTotal > 0) {
      currentContribution = currentValue * 100 / currentTotal;
    }
    if (baselineValue > 0) {
      percentageChange = ((currentValue - baselineValue) / baselineValue) * 100;
    } else {
      if (currentValue > 0) {
        percentageChange = 100;
      } else {
        percentageChange = 0;
      }
    }
    contributionDifference = baselineContribution - currentContribution;
    if (cumulativeBaselineTotal > 0) {
      cumulativeBaselineContribution = cumulativeBaselineValue * 100 / cumulativeBaselineTotal;
    }
    if (cumulativeCurrentTotal > 0) {
      cumulativeCurrentContribution = cumulativeCurrentValue * 100 / cumulativeCurrentTotal;
    }
    if (cumulativeBaselineValue > 0) {
      cumulativePercentageChange =
          ((cumulativeCurrentValue - cumulativeBaselineValue) / cumulativeBaselineValue) * 100;
    } else {
      if (cumulativeCurrentValue > 0) {
        cumulativePercentageChange = 100;
      } else {
        cumulativePercentageChange = 0;
      }
    }
    cumulativeContributionDifference =
        cumulativeBaselineContribution - cumulativeCurrentContribution;

  }

  static {
    columns = new String[] {
        "dimensionValue", "baselineValue", "currentValue", //
        "baselineContribution", "currentContribution", //
        "percentageChange", "contributionDifference",
        // CUMMULATIVE
        "cumulativeBaselineValue", "cumulativeCurrentValue", //
        "cumulativeBaselineContribution", "cumulativeCurrentContribution", //
        "cumulativePercentageChange", "cumulativeContributionDifference"//
    };

    decimalFormat = new DecimalFormat("0.##");
  }

  public String[] toArray() {
    return new String[] {
        dimensionValue, format(baselineValue), format(currentValue), //
        format(baselineContribution), format(currentContribution), //
        format(percentageChange), format(contributionDifference), //
        // CUMMULATIVE
        format(cumulativeBaselineValue), format(cumulativeCurrentValue), //
        format(cumulativeBaselineContribution), format(cumulativeCurrentContribution), //
        format(cumulativePercentageChange), format(cumulativeContributionDifference)
    };
  }

  public TimeBucket getTimeBucket() {
    return timeBucket;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public double getBaselineValue() {
    return baselineValue;
  }

  public double getCurrentValue() {
    return currentValue;
  }

  public double getCumulativeBaselineValue() {
    return cumulativeBaselineValue;
  }

  public double getCumulativeCurrentValue() {
    return cumulativeCurrentValue;
  }

  public double getBaselineContribution() {
    return baselineContribution;
  }

  public double getCurrentContribution() {
    return currentContribution;
  }

  public double getPercentageChange() {
    return percentageChange;
  }

  public double getCumulativeContributionDifference() {
    return cumulativeContributionDifference;
  }

  public double getCumulativePercentageChange() {
    return cumulativePercentageChange;
  }

  public double getContributionDifference() {
    return contributionDifference;
  }

  public static String format(Double number) {
    return decimalFormat.format(number);
  }

  public static String[] columns() {
    return columns;
  }

}
