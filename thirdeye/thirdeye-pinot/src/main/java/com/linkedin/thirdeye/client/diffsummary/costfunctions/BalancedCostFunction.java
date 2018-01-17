package com.linkedin.thirdeye.client.diffsummary.costfunctions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;


/**
 * This cost function consider change difference, change ratio, and node size (contribution percentage of a node).
 * More for details: {@link #computeCost(double, double, double, double, double)}.
 */
public class BalancedCostFunction implements CostFunction {
  public static final String CHANGE_CONTRIBUTION_THRESHOLD_PARAM = "threshold";
  // The threshold to the contribution to overall changes in percentage
  private double changeContributionThreshold = 3d;

  public BalancedCostFunction() {
  }

  public BalancedCostFunction(Map<String, String> params) {
    if (params.containsKey(CHANGE_CONTRIBUTION_THRESHOLD_PARAM)) {
      String pctThresholdString = params.get(CHANGE_CONTRIBUTION_THRESHOLD_PARAM);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(pctThresholdString));
      this.changeContributionThreshold = Double.parseDouble(pctThresholdString);
    }
  }

  public double getChangeContributionThreshold() {
    return changeContributionThreshold;
  }

  public void setChangeContributionThreshold(double changeContributionThreshold) {
    this.changeContributionThreshold = changeContributionThreshold;
  }

  /**
   * Returns the cost that consider change difference, change ratio, and node size (contribution percentage of a node).
   *
   * In brief, this function uses this formula to compute the cost:
   *   change difference * log(contribution percentage * change ratio)
   *
   * In addition, if a node's contribution to overall changes is smaller than the threshold, which is defined when
   * constructing this class, then the cost is always zero.
   *
   * @param baselineValue the baseline value of the current node.
   * @param currentValue the current value of the current node.
   * @param parentRatio the ratio between baseline and current value of parent node.
   * @param globalBaselineValue the baseline value of the top node.
   * @param globalCurrentValue the current value of the top node.
   *
   * @return the cost that consider change difference, change ratio, and node size.
   */
  @Override
  public double computeCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue) {

    // Typically, users don't care nodes with small contribution to overall changes
    double contributionToOverallChange =
        Math.abs((currentValue - baselineValue) / (globalCurrentValue - globalBaselineValue));
    if (Double.compare(contributionToOverallChange, changeContributionThreshold / 100d) < 0) {
      return 0d;
    }
    // Contribution is the size of the node
    double contribution = (((baselineValue + currentValue) / (globalBaselineValue + globalCurrentValue)));
    Preconditions.checkState(Double.compare(contribution, 0) >= 0, "Contribution {} is smaller than 0.", contribution);
    Preconditions.checkState(Double.compare(contribution, 1) <= 0, "Contribution {} is larger than 1", contribution);
    // The cost function considers change difference, change ratio, and node size (i.e., contribution)
    return fillEmptyValuesAndGetError(baselineValue, currentValue, parentRatio, contribution);
  }

  private static double error(double baselineValue, double currentValue, double parentRatio, double contribution) {
    double expectedBaselineValue = parentRatio * baselineValue;
    double expectedRatio = currentValue / expectedBaselineValue;
    double weightedExpectedRatio = (expectedRatio - 1) * contribution + 1;
    double logExpRatio = Math.log(weightedExpectedRatio);
    double cost = (currentValue - expectedBaselineValue) * logExpRatio;
    return cost;
  }

  private static double errorWithEmptyBaseline(double currentValue, double parentRatio) {
    if (Double.compare(parentRatio, 1) < 0) {
      parentRatio = 2 - parentRatio;
    }
    double logExpRatio = Math.log(parentRatio);
    double cost = currentValue * logExpRatio;
    return cost;
  }

  private static double errorWithEmptyCurrent(double baseline, double parentRatio) {
    if (Double.compare(parentRatio, 1) > 0) {
      parentRatio = 2 - parentRatio;
    }
    double logExpRatio = Math.log(parentRatio);
    double cost = -baseline * logExpRatio;
    return cost;
  }

  /**
   * Auto fill in baselineValue and currentValue using parentRatio when one of them is zero.
   * If baselineValue and currentValue both are zero or parentRatio is not finite, this function returns 0.
   */
  private static double fillEmptyValuesAndGetError(double baselineValue, double currentValue, double parentRatio,
      double contribution) {
    if (Double.compare(0., parentRatio) == 0 || Double.isNaN(parentRatio)) {
      parentRatio = 1d;
    }
    if (Double.compare(0., baselineValue) != 0 && Double.compare(0., currentValue) != 0) {
      return error(baselineValue, currentValue, parentRatio, contribution);
    } else if (Double.compare(baselineValue, 0d) == 0 || Double.compare(currentValue, 0d) == 0) {
      if (Double.compare(0., baselineValue) == 0) {
        return errorWithEmptyBaseline(currentValue, parentRatio);
      } else {
        return errorWithEmptyCurrent(baselineValue, parentRatio);
      }
    } else { // baselineValue and currentValue are zeros. Set cost to zero so the node will be naturally aggregated to its parent.
      return 0.;
    }
  }
}
