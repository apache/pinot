package org.apache.pinot.thirdeye.cube.cost;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
import java.util.Map;
import org.apache.pinot.thirdeye.cube.data.cube.CubeUtils;


public class OeRatioCostFunction implements CostFunction {
  // The threshold to the contribution to overall changes in percentage
  private static double epsilon = 0.00001;

  public OeRatioCostFunction() {
  }

  public OeRatioCostFunction(Map<String, String> params) {
  }

  /**
   * Returns the cost that consider change difference, change changeRatio, and node size (contribution percentage of a node).
   *
   * In brief, this function uses this formula to compute the cost:
   *   change difference * log(contribution percentage * change changeRatio)
   *
   * In addition, if a node's contribution to overall changes is smaller than the threshold, which is defined when
   * constructing this class, then the cost is always zero.
   *
   * @param parentChangeRatio the changeRatio between baseline and current value of parent node.
   * @param baselineValue the baseline value of the current node.
   * @param currentValue the current value of the current node.
   * @param baselineSize the size of baseline node.
   * @param currentSize the size of current node.
   * @param topBaselineValue the baseline value of the top node.
   * @param topCurrentValue the current value of the top node.
   * @param topBaselineSize the size of baseline data cube .
   * @param topCurrentSize the size of current data cube .
   *
   * @return the cost that consider change difference, change changeRatio, and node size.
   */
  @Override
  public double computeCost(double parentChangeRatio, double baselineValue, double currentValue, double baselineSize,
      double currentSize, double topBaselineValue, double topCurrentValue, double topBaselineSize,
      double topCurrentSize) {

    // Contribution is the size of the node
    double sizeFactor = (baselineSize + currentSize) / (topBaselineSize + topCurrentSize);
    // Ignore <1% nodes
    if (DoubleMath.fuzzyCompare(sizeFactor, 0.01, epsilon) < 0) {
      return 0d;
    }
    Preconditions.checkState(DoubleMath.fuzzyCompare(sizeFactor,0, epsilon) >= 0, "Contribution {} is smaller than 0.", sizeFactor);
    Preconditions.checkState(DoubleMath.fuzzyCompare(sizeFactor,1, epsilon) <= 0, "Contribution {} is larger than 1", sizeFactor);
    // The cost function considers change difference, change changeRatio, and node size (i.e., sizeFactor)
    return fillEmptyValuesAndGetError(baselineValue, currentValue, parentChangeRatio, sizeFactor);
  }

  private static double error(double baselineValue, double currentValue, double parentRatio, double sizeFactor) {
    double expectedBaselineValue = parentRatio * baselineValue;
    double expectedRatio = currentValue / expectedBaselineValue;
    double weightedExpectedRatio = (expectedRatio - 1) * sizeFactor + 1;
    double logExpRatio = Math.log(weightedExpectedRatio);
    return (currentValue - expectedBaselineValue) * logExpRatio;
  }

  /**
   * Calculates the error if either baseline or current value is missing (i.e., value is zero).
   *
   * @param baseline the baseline value.
   * @param currentValue the current value.
   * @param parentRatio parent's change ratio, which is used to produce a virtual change ratio for the node.
   * @param sizeFactor the size factor of the node w.r.t. the entire data.
   *
   * @return the error of the given baseline and current value.
   */
  private static double errorWithMissingBaselineOrCurrent(double baseline, double currentValue, double parentRatio,
      double sizeFactor) {
    parentRatio = CubeUtils.ensureChangeRatioDirection(baseline, currentValue, parentRatio);
    double logExpRatio = Math.log((parentRatio - 1) * sizeFactor + 1);
    return (currentValue - baseline) * logExpRatio;
  }

  /**
   * Auto fill in baselineValue and currentValue using parentRatio when one of them is zero.
   * If baselineValue and currentValue both are zero or parentRatio is not finite, this function returns 0.
   */
  private static double fillEmptyValuesAndGetError(double baselineValue, double currentValue, double parentRatio,
      double sizeFactor) {
    if (Double.compare(0., parentRatio) == 0 || Double.isNaN(parentRatio)) {
      parentRatio = 1d;
    }
    if (Double.compare(0., baselineValue) != 0 && Double.compare(0., currentValue) != 0) {
      return error(baselineValue, currentValue, parentRatio, sizeFactor);
    } else if (Double.compare(baselineValue, 0d) == 0 || Double.compare(currentValue, 0d) == 0) {
      if (Double.compare(0., baselineValue) == 0) {
        return errorWithMissingBaselineOrCurrent(0d, currentValue, parentRatio, sizeFactor);
      } else {
        return errorWithMissingBaselineOrCurrent(baselineValue, 0d, parentRatio, sizeFactor);
      }
    } else { // baselineValue and currentValue are zeros. Set cost to zero so the node will be naturally aggregated to its parent.
      return 0.;
    }
  }
}
