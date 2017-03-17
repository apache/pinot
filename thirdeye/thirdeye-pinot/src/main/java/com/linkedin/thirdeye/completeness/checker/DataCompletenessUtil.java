package com.linkedin.thirdeye.completeness.checker;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;


public class DataCompletenessUtil {
  private DataCompletenessUtil() {
    // left blank
  }

  public static double getPercentCompleteness(PercentCompletenessFunctionInput input) {
    DataCompletenessConstants.DataCompletenessAlgorithmName algorithm = input.getAlgorithm();
    List<Long> baselineCounts = input.getBaselineCounts();
    Long currentCount = input.getCurrentCount();

    double percentCompleteness = 0;
    double baselineTotalCount = 0;
    if (CollectionUtils.isNotEmpty(baselineCounts)) {
      switch (algorithm) {
        case WO4W_AVERAGE:
        default:
          for (Long baseline : baselineCounts) {
            baselineTotalCount = baselineTotalCount + baseline;
          }
          baselineTotalCount = baselineTotalCount/baselineCounts.size();
          break;
      }
    }
    if (baselineTotalCount != 0) {
      percentCompleteness = new Double(currentCount * 100) / baselineTotalCount;
    }
    if (baselineTotalCount == 0 && currentCount != 0) {
      percentCompleteness = 100;
    }
    return percentCompleteness;
  }
}
