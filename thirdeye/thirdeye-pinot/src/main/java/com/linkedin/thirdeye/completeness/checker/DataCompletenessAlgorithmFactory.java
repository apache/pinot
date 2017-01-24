package com.linkedin.thirdeye.completeness.checker;

import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessAlgorithmName;

public class DataCompletenessAlgorithmFactory {

  public static String getDataCompletenessAlgorithmClass(DataCompletenessAlgorithmName algorithmName) {
    String dataCompletenessAlgorithmClass = null;
    switch (algorithmName) {
      case WO4W_AVERAGE:
      default:
        dataCompletenessAlgorithmClass = Wo4WAvgDataCompletenessAlgorithm.class.getName();
        break;
    }
    return dataCompletenessAlgorithmClass;
  }

}
