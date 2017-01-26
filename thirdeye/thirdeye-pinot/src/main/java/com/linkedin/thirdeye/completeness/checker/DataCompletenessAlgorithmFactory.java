package com.linkedin.thirdeye.completeness.checker;


import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessAlgorithmName;

public class DataCompletenessAlgorithmFactory {


  public static DataCompletenessAlgorithm getDataCompletenessAlgorithmFromName(DataCompletenessAlgorithmName algorithmName) {
    DataCompletenessAlgorithm dataCompletenessAlgorithm = null;
    switch (algorithmName) {
      case WO4W_AVERAGE:
      default:
        dataCompletenessAlgorithm = new Wo4WAvgDataCompletenessAlgorithm();
        break;
    }
    return dataCompletenessAlgorithm;
  }

}
