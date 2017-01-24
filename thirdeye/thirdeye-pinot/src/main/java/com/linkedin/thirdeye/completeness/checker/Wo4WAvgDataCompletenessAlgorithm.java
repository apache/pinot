package com.linkedin.thirdeye.completeness.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessAlgorithmName;
import com.linkedin.thirdeye.dashboard.resources.DataCompletenessResource;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;

public class Wo4WAvgDataCompletenessAlgorithm implements DataCompletenessAlgorithm {

  public static double DEFAULT_EXPECTED_COMPLETENESS = 80;
  public static double CONSIDER_COMPLETE_AFTER = 95;
  public static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private DataCompletenessResource dataCompletenessResource = null;

  public Wo4WAvgDataCompletenessAlgorithm() {
    dataCompletenessResource = new DataCompletenessResource();
  }

  @Override
  public List<Long> getBaselineCounts(String dataset, Long bucketValue) {
    long weekInMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    List<Long> baselineCounts = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      long count = 0;
      long baselineInMS = bucketValue - weekInMillis;
      DataCompletenessConfigDTO config =
          DAO_REGISTRY.getDataCompletenessConfigDAO().findByDatasetAndDateMS(dataset, baselineInMS);
      if (config != null) {
        count = config.getCountStar();
      }
      baselineCounts.add(count);
    }
    return baselineCounts;
  }

  @Override
  public double getPercentCompleteness(List<Long> baselineCounts, Long currentCount) {
    PercentCompletenessFunctionInput input = new PercentCompletenessFunctionInput();
    input.setAlgorithm(DataCompletenessAlgorithmName.WO4W_AVERAGE);
    input.setBaselineCounts(baselineCounts);
    input.setCurrentCount(currentCount);
    String jsonString = PercentCompletenessFunctionInput.toJson(input);
    double percentCompleteness = dataCompletenessResource.getPercentCompleteness(jsonString);
    return percentCompleteness;
  }

  @Override
  public boolean isDataComplete(Double percentComplete, Double expectedCompleteness) {
    boolean isDataComplete = false;
    if (expectedCompleteness == null) {
      expectedCompleteness = DEFAULT_EXPECTED_COMPLETENESS;
    }
    if (percentComplete >= expectedCompleteness) {
      isDataComplete = true;
    }
    return isDataComplete;
  }

  @Override
  public double getConsiderCompleteAfter() {
    return CONSIDER_COMPLETE_AFTER;
  }



}
