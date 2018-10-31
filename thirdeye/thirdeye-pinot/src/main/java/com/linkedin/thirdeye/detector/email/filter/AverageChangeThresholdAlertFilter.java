package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The filter remove the anomalies whose average current value does not increase over its average baseline value by
 * the threshold, i.e., only anomalies whose (avg. current - avg. baseline > threshold) pass the filter.
 */
public class AverageChangeThresholdAlertFilter extends BaseAlertFilter {
  // These default parameters are accessed through Java reflection. Do not remove.
  public static final String DEFAULT_THRESHOLD = "100";

  public static final String THRESHOLD = "threshold";

  private double threshold = Double.parseDouble(DEFAULT_THRESHOLD);

  @Override
  public List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(THRESHOLD)));
  }

  public double getThreshold() {
    return threshold;
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    double averageCurrent = anomaly.getAvgCurrentVal();
    double averageBaseline = anomaly.getAvgBaselineVal();
    double diff = averageCurrent - averageBaseline;

    return (diff > threshold);
  }
}
