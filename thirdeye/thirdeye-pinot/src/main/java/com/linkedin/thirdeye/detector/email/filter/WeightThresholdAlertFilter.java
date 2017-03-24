package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/14/17.
 *
 * This WeightThresholdAlertFilter checks if the weight level of given merged anomaly result is between given up
 * and down threshold. The up and down threshold should be positive floating point number. This class return false if
 * - downThreshold < weight < upThreshold; otherwise, return true.
 * Note that, down and up thresholds are supposed to be positive double value
 */
public class WeightThresholdAlertFilter extends BaseAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(WeightThresholdAlertFilter.class);

  public static final String DEFAULT_UP_THRESHOLD = Double.toString(Double.POSITIVE_INFINITY);
  public static final String DEFAULT_DOWN_THRESHOLD = Double.toString(Double.POSITIVE_INFINITY);

  public static final String UP_THRESHOLD = "upThreshold";
  public static final String DOWN_THRESHOLD = "downThreshold";

  private double upThreshold = Double.parseDouble(DEFAULT_UP_THRESHOLD);
  private double downThreshold = Double.parseDouble(DEFAULT_DOWN_THRESHOLD);

  public double getUpThreshold() {
    return upThreshold;
  }

  public void setUpThreshold(double upThreshold) {
    this.upThreshold = Math.abs(upThreshold);
  }

  public double getDownThreshold() {
    return downThreshold;
  }

  public void setDownThreshold(double downThreshold) {
    this.downThreshold = Math.abs(downThreshold);
  }


  public WeightThresholdAlertFilter(){

  }

  public WeightThresholdAlertFilter(double upThreshold, double downThreshold){
    setUpThreshold(upThreshold);
    setDownThreshold(downThreshold);
  }

  @Override
  public List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(UP_THRESHOLD, DOWN_THRESHOLD)));
  }

  // Check if the weight of the given MergedAnomalyResultDTO is greater or equal to the up threshold
  // or is less or equal to the down threshold.
  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    double weight = anomaly.getWeight();
    return (weight >= upThreshold) || (weight <= -1 * downThreshold);
  }
}
