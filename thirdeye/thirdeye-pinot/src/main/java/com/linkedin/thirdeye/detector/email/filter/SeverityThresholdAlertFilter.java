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
 * This SeverityThresholdAlertFilter checks if the severity level of given merged anomaly result is between given up
 * and down threshold. The up and down threshold should be positive floating point number. This class return false if
 * - downThreshold < severity < upThreshold; otherwise, return true.
 */
public class SeverityThresholdAlertFilter extends BaseAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(SeverityThresholdAlertFilter.class);

  public static final String DEFAULT_UP_THRESHOLD = "0.5";
  public static final String DEFAULT_DOWN_THRESHOLD = "0.5";

  public static final String UP_THRESHOLD = "upThreshold";
  public static final String DOWN_THRESHOLD = "upThreshold";

  private double upThreshold = Double.parseDouble(DEFAULT_UP_THRESHOLD);
  private double dwnThreshold = Double.parseDouble(DEFAULT_DOWN_THRESHOLD);

  public double getUpThreshold() {
    return upThreshold;
  }

  public void setUpThreshold(double upThreshold) {
    this.upThreshold = Math.abs(upThreshold);
  }

  public double getDownThreshold() {
    return dwnThreshold;
  }

  public void setDownThreshold(double dwnThreshold) {
    this.dwnThreshold = Math.abs(dwnThreshold);
  }


  public SeverityThresholdAlertFilter(){

  }

  public SeverityThresholdAlertFilter(double upThreshold, double downThreshold){
    setUpThreshold(upThreshold);
    setDownThreshold(downThreshold);
  }

  @Override
  public List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(UP_THRESHOLD, DOWN_THRESHOLD)));
  }

  // Check if the severity of the given MergedAnomalyResultDTO is greater or equal to the up threshold
  // or is less or equal to the down threshold.
  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    double severity = anomaly.getWeight();
    return (severity >= upThreshold) || (severity <= -1*dwnThreshold);
  }
}
