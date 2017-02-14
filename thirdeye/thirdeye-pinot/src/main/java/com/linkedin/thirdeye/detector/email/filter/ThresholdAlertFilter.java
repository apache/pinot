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
 */
public class ThresholdAlertFilter extends BaseAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(ThresholdAlertFilter.class);

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
    this.upThreshold = upThreshold;
  }

  public double getDwnThreshold() {
    return dwnThreshold;
  }

  public void setDwnThreshold(double dwnThreshold) {
    this.dwnThreshold = Math.abs(dwnThreshold);
  }


  public ThresholdAlertFilter(){

  }

  public ThresholdAlertFilter(double upThreshold, double dwnThreshold){
    this.upThreshold = upThreshold;
    this.dwnThreshold = Math.abs(dwnThreshold);
  }

  @Override
  public List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(UP_THRESHOLD, DOWN_THRESHOLD)));
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    double severity = anomaly.getWeight();
    return (severity >= upThreshold) || (severity <= -1*dwnThreshold);
  }
}
