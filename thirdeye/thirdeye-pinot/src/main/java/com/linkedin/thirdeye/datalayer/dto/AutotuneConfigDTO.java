package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.FilterPattern;
import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AutotuneConfigDTO extends AutotuneConfigBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AutotuneConfigDTO.class);
  private AlertFilter alertFilter;
  public static final String PATTERN = "pattern";
  public static final String FEATURES = "features";
  public static final String SENSITIVITY = "sensitivity";
  // user defined pattern to be used each time when tuning alert filter
  private FilterPattern userDefinedPattern;
  private String features;
  private String sensitivity;

  public AutotuneConfigDTO() {

  }

  public AutotuneConfigDTO(BaseAlertFilter alertFilter){
    setAlertFilter(alertFilter);
    setAutoTuneConfigByAlertFilter(alertFilter);
  }

  public void setAlertFilter(AlertFilter alertFilter) {
    this.alertFilter = alertFilter;
  }

  public AlertFilter getAlertFilter() {
    return this.alertFilter;
  }

  public void setUserDefinedPattern(FilterPattern userDefinedPattern) {
    this.userDefinedPattern = userDefinedPattern;
  }

  public FilterPattern getUserDefinedPattern() {
    return this.userDefinedPattern;
  }

  public void setFeatures(String features) {
    this.features = features;
  }

  public String getFeatures() {
    return this.features;
  }

  public void setAutoTuneConfigByAlertFilter(BaseAlertFilter alertFilter) {
    Properties alertFilterProperties = alertFilter.toProperties();
    this.userDefinedPattern = FilterPattern.valueOf(alertFilterProperties.getProperty(PATTERN, String.valueOf(FilterPattern.TWO_SIDED)));
    this.features = alertFilterProperties.getProperty(FEATURES, "");
    this.sensitivity = alertFilterProperties.getProperty(SENSITIVITY, "MEDIAN");
  }
}
