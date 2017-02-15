package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/15/17.
 */
public class GeneralThresholdAlertFilter extends BaseAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(GeneralThresholdAlertFilter.class);
  public static final String DEFAULT_THRESHOLD_FIELD = "weight";
  public static final String DEFAULT_UP_THRESHOLD = "0.5";
  public static final String DEFAULT_DOWN_THRESHOLD = "0.5";

  public static final String THRESHOLD_FIELD = "thresholdField";
  public static final String UP_THRESHOLD = "upThreshold";
  public static final String DOWN_THRESHOLD = "downThreshold";

  private String thresholdField = DEFAULT_THRESHOLD_FIELD;


  private double upThreshold = Double.parseDouble(DEFAULT_UP_THRESHOLD);
  private double downThreshold = Double.parseDouble(DEFAULT_DOWN_THRESHOLD);

  public GeneralThresholdAlertFilter(){

  }

  public GeneralThresholdAlertFilter(String thresholdField, double upThreshold, double downThreshold){
    setThresholdField(thresholdField);
    setDownThreshold(downThreshold);
    setUpThreshold(upThreshold);
  }

  @Override
  public List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(THRESHOLD_FIELD, UP_THRESHOLD, DOWN_THRESHOLD)));
  }

  private final int MAX_EXPLORE_DEPTH = 3;
  public Double getFieldValueFromClass(MergedAnomalyResultDTO anomaly, String fieldName) throws NoSuchFieldException, IllegalAccessException{
    Class tmpClass = anomaly.getClass();
    NoSuchFieldException noSuchFieldException = null;
    for(int i = 0; i < MAX_EXPLORE_DEPTH; i++){
      Field field = null;
      try {
         field = tmpClass.getDeclaredField(fieldName);
      }
      catch (NoSuchFieldException e){
        if(noSuchFieldException == null){
          noSuchFieldException = e;
        }
        tmpClass = tmpClass.getSuperclass();
        continue;
      }
      if(field != null){
        double val = 0;
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        val = field.getDouble(anomaly);
        field.setAccessible(accessible);
        return val;
      }
    }
    throw noSuchFieldException;
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    // Get required field value from MergedAnomalyResultDTO
    Double fieldVal = null;
    try {
      fieldVal = getFieldValueFromClass(anomaly, thresholdField);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOG.warn("Failed to get the field {} for class {} exception: {}", thresholdField, anomaly.getClass().getSimpleName(), e.toString());
    }
    if(fieldVal == null){ // If fail to get the field value from anomaly, return true to all anomalies
      return true;
    }

    return (fieldVal >= upThreshold) || (fieldVal <= -1 * downThreshold);
  }


  public String getThresholdField() {
    return thresholdField;
  }

  public void setThresholdField(String thresholdField) {
    this.thresholdField = thresholdField;
  }

  public double getUpThreshold() {
    return upThreshold;
  }

  public void setUpThreshold(double upThreshold) {
    this.upThreshold = Math.abs(upThreshold);
  }

  public double getDownThreshold() {
    return downThreshold;
  }

  public void setDownThreshold(double dwnThreshold) {
    this.downThreshold = Math.abs(dwnThreshold);
  }
}
