package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.joda.time.DateTime;


public class EmailContentFormatterContext {
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private AlertConfigDTO alertConfig;
  private DateTime start; // anomaly search region starts
  private DateTime end; // anomaly search region ends

  public AnomalyFunctionDTO getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionDTO anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

  public AlertConfigDTO getAlertConfig() {
    return alertConfig;
  }

  public void setAlertConfig(AlertConfigDTO alertConfig) {
    this.alertConfig = alertConfig;
  }

  public DateTime getStart() {
    return start;
  }

  public void setStart(DateTime start) {
    this.start = start;
  }

  public DateTime getEnd() {
    return end;
  }

  public void setEnd(DateTime end) {
    this.end = end;
  }
}
