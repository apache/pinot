package com.linkedin.thirdeye.anomaly;

import java.util.List;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;

public class JobScheduler {
  
  public void start(){
    //read the anomaly function specs
    List<AnomalyFunctionSpec>  functionSpecs = readAnomalyFunctionSpecs();
    for (AnomalyFunctionSpec anomalyFunctionSpec : functionSpecs) {
     scheduleJob(anomalyFunctionSpec); 
    }
  }
  
  private void scheduleJob(AnomalyFunctionSpec anomalyFunctionSpec) {
    
    //write the entry in TASK DB
  }

  private List<AnomalyFunctionSpec> readAnomalyFunctionSpecs() {
    return null;
  }

  public void stop(){
    
  }
}
