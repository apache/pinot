package com.linkedin.thirdeye.anomaly;

public class ThirdeyeAnomalyConstants {

  public enum JobStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  };

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  };

  public enum AnomalyFunctionStatus {
    WAITING,
    SCHEDULED,
    FAILED
  };

}
