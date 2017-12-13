package com.linkedin.thirdeye.anomaly.onboard;


public interface DetectionOnboardTask {

  String getTaskName();

  void setTaskContext(DetectionOnboardTaskContext taskContext);

  DetectionOnboardTaskContext getTaskContext();

  void run();
}
