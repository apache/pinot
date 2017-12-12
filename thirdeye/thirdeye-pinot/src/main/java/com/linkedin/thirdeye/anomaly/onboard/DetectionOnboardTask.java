package com.linkedin.thirdeye.anomaly.onboard;

import java.util.concurrent.Callable;

public interface DetectionOnboardTask extends Callable {

  String getTaskName();

  void setTaskContext(DetectionOnboardTaskContext taskContext);
}
