package com.linkedin.thirdeye.anomaly.job;

import java.util.List;

public interface JobRunner extends Runnable {

  long createJob();
  List<Long> createTasks();
}
