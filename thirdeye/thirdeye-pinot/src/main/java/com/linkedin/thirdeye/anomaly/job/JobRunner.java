package com.linkedin.thirdeye.anomaly.job;

import java.util.List;

/**
 * An interface for Runner classes of the executors
 */
public interface JobRunner extends Runnable {

  Long createJob();
  List<Long> createTasks();
}
