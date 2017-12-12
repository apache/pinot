package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;

public class DetectionOnBoardJobRunner implements Runnable {
  private static final int TIME_OUT_SIZE = 5;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.MINUTES;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private final DetectionOnboardJobContext jobContext;
  private final List<DetectionOnboardTask> tasks;
  private final DetectionOnboardJobStatus jobStatus;

  public DetectionOnBoardJobRunner(DetectionOnboardJobContext jobContext, List<DetectionOnboardTask> tasks,
      DetectionOnboardJobStatus jobStatus) {
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);

    this.jobContext = jobContext;
    this.tasks = tasks;
    this.jobStatus = jobStatus;
  }

  @Override
  public void run() {
    for (DetectionOnboardTask task : tasks) {
      Configuration taskConfig = jobContext.getConfiguration().subset(task.getTaskName());
      boolean abortAtFailure = taskConfig.getBoolean("abortAtFailure", true);
      try {
        // Construct Task context
        DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();
        // Submit task
        task.setTaskContext(taskContext);
        Future<?> taskFuture = executorService.submit(task);
        // Wait until time out
        taskFuture.get(TIME_OUT_SIZE, TIME_OUT_UNIT);
      } catch (Exception e) {

      }
    }
  }
}
