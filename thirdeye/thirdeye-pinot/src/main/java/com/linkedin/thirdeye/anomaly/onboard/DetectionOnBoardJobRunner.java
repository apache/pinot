package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.exception.ExceptionUtils;

public class DetectionOnBoardJobRunner implements Runnable {
  private static final int TASK_TIME_OUT_SIZE = 5;
  private static final TimeUnit TASK_TIME_OUT_UNIT = TimeUnit.MINUTES;
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
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);

    for (DetectionOnboardTask task : tasks) {
      DetectionOnboardTaskStatus taskStatus = new DetectionOnboardTaskStatus();
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.WAITING);
      jobStatus.addTaskStatus(taskStatus);

      // Construct Task context and configuration
      Configuration taskConfig = jobContext.getConfiguration().subset(task.getTaskName());
      final boolean abortAtFailure = taskConfig.getBoolean("abortAtFailure", true);
      DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();
      taskContext.setConfiguration(taskConfig);
      taskContext.setExecutionContext(jobContext.getExecutionContext());

      // Submit task
      try {
        task.setTaskContext(taskContext);
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);
        Future<DetectionOnboardTaskStatus> taskFuture = executorService.submit(task);
        // Wait until time out
        DetectionOnboardTaskStatus returnedTaskStatus = taskFuture.get(TASK_TIME_OUT_SIZE, TASK_TIME_OUT_UNIT);
        taskStatus.setTaskStatus(returnedTaskStatus.getTaskStatus());
        taskStatus.setMessage(returnedTaskStatus.getMessage());
      } catch (TimeoutException e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.TIMEOUT);
      } catch (Exception e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
        taskStatus.setMessage(String.format("Unknown Error: %s", ExceptionUtils.getStackTrace(e)));
      }

      if (abortAtFailure && !TaskConstants.TaskStatus.COMPLETED.equals(taskStatus.getTaskStatus())) {
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        return;
      }
    }
    jobStatus.setJobStatus(JobConstants.JobStatus.COMPLETED);
  }
}
