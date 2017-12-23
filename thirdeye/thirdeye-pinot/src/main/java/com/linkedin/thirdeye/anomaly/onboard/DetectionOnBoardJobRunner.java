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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectionOnBoardJobRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnBoardJobRunner.class);
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public static final String ABORT_ON_FAILURE= "abortOnFailure";
  private final DetectionOnboardJobContext jobContext;
  private final List<DetectionOnboardTask> tasks;
  private final DetectionOnboardJobStatus jobStatus;
  private final int taskTimeOutSize;
  private final TimeUnit taskTimeOutUnit;

  public DetectionOnBoardJobRunner(DetectionOnboardJobContext jobContext, List<DetectionOnboardTask> tasks,
      DetectionOnboardJobStatus jobStatus) {
    this(jobContext, tasks, jobStatus, 5, TimeUnit.MINUTES);
  }

  public DetectionOnBoardJobRunner(DetectionOnboardJobContext jobContext, List<DetectionOnboardTask> tasks,
      DetectionOnboardJobStatus jobStatus, int taskTimeOutSize, TimeUnit taskTimeOutUnit) {
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);
    Preconditions.checkNotNull(taskTimeOutUnit);

    this.jobContext = jobContext;
    this.tasks = tasks;
    this.jobStatus = jobStatus;
    this.taskTimeOutSize = taskTimeOutSize;
    this.taskTimeOutUnit = taskTimeOutUnit;
  }

  @Override
  public void run() {
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);

    for (DetectionOnboardTask task : tasks) {
      DetectionOnboardTaskStatus taskStatus =
          new DetectionOnboardTaskStatus(task.getTaskName(), TaskConstants.TaskStatus.WAITING, "");
      jobStatus.addTaskStatus(taskStatus);

      // Construct Task context and configuration
      Configuration taskConfig = jobContext.getConfiguration().subset(task.getTaskName());
      final boolean abortOnFailure = taskConfig.getBoolean(ABORT_ON_FAILURE, true);
      DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();
      taskContext.setConfiguration(taskConfig);
      taskContext.setExecutionContext(jobContext.getExecutionContext());

      // Submit task
      try {
        task.setTaskContext(taskContext);
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);
        Future<DetectionOnboardTaskStatus> taskFuture = executorService.submit(new DetectionOnboardTaskRunner(task));
        // Wait until time out
        DetectionOnboardTaskStatus returnedTaskStatus = taskFuture.get(taskTimeOutSize, taskTimeOutUnit);
        taskStatus.setTaskStatus(returnedTaskStatus.getTaskStatus());
        taskStatus.setMessage(returnedTaskStatus.getMessage());
      } catch (TimeoutException e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.TIMEOUT);
        LOG.warn("Task {} timed out.", task.getTaskName());
      } catch (InterruptedException e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
        taskStatus.setMessage("Job execution is interrupted.");
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        jobStatus.setMessage(String.format("Job execution is interrupted: %s", ExceptionUtils.getStackTrace(e)));
        LOG.error("Job execution is interrupted.", e);
        return; // Stop executing the job because the thread to execute the job is interrupted.
      } catch (Exception e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
        taskStatus.setMessage(String.format("Execution Error: %s", ExceptionUtils.getStackTrace(e)));
        LOG.error("Encountered unknown error while running job {}.", jobContext.getJobName(), e);
      }

      if (abortOnFailure && !TaskConstants.TaskStatus.COMPLETED.equals(taskStatus.getTaskStatus())) {
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        LOG.error("Failed to execute job {}.", jobContext.getJobName());
        return;
      }
    }
    jobStatus.setJobStatus(JobConstants.JobStatus.COMPLETED);
  }
}
