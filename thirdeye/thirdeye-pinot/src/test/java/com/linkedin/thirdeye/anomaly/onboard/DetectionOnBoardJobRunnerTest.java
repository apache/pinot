package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DetectionOnBoardJobRunnerTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnBoardJobRunnerTest.class);

  private final static int timeOutSize = 1;
  private final static TimeUnit timeOutUnit = TimeUnit.SECONDS;

  @Test
  public void testNormalRun() {
    final String jobName = "normalJob";

    DetectionOnboardJob onboardJob = new DetectionOnboardServiceExecutorTest.MockedDetectionOnboardJob(jobName);
    onboardJob.initialize(Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, 1, configuration);

    DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
    jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);
    jobRunner.run();
    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.COMPLETED);
    Assert.assertEquals(jobStatus.getTaskStatuses().size(), 2);
    for (DetectionOnboardTaskStatus taskStatus : jobStatus.getTaskStatuses()) {
      Assert.assertEquals(taskStatus.getTaskStatus(), TaskConstants.TaskStatus.COMPLETED);
    }
  }

  @Test
  public void testAbortAtTimeOut() {
    final String jobName = "abortAtTimeOutJob";

    DetectionOnboardJob onboardJob = new TimeOutDetectionOnboardJob(jobName);
    onboardJob.initialize(Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, 1, configuration);

    DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
    jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner =
        new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus, timeOutSize, timeOutUnit);
    jobRunner.run();

    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.FAILED);
    // There should be 1 task status because the second task will not be executed.
    Assert.assertEquals(jobStatus.getTaskStatuses().size(), 1);
    for (DetectionOnboardTaskStatus taskStatus : jobStatus.getTaskStatuses()) {
      Assert.assertEquals(taskStatus.getTaskStatus(), TaskConstants.TaskStatus.TIMEOUT);
    }
  }

  @Test
  public void testAbortAtFailure() {
    final String jobName = "abortAtFailureJob";

    DetectionOnboardJob onboardJob = new HasFailureDetectionOnboardJob(jobName);
    onboardJob.initialize(Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, 1, configuration);

    DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
    jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner =
        new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus, timeOutSize, timeOutUnit);
    jobRunner.run();

    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.FAILED);
    // There should be 1 task status because the second task will not be executed.
    Assert.assertEquals(jobStatus.getTaskStatuses().size(), 1);
    for (DetectionOnboardTaskStatus taskStatus : jobStatus.getTaskStatuses()) {
      Assert.assertEquals(taskStatus.getTaskStatus(), TaskConstants.TaskStatus.FAILED);
    }
  }

  @Test
  public void testContinueAtFailure() {
    final String jobName = "continueAtFailureJob";

    Map<String, String> properties = new HashMap<>();
    properties.put("faultyTask.abortAtFailure", "false");

    DetectionOnboardJob onboardJob = new HasFailureDetectionOnboardJob(jobName);
    onboardJob.initialize(properties);
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, 1, configuration);

    DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
    jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner =
        new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus, timeOutSize, timeOutUnit);
    jobRunner.run();

    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.COMPLETED);
    List<DetectionOnboardTaskStatus> taskStatuses = jobStatus.getTaskStatuses();
    Assert.assertEquals(taskStatuses.size(), 2);
    Assert.assertEquals(taskStatuses.get(0).getTaskStatus(), TaskConstants.TaskStatus.FAILED);
    Assert.assertNotNull(taskStatuses.get(0).getMessage());
    Assert.assertTrue(!taskStatuses.get(0).getMessage().isEmpty());
    Assert.assertEquals(taskStatuses.get(1).getTaskStatus(), TaskConstants.TaskStatus.COMPLETED);
  }

  static class TimeOutDetectionOnboardJob extends BaseDetectionOnboardJob {
    public TimeOutDetectionOnboardJob(String jobName) {
      super(jobName);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new TimeOutDetectionOnboardTask("timeOutTask"));
      taskList.add(new DetectionOnboardServiceExecutorTest.MockedDetectionOnboardTask("task2"));
      return taskList;
    }
  }

  static class TimeOutDetectionOnboardTask extends BaseDetectionOnboardTask {
    public TimeOutDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeOutUnit.toMillis(timeOutSize + 2));
      } catch (InterruptedException e) {
        // Do nothing
      }
    }
  }

  static class HasFailureDetectionOnboardJob extends BaseDetectionOnboardJob {
    public HasFailureDetectionOnboardJob(String jobName) {
      super(jobName);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new AlwaysFailDetectionOnboardTask("faultyTask"));
      taskList.add(new DetectionOnboardServiceExecutorTest.MockedDetectionOnboardTask("task2"));
      return taskList;
    }
  }

  static class AlwaysFailDetectionOnboardTask extends BaseDetectionOnboardTask {
    public AlwaysFailDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() {
      LOG.info("Triggering NullPointerException for TESTING purpose.");
      List<String> nullList = null;
      nullList.get(100);
    }
  }
}
