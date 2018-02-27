package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DetectionOnboardServiceExecutorTest {
  private DetectionOnboardServiceExecutor executor;

  @BeforeClass
  public void initExecutor() {
    executor = new DetectionOnboardServiceExecutor();
    executor.start();
  }

  @AfterClass
  public void shutdownExecutor() {
    executor.shutdown();
  }

  @Test
  public void testCreate() throws InterruptedException {
    long jobId = executor.createDetectionOnboardingJob(
        new DummyDetectionOnboardJob("NormalOnboardJob", Collections.<String, String>emptyMap()));
    Thread.sleep(1000);
    JobConstants.JobStatus jobStatus = executor.getDetectionOnboardingJobStatus(jobId).getJobStatus();
    Assert.assertTrue(
        JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
  }

  @Test(dependsOnMethods = "testCreate", expectedExceptions = IllegalArgumentException.class)
  public void testDuplicateJobs() {
    executor.createDetectionOnboardingJob(
        new DummyDetectionOnboardJob("NormalOnboardJob", Collections.<String, String>emptyMap()));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDuplicateTasks() {
    executor.createDetectionOnboardingJob(
        new DuplicateTaskDetectionOnboardJob("DuplicateTaskOnboardJob", Collections.<String, String>emptyMap()));
  }

  @Test(expectedExceptions = RejectedExecutionException.class)
  public void testSubmitAfterShutdown() throws InterruptedException {
    DetectionOnboardServiceExecutor executor = new DetectionOnboardServiceExecutor();
    executor.start();
    executor.shutdown();
    executor.createDetectionOnboardingJob(
        new DummyDetectionOnboardJob("NormalOnboardJob", Collections.<String, String>emptyMap()));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullTaskList() {
    executor.createDetectionOnboardingJob(
        new NullTaskListDetectionOnboardJob("NullTaskListJob", Collections.<String, String>emptyMap()));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullConfiguration() {
    executor.createDetectionOnboardingJob(
        new NullConfigurationDetectionOnboardJob("NullConfigurationJob", Collections.<String, String>emptyMap()));
  }

  static class DummyDetectionOnboardJob extends BaseDetectionOnboardJob {

    public DummyDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new DummyDetectionOnboardTask("task1"));
      taskList.add(new DummyDetectionOnboardTask("task2"));
      return taskList;
    }
  }

  static class DummyDetectionOnboardTask extends BaseDetectionOnboardTask {

    public DummyDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() { }
  }

  static class DuplicateTaskDetectionOnboardJob extends BaseDetectionOnboardJob {

    public DuplicateTaskDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new DummyDetectionOnboardTask("task"));
      taskList.add(new DummyDetectionOnboardTask("task"));
      return taskList;
    }
  }

  static class NullTaskListDetectionOnboardJob extends BaseDetectionOnboardJob {

    public NullTaskListDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      return null;
    }
  }

  static class NullConfigurationDetectionOnboardJob extends BaseDetectionOnboardJob {

    public NullConfigurationDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return null;
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      return Collections.emptyList();
    }
  }

}
