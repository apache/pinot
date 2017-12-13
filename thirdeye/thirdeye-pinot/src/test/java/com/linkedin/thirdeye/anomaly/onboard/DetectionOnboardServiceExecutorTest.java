package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    Map<String, String> properties = new HashMap<>();
    properties.put("task1.property1", "value11");
    properties.put("task1.property2", "value12");
    properties.put("task2.property1", "value21");

    DetectionOnboardJobStatus detectionJobStatus =
        executor.createDetectionOnboardingJob(new MockedDetectionOnboardJob("MockedOnboardJob"), properties);
    Thread.sleep(1000);
    JobConstants.JobStatus jobStatus = detectionJobStatus.getJobStatus();
    Assert.assertTrue(
        JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
  }

  @Test(dependsOnMethods = "testCreate")
  public void testDuplicateJobs() {
    DetectionOnboardJobStatus duplicateDetectionJobStatus = executor
        .createDetectionOnboardingJob(new MockedDetectionOnboardJob("MockedOnboardJob"),
            Collections.<String, String>emptyMap());
    JobConstants.JobStatus jobStatus = duplicateDetectionJobStatus.getJobStatus();
    Assert.assertEquals(jobStatus, JobConstants.JobStatus.FAILED);
    Assert.assertNotNull(duplicateDetectionJobStatus.getMessage());
  }

  @Test
  public void testSubmitAfterShutdown() throws InterruptedException {
    DetectionOnboardServiceExecutor executor = new DetectionOnboardServiceExecutor();
    executor.start();
    executor.shutdown();
    DetectionOnboardJobStatus detectionJobStatus = executor
        .createDetectionOnboardingJob(new MockedDetectionOnboardJob("MockedOnboardJob"),
            Collections.<String, String>emptyMap());
    JobConstants.JobStatus jobStatus = detectionJobStatus.getJobStatus();
    Assert.assertEquals(jobStatus, JobConstants.JobStatus.FAILED);
    Assert.assertNotNull(detectionJobStatus.getMessage());
  }

  static class MockedDetectionOnboardJob extends BaseDetectionOnboardJob {

    public MockedDetectionOnboardJob(String jobName) {
      super(jobName);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new MockedDetectionOnboardTask("task1"));
      taskList.add(new MockedDetectionOnboardTask("task2"));
      return taskList;
    }
  }

  static class MockedDetectionOnboardTask extends BaseDetectionOnboardTask {

    public MockedDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() {
      DetectionOnboardExecutionContext executionContext = this.getTaskContext().getExecutionContext();
      executionContext.setExecutionResult(getTaskName(), getTaskContext().getConfiguration());
    }
  }
}
