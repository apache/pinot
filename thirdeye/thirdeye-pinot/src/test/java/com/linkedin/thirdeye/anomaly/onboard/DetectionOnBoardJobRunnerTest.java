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
import org.apache.commons.configuration.StrictConfigurationComparator;
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
    final int jobId = 1;
    final String jobName = "normalJob";

    DetectionOnboardJob onboardJob = new NormalDetectionOnboardJob(jobName, Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);

    DetectionOnboardJobStatus jobStatus =
        new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);
    jobRunner.run();
    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.COMPLETED);
    Assert.assertEquals(jobStatus.getTaskStatuses().size(), 2);
    for (DetectionOnboardTaskStatus taskStatus : jobStatus.getTaskStatuses()) {
      Assert.assertEquals(taskStatus.getTaskStatus(), TaskConstants.TaskStatus.COMPLETED);
    }

    // Check execution context
    DetectionOnboardExecutionContext executionContext = jobContext.getExecutionContext();
    String task1Result = (String) executionContext.getExecutionResult(NormalDetectionOnboardJob.TASK1_NAME);
    Assert.assertEquals(task1Result, NormalDetectionOnboardJob.TASK1_NAME + NormalDetectionOnboardTask.VALUE_POSTFIX);

    String task2Result = (String) executionContext.getExecutionResult(NormalDetectionOnboardJob.TASK2_NAME);
    Assert.assertEquals(task2Result, NormalDetectionOnboardJob.TASK2_NAME + NormalDetectionOnboardTask.VALUE_POSTFIX);
  }

  @Test
  public void testTaskConfig() {
    final int jobId = 1;
    final String jobName = "normalJob";

    Map<String, String> properties = new HashMap<>();
    properties.put("task1.property1", "value11");
    properties.put("task1.property2", "value12");
    properties.put("task2.property1", "value21");

    DetectionOnboardJob onboardJob = new LogConfigDetectionOnboardJob(jobName, properties);
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);

    DetectionOnboardJobStatus jobStatus =
        new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");

    // Submit the job to executor
    DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);
    jobRunner.run();
    Assert.assertEquals(jobStatus.getJobStatus(), JobConstants.JobStatus.COMPLETED);

    // Check execution context
    DetectionOnboardExecutionContext executionContext = jobContext.getExecutionContext();
    {
      Configuration task1Result =
          (Configuration) executionContext.getExecutionResult(LogConfigDetectionOnboardJob.TASK1_NAME);
      Map<String, String> expectedTask1Property = new HashMap<>();
      expectedTask1Property.put("property1", "value11");
      expectedTask1Property.put("property2", "value12");
      Configuration expectedTask1Config = new MapConfiguration(expectedTask1Property);
      Assert.assertTrue(new StrictConfigurationComparator().compare(task1Result, expectedTask1Config));
    }
    {
      Configuration task2Result =
          (Configuration) executionContext.getExecutionResult(LogConfigDetectionOnboardJob.TASK2_NAME);
      Map<String, String> expectedTask2Property = new HashMap<>();
      expectedTask2Property.put("property1", "value21");
      Configuration expectedTask1Config = new MapConfiguration(expectedTask2Property);
      Assert.assertTrue(new StrictConfigurationComparator().compare(task2Result, expectedTask1Config));
    }
  }

  @Test
  public void testAbortAtTimeOut() {
    final int jobId = 1;
    final String jobName = "abortAtTimeOutJob";

    DetectionOnboardJob onboardJob = new TimeOutDetectionOnboardJob(jobName, Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);

    DetectionOnboardJobStatus jobStatus =
        new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");

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
  public void testAbortOnFailure() {
    final int jobId = 1;
    final String jobName = "abortOnFailureJob";

    DetectionOnboardJob onboardJob = new HasFailureDetectionOnboardJob(jobName, Collections.<String, String>emptyMap());
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);

    DetectionOnboardJobStatus jobStatus =
        new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");

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
  public void testContinueOnFailure() {
    final int jobId = 1;
    final String jobName = "continueOnFailureJob";

    Map<String, String> properties = new HashMap<>();
    properties.put("faultyTask." + DetectionOnBoardJobRunner.ABORT_ON_FAILURE, "false");

    DetectionOnboardJob onboardJob = new HasFailureDetectionOnboardJob(jobName, properties);
    List<DetectionOnboardTask> tasks = onboardJob.getTasks();
    Configuration configuration = onboardJob.getTaskConfiguration();
    DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobId, jobName, configuration);

    DetectionOnboardJobStatus jobStatus =
        new DetectionOnboardJobStatus(jobId, jobName, JobConstants.JobStatus.SCHEDULED, "");

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
    public TimeOutDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new TimeOutDetectionOnboardTask("timeOutTask"));
      taskList.add(new NormalDetectionOnboardTask("someNormalTask"));
      return taskList;
    }
  }

  static class NormalDetectionOnboardJob extends BaseDetectionOnboardJob {
    static final String TASK1_NAME = "task1";
    static final String TASK2_NAME = "task2";

    public NormalDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new NormalDetectionOnboardTask(TASK1_NAME));
      taskList.add(new NormalDetectionOnboardTask(TASK2_NAME));
      return taskList;
    }
  }

  static class NormalDetectionOnboardTask extends BaseDetectionOnboardTask {
    static final String VALUE_POSTFIX = "Value";

    public NormalDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() {
      String taskName = getTaskName();
      DetectionOnboardExecutionContext executionContext = this.getTaskContext().getExecutionContext();
      executionContext.setExecutionResult(taskName, taskName + VALUE_POSTFIX);
    }
  }

  static class LogConfigDetectionOnboardJob extends BaseDetectionOnboardJob {
    static final String TASK1_NAME = "task1";
    static final String TASK2_NAME = "task2";

    public LogConfigDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new LogConfigDetectionOnboardTask(TASK1_NAME));
      taskList.add(new LogConfigDetectionOnboardTask(TASK2_NAME));
      return taskList;
    }
  }

  static class LogConfigDetectionOnboardTask extends BaseDetectionOnboardTask {
    public LogConfigDetectionOnboardTask(String taskName) {
      super(taskName);
    }

    @Override
    public void run() {
      Configuration configuration = getTaskContext().getConfiguration();
      getTaskContext().getExecutionContext().setExecutionResult(getTaskName(), configuration);
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
    public HasFailureDetectionOnboardJob(String jobName, Map<String, String> properties) {
      super(jobName, properties);
    }

    @Override
    public Configuration getTaskConfiguration() {
      return new MapConfiguration(properties);
    }

    @Override
    public List<DetectionOnboardTask> getTasks() {
      List<DetectionOnboardTask> taskList = new ArrayList<>();
      taskList.add(new AlwaysFailDetectionOnboardTask("faultyTask"));
      taskList.add(new NormalDetectionOnboardTask("someNormalTask"));
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
