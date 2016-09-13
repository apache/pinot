package com.linkedin.thirdeye.integration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobContext;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobRunner;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskInfoFactory;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;


public class TestDetectionJobRunner  extends AbstractRunnerDbTestBase {

  DetectionJobRunner detectionJobRunner;
  DetectionJobContext detectionJobContext;
  JobExecutionContext mockedJobExecutionContext;
  JobDetail mockedJobDetail;
  AnomalyFunctionSpec anomalyFunctionSpec;

  @Test
  public void setup() {

    detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyJobDAO(anomalyJobDAO);
    detectionJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    detectionJobContext.setAnomalyFunctionDAO(anomalyFunctionDAO);
    anomalyFunctionSpec = getTestFunctionSpec("m1", "c");
    long functionId = anomalyFunctionDAO.save(anomalyFunctionSpec);
    String functionName = anomalyFunctionSpec.getFunctionName();
    String jobName = String.format("%s_%d", functionName, functionId);
    detectionJobContext.setAnomalyFunctionId(functionId);
    detectionJobContext.setJobName(jobName);

    Map<String, Object> jobDataHashMap = new HashMap<>();
    jobDataHashMap.put(DetectionJobRunner.DETECTION_JOB_CONTEXT, detectionJobContext);
    JobDataMap jobDataMap = new JobDataMap(jobDataHashMap);
    mockedJobDetail = Mockito.mock(JobDetail.class);
    Mockito.when(mockedJobDetail.getKey()).thenReturn(JobKey.jobKey(jobName));
    Mockito.when(mockedJobDetail.getJobDataMap()).thenReturn(jobDataMap);
    mockedJobExecutionContext = Mockito.mock(JobExecutionContext.class);
    Mockito.when(mockedJobExecutionContext.getJobDetail()).thenReturn(mockedJobDetail);

    detectionJobRunner = new DetectionJobRunner();
  }

  @Test(dependsOnMethods = {"setup"})
  public void testCreateDetectionJob() throws JobExecutionException, IOException {

    detectionJobRunner.execute(mockedJobExecutionContext);

    List<AnomalyJobSpec> anomalyJobSpecs = anomalyJobDAO.findAll();
    AnomalyJobSpec anomalyJob = anomalyJobSpecs.get(0);
    Assert.assertEquals(anomalyJobSpecs.size(), 1);
    Assert.assertEquals(anomalyJob.getJobName(), detectionJobContext.getJobName());
    Assert.assertEquals(anomalyJob.getStatus(), JobStatus.SCHEDULED);

    List<AnomalyTaskSpec> anomalyTaskSpecs = anomalyTaskDAO.findAll();
    AnomalyTaskSpec anomalyTask = anomalyTaskSpecs.get(0);
    Assert.assertEquals(anomalyTaskSpecs.size(), 1);
    Assert.assertEquals(anomalyTask.getJobName(), detectionJobContext.getJobName());
    Assert.assertEquals(anomalyTask.getTaskType(), TaskType.ANOMALY_DETECTION);
    Assert.assertEquals(anomalyTask.getStatus(), TaskStatus.WAITING);
    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) TaskInfoFactory.getTaskInfoFromTaskType(TaskType.ANOMALY_DETECTION, anomalyTask.getTaskInfo());
    Assert.assertEquals(detectionTaskInfo.getJobExecutionId(), anomalyJob.getId());
    Assert.assertNull(detectionTaskInfo.getGroupByDimension());
    Assert.assertEquals(detectionTaskInfo.getAnomalyFunctionSpec(), anomalyFunctionSpec);
    Assert.assertEquals(anomalyTask.getJob(), anomalyJob);

    anomalyJobDAO.delete(anomalyJob);
    anomalyTaskDAO.delete(anomalyTask);


  }

}
