package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DetectionOnboardResourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardResourceTest.class);
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionOnboardServiceExecutor executor;
  private DetectionOnboardResource detectionOnboardResource;
  private DAOTestBase daoTestBase;

  @BeforeClass
  public void initResource() {
    daoTestBase = DAOTestBase.getInstance();
    executor = new DetectionOnboardServiceExecutor();
    executor.start();
    detectionOnboardResource = new DetectionOnboardResource(executor, new MapConfiguration(Collections.emptyMap()));
  }

  @AfterClass
  public void shutdownResource() {
    executor.shutdown();
    daoTestBase.cleanup();
  }


  @Test
  public void testCreateJob() throws IOException {
    Map<String, String> properties = OnboardingTaskTestUtils.getJobProperties();

    String propertiesJson = OBJECT_MAPPER.writeValueAsString(properties);
    String normalJobStatusJson = detectionOnboardResource.createDetectionOnboardingJob("NormalJob", propertiesJson);
    long jobId;
    {
      DetectionOnboardJobStatus onboardJobStatus =
          OBJECT_MAPPER.readValue(normalJobStatusJson, DetectionOnboardJobStatus.class);
      JobConstants.JobStatus jobStatus = onboardJobStatus.getJobStatus();
      Assert.assertTrue(
          JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
      jobId = onboardJobStatus.getJobId();
    }

    {
      DetectionOnboardJobStatus onboardJobStatus = OBJECT_MAPPER
          .readValue(detectionOnboardResource.getDetectionOnboardingJobStatus(jobId), DetectionOnboardJobStatus.class);
      JobConstants.JobStatus jobStatus = onboardJobStatus.getJobStatus();
      LOG.info("Job Status: {}" + jobStatus);
      Assert.assertTrue(
          JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
    }
  }

  @Test(dependsOnMethods = "testCreateJob")
  public void testFailedJobCreation() throws IOException {
    // Trigger error of duplicate job names
    Map<String, String> properties = Collections.emptyMap();

    String propertiesJson = OBJECT_MAPPER.writeValueAsString(properties);
    String normalJobStatusJson = detectionOnboardResource.createDetectionOnboardingJob("NormalJob", propertiesJson);
    DetectionOnboardJobStatus onboardJobStatus =
        OBJECT_MAPPER.readValue(normalJobStatusJson, DetectionOnboardJobStatus.class);
    Assert.assertEquals(onboardJobStatus.getJobStatus(), JobConstants.JobStatus.FAILED);
    Assert.assertNotNull(onboardJobStatus.getMessage());
  }

  @Test
  public void testNonExistingJobId() throws IOException {
    DetectionOnboardJobStatus onboardJobStatus = OBJECT_MAPPER
        .readValue(detectionOnboardResource.getDetectionOnboardingJobStatus(-1L), DetectionOnboardJobStatus.class);
    JobConstants.JobStatus jobStatus = onboardJobStatus.getJobStatus();
    Assert.assertEquals(onboardJobStatus.getJobStatus(), JobConstants.JobStatus.UNKNOWN);
    Assert.assertNotNull(onboardJobStatus.getMessage());
  }

}
