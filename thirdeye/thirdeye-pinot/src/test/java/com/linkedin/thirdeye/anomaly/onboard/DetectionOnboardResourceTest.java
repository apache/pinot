package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DetectionOnboardResourceTest {
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionOnboardServiceExecutor executor;
  private DetectionOnboardResource detectionOnboardResource;

  @BeforeClass
  public void initResource() {
    executor = new DetectionOnboardServiceExecutor();
    executor.start();
    detectionOnboardResource = new DetectionOnboardResource(executor);
  }

  @AfterClass
  public void shutdownResource() {
    executor.shutdown();
  }


  @Test
  public void testCreateJob() throws IOException {
    Map<String, String> properties = Collections.emptyMap();

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
