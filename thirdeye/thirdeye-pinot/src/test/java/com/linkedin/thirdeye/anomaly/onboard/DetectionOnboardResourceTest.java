package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DetectionOnboardResourceTest {
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionOnboardResource detectionOnboardResource;

  @BeforeClass
  public void testCreate() {
    DetectionOnboardServiceExecutor detectionOnboardServiceExecutor = new DetectionOnboardServiceExecutor();
    detectionOnboardServiceExecutor.start();
    detectionOnboardResource = new DetectionOnboardResource(detectionOnboardServiceExecutor);
  }

  @Test
  public void testCreateJob() throws IOException {
    Map<String, String> properties = Collections.emptyMap();

    String propertiesJson = OBJECT_MAPPER.writeValueAsString(properties);
    String normalJobStatusJson = detectionOnboardResource.createDetectionOnboardingJob("NormalJob", propertiesJson);
    DetectionOnboardJobStatus onboardJobStatus =
        OBJECT_MAPPER.readValue(normalJobStatusJson, DetectionOnboardJobStatus.class);
    JobConstants.JobStatus jobStatus = onboardJobStatus.getJobStatus();
    Assert.assertTrue(
        JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
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

}
