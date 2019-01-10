/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardJobStatus;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DetectionOnboardResourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardResourceTest.class);
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionOnboardResource detectionOnboardResource;
  private DAOTestBase daoTestBase;

  @BeforeClass
  public void initResource() {
    daoTestBase = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionOnboardResource = new DetectionOnboardResource(daoRegistry.getTaskDAO(), daoRegistry.getAnomalyFunctionDAO());
  }

  @AfterClass
  public void shutdownResource() {
    daoTestBase.cleanup();
  }

  @Test
  public void testCreateJob() throws Exception {
    Map<String, String> properties = OnboardingTaskTestUtils.getJobProperties();

    String propertiesJson = OBJECT_MAPPER.writeValueAsString(properties);
    String normalJobStatusJson = detectionOnboardResource.createDetectionOnboardingJob("NormalJob", propertiesJson);

    DetectionOnboardJobStatus onboardJobStatus =
        OBJECT_MAPPER.readValue(normalJobStatusJson, DetectionOnboardJobStatus.class);
    JobConstants.JobStatus jobStatus = onboardJobStatus.getJobStatus();
    Assert.assertTrue(
        JobConstants.JobStatus.COMPLETED.equals(jobStatus) || JobConstants.JobStatus.SCHEDULED.equals(jobStatus));
    long jobId = onboardJobStatus.getJobId();

    DetectionOnboardJobStatus onboardJobStatusGet =
        OBJECT_MAPPER.readValue(detectionOnboardResource.getDetectionOnboardingJobStatus(jobId), DetectionOnboardJobStatus.class);
    JobConstants.JobStatus jobStatusGet = onboardJobStatusGet.getJobStatus();
    LOG.info("Job Status: {}", jobStatusGet);
    Assert.assertTrue(
        JobConstants.JobStatus.COMPLETED.equals(jobStatusGet) || JobConstants.JobStatus.SCHEDULED.equals(jobStatusGet));
  }

  @Test(dependsOnMethods = "testCreateJob")
  public void testFailedJobCreation() throws Exception {
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
    Assert.assertEquals(onboardJobStatus.getJobStatus(), JobConstants.JobStatus.UNKNOWN);
    Assert.assertNotNull(onboardJobStatus.getMessage());
  }

}
