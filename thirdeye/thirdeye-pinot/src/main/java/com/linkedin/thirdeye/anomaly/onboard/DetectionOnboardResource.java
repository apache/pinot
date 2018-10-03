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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardJobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * NOTE: This resource was re-written for backwards-compatibility with existing UI code.
 */
@Path("/detection-onboard")
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.ONBOARD_TAG})
public class DetectionOnboardResource {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TaskManager taskDAO;
  private final AnomalyFunctionManager anomalyDAO;

  public DetectionOnboardResource(TaskManager taskDAO, AnomalyFunctionManager anomalyDAO) {
    this.taskDAO = taskDAO;
    this.anomalyDAO = anomalyDAO;
  }

  /**
   * Create a job with the given name and properties.
   *
   * @param jobName     the unique name for the job. Must be equal to anomaly function name.
   * @param jsonPayload a map of properties in JSON string.
   *
   * @return Job name
   */
  @POST
  @Path("/create-job")
  @ApiOperation("POST request to update an existing alert function with properties payload")
  public String createDetectionOnboardingJob(@ApiParam(required = true) @NotNull @QueryParam("jobName") String jobName,
      @ApiParam("jsonPayload") String jsonPayload) {

    // Check user input
    if (jsonPayload == null) {
      jsonPayload = "";
    }

    long anomalyFunctionId;
    Map<String, String> properties;

    try {
      properties = OBJECT_MAPPER.readValue(jsonPayload, Map.class);

      // create minimal anomaly function
      AnomalyFunctionDTO function = new AnomalyFunctionDTO();
      function.setFunctionName(jobName);
      function.setMetricId(-1);
      function.setIsActive(false);
      anomalyFunctionId = anomalyDAO.save(function);

    } catch (Exception e) {
      LOG.error("Error creating anomaly function '{}'", jobName, e);
      return makeErrorStatus(-1, jobName, JobConstants.JobStatus.FAILED);
    }

    try {
      // launch minimal task (with job id == anomalyFunctionId)
      // NOTE: the existing task framework is an incredible hack

      ReplayTaskInfo taskInfo = new ReplayTaskInfo();
      taskInfo.setJobName(jobName);
      taskInfo.setProperties(properties);

      String taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);

      TaskDTO task = new TaskDTO();
      task.setTaskType(TaskConstants.TaskType.REPLAY);
      task.setJobName(jobName);
      task.setStatus(TaskConstants.TaskStatus.WAITING);
      task.setStartTime(System.currentTimeMillis());
      task.setTaskInfo(taskInfoJson);
      task.setJobId(anomalyFunctionId);
      this.taskDAO.save(task);

      return detectionOnboardJobStatusToJsonString(new DetectionOnboardJobStatus(anomalyFunctionId, jobName, JobConstants.JobStatus.SCHEDULED, ""));
    } catch (Exception e) {
      LOG.error("Error creating onboarding job '{}'", jobName, e);
      this.anomalyDAO.deleteById(anomalyFunctionId);
      return makeErrorStatus(-1, jobName, JobConstants.JobStatus.FAILED);
    }

  }

  /**
   * Returns the job status in JSON string.
   *
   * @param jobId the name of the job.
   *
   * @return the job status in JSON string.
   */
  @GET
  @Path("/get-status")
  @ApiOperation("GET request for job status (a sequence of events including create, replay, autotune)")
  public String getDetectionOnboardingJobStatus(@QueryParam("jobId") long jobId) {
    AnomalyFunctionDTO anomalyFunction = this.anomalyDAO.findById(jobId);
    if (anomalyFunction == null) {
      return makeErrorStatus(jobId, "Unknown Job", JobConstants.JobStatus.UNKNOWN);
    }

    DetectionOnboardJobStatus detectionOnboardingJobStatus = anomalyFunction.getOnboardJobStatus();
    if (detectionOnboardingJobStatus == null) {
      return makeErrorStatus(jobId, anomalyFunction.getFunctionName(), JobConstants.JobStatus.SCHEDULED);
    }

    return detectionOnboardJobStatusToJsonString(detectionOnboardingJobStatus);
  }

  /**
   * Helper. Returns serialized job status
   *
   * @param jobId job id
   * @param jobName job name
   * @param jobStatus job status
   * @return
   */
  private String makeErrorStatus(long jobId, String jobName, JobConstants.JobStatus jobStatus) {
    return detectionOnboardJobStatusToJsonString(
        new DetectionOnboardJobStatus(jobId, jobName, jobStatus, String.format("Job %d", jobId)));
  }

  /**
   * Converts job status to a JSON string or returns the plain text of any exception that is thrown during the
   * conversion.
   *
   * @param detectionOnboardingJobStatus the job status to be converted to a JSON string.
   *
   * @return the JSON string of the given job status.
   */
  private String detectionOnboardJobStatusToJsonString(DetectionOnboardJobStatus detectionOnboardingJobStatus) {
    try {
      return OBJECT_MAPPER.writeValueAsString(detectionOnboardingJobStatus);
    } catch (JsonProcessingException e) {
      LOG.error("Failed to convert job status to a json string.", e);
      return ExceptionUtils.getStackTrace(e);
    }
  }

}
