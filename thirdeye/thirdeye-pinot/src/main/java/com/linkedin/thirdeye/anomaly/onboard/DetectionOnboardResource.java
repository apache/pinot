package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import java.io.IOException;
import java.util.HashMap;
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

@Path("/detection-onboard")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionOnboardResource {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionOnboardService detectionOnboardService;

  public DetectionOnboardResource(DetectionOnboardService detectionOnboardService) {
    Preconditions.checkNotNull(detectionOnboardService);
    this.detectionOnboardService = detectionOnboardService;
  }

  /**
   * Create a job with the given name and properties.
   *
   * @param jobName     the unique name for the job.
   * @param jsonPayload a map of properties in JSON string.
   *
   * @return Job status in JSON string.
   */
  @POST
  @Path("/create-job")
  public String createDetectionOnboardingJob(@NotNull @QueryParam("jobName") String jobName,
      @QueryParam("payload") String jsonPayload) {

    // Check user's input
    if (jsonPayload == null) {
      jsonPayload = "";
    }

    // Invoke backend function
    DetectionOnboardJobStatus detectionOnboardingJobStatus;
    try {
      Map<String, String> properties = OBJECT_MAPPER.readValue(jsonPayload, HashMap.class);
      // TODO: Dynamically create different type of Detection Onboard Job?
      long jobId =
          detectionOnboardService.createDetectionOnboardingJob(new DefaultDetectionOnboardJob(jobName), properties);
      detectionOnboardingJobStatus = detectionOnboardService.getDetectionOnboardingJobStatus(jobId);
    } catch (Exception e) {
      detectionOnboardingJobStatus = new DetectionOnboardJobStatus();
      detectionOnboardingJobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
      detectionOnboardingJobStatus
          .setMessage(String.format("Failed to create job %s. %s", jobName, ExceptionUtils.getStackTrace(e)));
    }

    return detectionOnboardJobStatusToJsonString(detectionOnboardingJobStatus);
  }

  /**
   * Returns the job status in JSON string.
   *
   * @param jobId the id of the job.
   *
   * @return the job status in JSON string.
   */
  @GET
  @Path("/get-status")
  public String getDetectionOnboardingJobStatus(@QueryParam("jobId") long jobId) {
    DetectionOnboardJobStatus detectionOnboardingJobStatus =
        detectionOnboardService.getDetectionOnboardingJobStatus(jobId);
    // Create StatusNotFound message
    if (detectionOnboardingJobStatus == null) {
      detectionOnboardingJobStatus = new DetectionOnboardJobStatus();
      detectionOnboardingJobStatus.setJobId(jobId);
      detectionOnboardingJobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
      detectionOnboardingJobStatus.setMessage(String.format("Unable to find job id: %d", jobId));
    }
    return detectionOnboardJobStatusToJsonString(detectionOnboardingJobStatus);
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
