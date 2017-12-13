package com.linkedin.thirdeye.anomaly.onboard;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
    this.detectionOnboardService = detectionOnboardService;
  }

  @POST
  @Path("/create-job")
  public String createDetectionOnboardingJob(@NotNull @QueryParam("jobName") String jobName,
      @QueryParam("payload") String jsonPayload) {

    // Check user's input

    // Invoke backend function
    Map<String, String> properties = OBJECT_MAPPER.convertValue(jsonPayload, Map.class);
    // TODO: Dynamically create different type of Detection Onboard Job?
    DetectionOnboardJobStatus detectionOnboardingJob =
        detectionOnboardService.createDetectionOnboardingJob(new DefaultDetectionOnboardJob(jobName), properties);

    try {
      return OBJECT_MAPPER.writeValueAsString(detectionOnboardingJob);
    } catch (JsonProcessingException e) {
      LOG.error("Failed to convert job status to a json string.", e);
      return ExceptionUtils.getStackTrace(e);
    }
  }

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
    try {
      return OBJECT_MAPPER.writeValueAsString(detectionOnboardingJobStatus);
    } catch (JsonProcessingException e) {
      LOG.error("Failed to convert job status to a json string.", e);
      return ExceptionUtils.getStackTrace(e);
    }
  }
}
