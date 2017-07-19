package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/job-info")
@Produces(MediaType.APPLICATION_JSON)
public class JobResource {
  private static final Logger LOG = LoggerFactory.getLogger(JobResource.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private JobManager jobDao;
  private TaskManager taskDao;


  public JobResource() {
    this.jobDao = DAO_REGISTRY.getJobDAO();
    this.taskDao = DAO_REGISTRY.getTaskDAO();

  }

  @GET
  @Path("/listRecentJobs")
  @Produces(MediaType.APPLICATION_JSON)
  public String listRecentJobs(@DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("10") @QueryParam("jtPageSize") int jtPageSize) {
    List<JobDTO> jobDTOs = jobDao.findNRecentJobs(jtStartIndex + jtPageSize);
    List<JobDTO> subList = Utils.sublist(jobDTOs, jtStartIndex, jtPageSize);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(subList);
    return rootNode.toString();
  }

  @GET
  @Path("/listJobsForDataset")
  @Produces(MediaType.APPLICATION_JSON)
  public String listJobsForDataset(@NotNull @QueryParam("dataset") String dataset, @DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("10") @QueryParam("jtPageSize") int jtPageSize) {
//    Map<String, Object> filters = new HashMap<>();
//    filters.put("dataset", dataset);
//    List<JobDTO> jobDTOs = jobDao.findByParams(filters);
    //TODO: we don't have enough info to find jobs for a dataset, may be we should change this to functions?
    List<JobDTO> jobDTOs = Collections.emptyList();
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(jobDTOs);
    return rootNode.toString();
  }

  @GET
  @Path("/listTasksForJob")
  @Produces(MediaType.APPLICATION_JSON)
  public String listTasksForJob(@NotNull @QueryParam("jobId") long jobId) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("jobId", jobId);
    List<TaskDTO> taskDTOs = taskDao.findByParams(filters);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(taskDTOs);
    return rootNode.toString();
  }

  /**
   * Get the execution status of the given job id
   * @param id
   * @return the status of the job
   * COMPLETE if all tasks of the job are done without errors
   * FAILED if at least one of the tasks underneath is failed
   * SCHEDULED if at least one task underneath is still running or waiting
   */
  @GET
  @Path("/job/{id}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public JobStatus getJobStatus(@NotNull @PathParam("id") long id) {
    JobDTO jobDTO = jobDao.findById(id);
    List<TaskDTO> taskDTOs = taskDao.findByJobIdStatusNotIn(id, TaskStatus.COMPLETED);
    JobStatus jobStatus = jobDTO.getStatus();
    if (jobStatus.equals(JobStatus.FAILED) || jobStatus.equals(JobStatus.COMPLETED)) {
      // The final job status is reached. No change is made.
      return jobStatus;
    }
    JobStatus previousStatus = jobStatus;
    if (taskDTOs.size() > 0) {
      boolean containFails = false;
      for (TaskDTO task : taskDTOs) {
        if (task.getStatus().equals(TaskStatus.FAILED)) {
          containFails = true;
        }
      }
      if (containFails) {
        jobStatus = JobStatus.FAILED;
      } else {
        jobStatus = JobStatus.SCHEDULED;
      }
    } else {
      jobStatus = JobStatus.COMPLETED;
    }
    // If there is no change, no update is made.
    if (!previousStatus.equals(jobStatus)) {
      jobDTO.setStatus(jobStatus);
      jobDao.update(jobDTO);
    }
    return jobStatus;
  }
}
