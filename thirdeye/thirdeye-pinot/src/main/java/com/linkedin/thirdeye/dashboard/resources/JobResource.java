package com.linkedin.thirdeye.dashboard.resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.util.JsonResponseUtil;

@Path(value = "/thirdeye-admin/jobs")
@Produces(MediaType.APPLICATION_JSON)
public class JobResource {

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private JobManager jobDao;

  public JobResource() {
    this.jobDao = DAO_REGISTRY.getJobDAO();
  }

  @GET
  @Path("/listRunningJobs")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewRunningJobs(@DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("10") @QueryParam("jtPageSize") int jtPageSize) {
    List<JobDTO> jobDTOs = jobDao.findByStatus(JobStatus.SCHEDULED);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(jobDTOs);
    return rootNode.toString();
  }
  
  @GET
  @Path("/listJobsForDataset")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewJobsForDataset(@NotNull @QueryParam("dataset") String dataset, @DefaultValue("0") @QueryParam("jtStartIndex") int jtStartIndex,
      @DefaultValue("10") @QueryParam("jtPageSize") int jtPageSize) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("dataset", dataset);
    List<JobDTO> jobDTOs = jobDao.findByParams(filters);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(jobDTOs);
    return rootNode.toString();
  }

  @GET
  @Path("/listTasksForJob")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewJobTasks(@NotNull @QueryParam("dataset") String dataset) {
    Map<String, Object> filters = new HashMap<>();
    filters.put("dataset", dataset);
    List<JobDTO> jobDTOs = jobDao.findByParams(filters);
    ObjectNode rootNode = JsonResponseUtil.buildResponseJSON(jobDTOs);
    return rootNode.toString();
  }
}
