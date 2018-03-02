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

}
