package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTaskInfo;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.core.periodictask.TaskExecutionResult;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.api.resources.Constants.*;


/**
 * @author Harish Shankar 
 */
@Api(tags = Constants.TRIGGER_TAG)
@Path("/")
public class PinotControllerTaskRestletResource {
  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  PeriodicTaskScheduler _periodicTaskScheduler;

  @GET
  @Path("/trigger/tasknames")
  @ApiOperation("List all available controller periodic tasks")
  public List<PeriodicTaskInfo> listTaskTypes() {
    List<PeriodicTaskInfo> taskList = _periodicTaskScheduler.getRegisteredTasks();
    return taskList;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/triggers/execute")
  @ApiOperation("Schedule task and return a task execution status to the user")
  public TaskExecutionResult execute(@ApiParam(value = "Task name") @QueryParam("taskName") String taskType) {
    try {
      return _periodicTaskScheduler.execute(TASK_SEGMENT_RELOCATOR);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to execute task",
          Response.Status.BAD_REQUEST);
    }
  }
}
