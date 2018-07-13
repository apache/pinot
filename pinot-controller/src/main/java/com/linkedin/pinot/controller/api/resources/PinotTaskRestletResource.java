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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import org.apache.helix.task.TaskState;


@Api(tags = Constants.TASK_TAG)
@Path("/")
public class PinotTaskRestletResource {
  private static final String TASK_QUEUE_STATE_STOP = "STOP";
  private static final String TASK_QUEUE_STATE_RESUME = "RESUME";

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @GET
  @Path("/tasks/tasktypes")
  @ApiOperation("List all task types")
  public Set<String> listTaskTypes() {
    try {
      return _pinotHelixTaskResourceManager.getTaskTypes();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/tasks/{taskType}")
  @ApiOperation("List all tasks for the given task type")
  public Set<String> getTasks(@ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    try {
      return _pinotHelixTaskResourceManager.getTasks(taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/taskconfig/{taskName}")
  @ApiOperation("Get the child task configs for the given task name")
  public List<PinotTaskConfig> getTaskConfigs(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    try {
      return _pinotHelixTaskResourceManager.getTaskConfigs(taskName);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/taskstates/{taskType}")
  @ApiOperation("Get a map from task name to task state for the given task type")
  public Map<String, TaskState> getTaskStates(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    try {
      return _pinotHelixTaskResourceManager.getTaskStates(taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/taskstate/{taskName}")
  @ApiOperation("Get the task state for the given task name")
  public StringResultResponse getTaskState(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    try {
      return new StringResultResponse(_pinotHelixTaskResourceManager.getTaskState(taskName).toString());
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/taskqueues")
  @ApiOperation("List all task queues")
  public Set<String> getTaskQueues() {
    try {
      return _pinotHelixTaskResourceManager.getTaskQueues();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Path("/tasks/taskqueuestate/{taskType}")
  @ApiOperation("Get the task queue state for the given task type")
  public StringResultResponse getTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    try {
      return new StringResultResponse(_pinotHelixTaskResourceManager.getTaskQueueState(taskType).toString());
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("/tasks/scheduletasks")
  @ApiOperation("Schedule tasks")
  public Map<String, String> scheduleTasks() {
    try {
      return _pinotTaskManager.scheduleTasks();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("/tasks/cleanuptasks/{taskType}")
  @ApiOperation("Clean up tasks for the given task type")
  public SuccessResponse cleanUpTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    try {
      _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
      return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Stop/resume a task queue")
  public SuccessResponse toggleTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "state", required = true) @QueryParam("state") String state) {
    try {
      switch (state.toUpperCase()) {
        case TASK_QUEUE_STATE_STOP:
          _pinotHelixTaskResourceManager.stopTaskQueue(taskType);
          return new SuccessResponse("Successfully stopped task queue for task type: " + taskType);
        case TASK_QUEUE_STATE_RESUME:
          _pinotHelixTaskResourceManager.resumeTaskQueue(taskType);
          return new SuccessResponse("Successfully resumed task queue for task type: " + taskType);
        default:
          throw new IllegalArgumentException("Unsupported state: " + state);
      }
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Delete a task queue")
  public SuccessResponse deleteTaskQueue(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    try {
      _pinotHelixTaskResourceManager.deleteTaskQueue(taskType);
      return new SuccessResponse("Successfully deleted task queue for task type: " + taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }
}
