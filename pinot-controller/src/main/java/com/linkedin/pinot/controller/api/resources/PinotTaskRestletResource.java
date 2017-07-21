/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;


@Api(tags = "task")
public class PinotTaskRestletResource {
  private static final String TASK_QUEUE_STATE_STOP = "STOP";
  private static final String TASK_QUEUE_STATE_RESUME = "RESUME";

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  /**
   * URI Mappings:
   * <ul>
   *   <li>
   *     "/tasks/tasktypes":
   *     List all task types.
   *   </li>
   *   <li>
   *     "/tasks/tasks/{taskType}":
   *     List all tasks for the specified task type.
   *   </li>
   *   <li>
   *     "/tasks/taskconfig/{taskName}":
   *     Get the task config for the specified task name.
   *   </li>
   *   <li>
   *     "/tasks/taskstates/{taskType}":
   *     Get a map from task name to task state for the specified task type.
   *   </li>
   *   <li>
   *     "/tasks/taskstate/{taskName}":
   *     Get the task state for the specified task name.
   *   </li>
   *   <li>
   *     "/tasks/taskqueues":
   *     List all task queues.
   *   </li>
   *   <li>
   *     "/tasks/taskqueuestate/{taskType}":
   *     Get the task queue state for the specified task type.
   *   </li>
   * </ul>
   */

  @GET
  @Path("/tasks/tasktypes")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all task types", notes = "List all task types")
  public JSONArray listTaskTypes(

  ) {
    List<String> taskTypes = new ArrayList<>(_pinotHelixTaskResourceManager.getTaskTypes());
    Collections.sort(taskTypes);
    return new JSONArray(taskTypes);
  }

  @GET
  @ApiOperation( value = "List all tasks", notes = "List all tasks")
  @Path("/tasks/tasks/{taskType}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONArray getTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType
  ) {
    List<String> tasks = new ArrayList<>(_pinotHelixTaskResourceManager.getTasks(taskType));
    Collections.sort(tasks);
    return new JSONArray(tasks);
  }

  @GET
  @ApiOperation(value = "Get a task's configuration")
  @Path("/tasks/taskconfig/{taskName}")
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getTaskConfig(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName
  ) {
    try {
      PinotTaskConfig taskConfig = _pinotHelixTaskResourceManager.getTaskConfig(taskName);
      JSONObject result = new JSONObject();
      result.put("taskType", taskConfig.getTaskType());
      result.put("configs", new JSONObject(taskConfig.getConfigs()));
      return result;
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @ApiOperation(value = "Get all tasks' configuration", notes = "Get all tasks' configuration")
  @Path("/tasks/taskstates/{taskType}")
  public JSONObject getTasksConfiguration(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType
  ) {
    try {
      return new JSONObject(_pinotHelixTaskResourceManager.getTaskStates(taskType));
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @ApiOperation(value = "Get a task's state", notes = "Get a task's state")
  @Path("/tasks/taskstate/{taskName}")
  public String getTaskState(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName
  ) {
    try {
      return _pinotHelixTaskResourceManager.getTaskState(taskName).toString();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @ApiOperation(value = "List all task queues", notes = "List all task queues")
  @Path("/tasks/taskqueues")
  public JSONArray getTaskQueues(

  ) {
    try {
      List<String> taskQueues = new ArrayList<>(_pinotHelixTaskResourceManager.getTaskQueues());
      Collections.sort(taskQueues);
      return new JSONArray(taskQueues);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @ApiOperation(value = "Get a task queue's state", notes = "Get a task queue's state")
  @Path("/tasks/taskqueuestate/{taskType}")
  public String getTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType
  ) {
    try {
      return _pinotHelixTaskResourceManager.getTaskQueueState(taskType).toString();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * URI Mappings:
   * <ul>
   *   <li>
   *     "/tasks/taskqueue/{taskType}":
   *     Create a task queue for the specified task type.
   *   </li>
   *   <li>
   *     "/tasks/task/{taskType}":
   *     Submit a task of the specified task type to the task queue.
   *   </li>
   * </ul>
   */
  @POST
  @ApiOperation(value = "Create a task queue", notes = "Create a task queue")
  @Path("/tasks/taskqueue/{taskType}")
  public SuccessResponse createTaskQueue(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType
  ) {
    try {
      _pinotHelixTaskResourceManager.createTaskQueue(taskType);
      return new SuccessResponse("Successfully created task queue for task type: " + taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @ApiOperation(value = "Submit a task", notes = "Submit a task")
  @Path("/tasks/task/{taskType}")
  @Consumes(MediaType.APPLICATION_JSON)
  public SuccessResponse submitTask(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      String configMapStr
  ) {
    try {
      Map<String, String> configs = new HashMap<>();
      PinotTaskConfig pinotTaskConfig;
      if (configMapStr != null) {
        JSONObject jsonConfig = new JSONObject(configMapStr);
        Iterator iterator = jsonConfig.keys();
        while (iterator.hasNext()) {
          String key = (String) iterator.next();
          configs.put(key, jsonConfig.getString(key));
        }
      }
      pinotTaskConfig = new PinotTaskConfig(taskType, configs);
      String taskName = _pinotHelixTaskResourceManager.submitTask(pinotTaskConfig);
      return new SuccessResponse("Successfully submitted task: " + taskName);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * URI Mappings:
   * <ul>
   *   <li>
   *     "/tasks/taskqueue/{taskType}?state={state}":
   *     Stop/resume a task queue based on the specified {state} (stop|resume).
   *   </li>
   *   <li>
   *     "/tasks/scheduletasks":
   *     Schedule tasks.
   *   </li>
   * </ul>
   */

  @PUT
  @ApiOperation(value = "Stop/resume a task queue", notes = "Stop/resume a task queue")
  @Path("/tasks/taskqueue/{taskType}")
  public SuccessResponse toggleTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "state", required = true) @QueryParam("state") String state
  ) {
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

  @PUT
  @ApiOperation(value = "Schedule tasks", notes = "Schedule tasks")
  @Path("/tasks/scheduletasks")
  public SuccessResponse scheduleTasks(
  ) {
    try {
      _pinotTaskManager.scheduleTasks();
      return new SuccessResponse("Succeeded");
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }


  /**
   * URI Mappings:
   * <ul>
   *   <li>
   *     "/tasks/taskqueue/{taskType}":
   *     Delete a task queue for the specified task type.
   *   </li>
   * </ul>
   */

  @DELETE
  @ApiOperation(notes = "Delete a task queue", value = "Delete a task queue")
  @Path("/tasks/taskqueue/{taskType}")
  public SuccessResponse deleteTaskQueue(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType
  ) {
    try {
      _pinotHelixTaskResourceManager.deleteTaskQueue(taskType);
      return new SuccessResponse("Successfully deleted task queue for task type: " + taskType);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }
}
