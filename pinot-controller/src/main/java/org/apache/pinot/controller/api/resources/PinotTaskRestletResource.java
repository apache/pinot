/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task related rest APIs.
 * <ul>
 *   <li>GET '/tasks/tasktypes': List all task types</li>
 *   <li>GET '/tasks/{taskType}/state': Get the state (task queue state) for the given task type</li>
 *   <li>GET '/tasks/{taskType}/tasks': List all tasks for the given task type</li>
 *   <li>GET '/tasks/{taskType}/taskstates': Get a map from task to task state for the given task type</li>
 *   <li>GET '/tasks/task/{taskName}/state': Get the task state for the given task</li>
 *   <li>GET '/tasks/task/{taskName}/config': Get the task config (a list of child task configs) for the given task</li>
 *   <li>POST '/tasks/schedule': Schedule tasks</li>
 *   <li>PUT '/tasks/{taskType}/cleanup': Clean up finished tasks (COMPLETED, FAILED) for the given task type</li>
 *   <li>PUT '/tasks/{taskType}/stop': Stop all running/pending tasks (as well as the task queue) for the given task type</li>
 *   <li>PUT '/tasks/{taskType}/resume': Resume all stopped tasks (as well as the task queue) for the given task type</li>
 *   <li>DELETE '/tasks/{taskType}': Delete all tasks (as well as the task queue) for the given task type</li>
 * </ul>
 */
@Api(tags = Constants.TASK_TAG)
@Path("/")
public class PinotTaskRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskRestletResource.class);
  private static final String TASK_QUEUE_STATE_STOP = "STOP";
  private static final String TASK_QUEUE_STATE_RESUME = "RESUME";

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @GET
  @Path("/tasks/tasktypes")
  @ApiOperation("List all task types")
  public Set<String> listTaskTypes() {
    return _pinotHelixTaskResourceManager.getTaskTypes();
  }

  @Deprecated
  @GET
  @Path("/tasks/taskqueues")
  @ApiOperation("List all task queues (deprecated)")
  public Set<String> getTaskQueues() {
    return _pinotHelixTaskResourceManager.getTaskQueues();
  }

  @GET
  @Path("/tasks/{taskType}/state")
  @ApiOperation("Get the state (task queue state) for the given task type")
  public TaskState getTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskQueueState(taskType);
  }

  @Deprecated
  @GET
  @Path("/tasks/taskqueuestate/{taskType}")
  @ApiOperation("Get the state (task queue state) for the given task type (deprecated)")
  public StringResultResponse getTaskQueueStateDeprecated(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return new StringResultResponse(_pinotHelixTaskResourceManager.getTaskQueueState(taskType).toString());
  }

  @GET
  @Path("/tasks/{taskType}/tasks")
  @ApiOperation("List all tasks for the given task type")
  public Set<String> getTasks(@ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTasks(taskType);
  }

  @Deprecated
  @GET
  @Path("/tasks/tasks/{taskType}")
  @ApiOperation("List all tasks for the given task type (deprecated)")
  public Set<String> getTasksDeprecated(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTasks(taskType);
  }

  @GET
  @Path("/tasks/{taskType}/taskstates")
  @ApiOperation("Get a map from task to task state for the given task type")
  public Map<String, TaskState> getTaskStates(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  @Deprecated
  @GET
  @Path("/tasks/taskstates/{taskType}")
  @ApiOperation("Get a map from task to task state for the given task type (deprecated)")
  public Map<String, TaskState> getTaskStatesDeprecated(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  @GET
  @Path("/tasks/task/{taskName}/state")
  @ApiOperation("Get the task state for the given task")
  public TaskState getTaskState(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getTaskState(taskName);
  }

  @Deprecated
  @GET
  @Path("/tasks/taskstate/{taskName}")
  @ApiOperation("Get the task state for the given task (deprecated)")
  public StringResultResponse getTaskStateDeprecated(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return new StringResultResponse(_pinotHelixTaskResourceManager.getTaskState(taskName).toString());
  }

  @GET
  @Path("/tasks/task/{taskName}/config")
  @ApiOperation("Get the task config (a list of child task configs) for the given task")
  public List<PinotTaskConfig> getTaskConfigs(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getTaskConfigs(taskName);
  }

  @Deprecated
  @GET
  @Path("/tasks/taskconfig/{taskName}")
  @ApiOperation("Get the task config (a list of child task configs) for the given task (deprecated)")
  public List<PinotTaskConfig> getTaskConfigsDeprecated(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getTaskConfigs(taskName);
  }

  @POST
  @Path("/tasks/schedule")
  @ApiOperation("Schedule tasks and return a map from task type to task name scheduled")
  public Map<String, String> scheduleTasks(@ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "Table name (with type suffix)") @QueryParam("tableName") String tableName,
      @Context HttpHeaders httpHeaders) {
    if (tableName != null) {
      AccessControlUtils.validateWritePermission(httpHeaders, tableName, _accessControlFactory, LOGGER);
    } else {
      AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    }
    if (taskType != null) {
      // Schedule task for the given task type
      String taskName = tableName != null ? _pinotTaskManager.scheduleTask(taskType, tableName)
          : _pinotTaskManager.scheduleTask(taskType);
      return Collections.singletonMap(taskType, taskName);
    } else {
      // Schedule tasks for all task types
      return tableName != null ? _pinotTaskManager.scheduleTasks(tableName) : _pinotTaskManager.scheduleTasks();
    }
  }

  @Deprecated
  @PUT
  @Path("/tasks/scheduletasks")
  @ApiOperation("Schedule tasks (deprecated)")
  public Map<String, String> scheduleTasksDeprecated(@Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    return _pinotTaskManager.scheduleTasks();
  }

  @PUT
  @Path("/tasks/{taskType}/cleanup")
  @ApiOperation("Clean up finished tasks (COMPLETED, FAILED) for the given task type")
  public SuccessResponse cleanUpTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
    return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
  }

  @Deprecated
  @PUT
  @Path("/tasks/cleanuptasks/{taskType}")
  @ApiOperation("Clean up finished tasks (COMPLETED, FAILED) for the given task type (deprecated)")
  public SuccessResponse cleanUpTasksDeprecated(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
    return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/stop")
  @ApiOperation("Stop all running/pending tasks (as well as the task queue) for the given task type")
  public SuccessResponse stopTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.stopTaskQueue(taskType);
    return new SuccessResponse("Successfully stopped tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/resume")
  @ApiOperation("Resume all stopped tasks (as well as the task queue) for the given task type")
  public SuccessResponse resumeTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.resumeTaskQueue(taskType);
    return new SuccessResponse("Successfully resumed tasks for task type: " + taskType);
  }

  @Deprecated
  @PUT
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Stop/resume a task queue (deprecated)")
  public SuccessResponse toggleTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "state", required = true) @QueryParam("state") String state,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
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
  }

  @DELETE
  @Path("/tasks/{taskType}")
  @ApiOperation("Delete all tasks (as well as the task queue) for the given task type")
  public SuccessResponse deleteTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Whether to force deleting the tasks (expert only option, enable with cautious") @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.deleteTaskQueue(taskType, forceDelete);
    return new SuccessResponse("Successfully deleted tasks for task type: " + taskType);
  }

  @Deprecated
  @DELETE
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Delete a task queue (deprecated)")
  public SuccessResponse deleteTaskQueue(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Whether to force delete the task queue (expert only option, enable with cautious") @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete,
      @Context HttpHeaders httpHeaders) {
    AccessControlUtils.validateWritePermission(httpHeaders, _accessControlFactory, LOGGER);
    _pinotHelixTaskResourceManager.deleteTaskQueue(taskType, forceDelete);
    return new SuccessResponse("Successfully deleted task queue for task type: " + taskType);
  }
}
