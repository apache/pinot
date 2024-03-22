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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.HashBiMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.minion.BaseTaskGeneratorInfo;
import org.apache.pinot.common.minion.TaskManagerStatusCache;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.NoTaskScheduledException;
import org.apache.pinot.controller.api.exception.TaskAlreadyExistsException;
import org.apache.pinot.controller.api.exception.UnknownTaskTypeException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.server.ManagedAsync;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


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
 *   <li>POST '/tasks/execute': Execute an adhoc task</li>
 *   <li>PUT '/tasks/{taskType}/cleanup': Clean up finished tasks (COMPLETED, FAILED) for the given task type</li>
 *   <li>PUT '/tasks/{taskType}/stop': Stop all running/pending tasks (as well as the task queue) for the given task
 *   type</li>
 *   <li>PUT '/tasks/{taskType}/resume': Resume all stopped tasks (as well as the task queue) for the given task
 *   type</li>
 *   <li>DELETE '/tasks/{taskType}': Delete all tasks (as well as the task queue) for the given task type</li>
 * </ul>
 */
@Api(tags = Constants.TASK_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotTaskRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskRestletResource.class);

  private static final String TASK_QUEUE_STATE_STOP = "STOP";
  private static final String TASK_QUEUE_STATE_RESUME = "RESUME";

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  TaskManagerStatusCache _taskManagerStatusCache;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  Executor _executor;

  @Inject
  HttpClientConnectionManager _connectionManager;

  @Inject
  ControllerConf _controllerConf;

  @Context
  private UriInfo _uriInfo;

  @GET
  @Path("/tasks/tasktypes")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all task types")
  public Set<String> listTaskTypes() {
    return _pinotHelixTaskResourceManager.getTaskTypes();
  }

  @GET
  @Path("/tasks/{taskType}/state")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the state (task queue state) for the given task type")
  public TaskState getTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskQueueState(taskType);
  }

  @GET
  @Path("/tasks/{taskType}/tasks")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all tasks for the given task type")
  public Set<String> getTasks(@ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTasks(taskType);
  }

  @GET
  @Path("/tasks/{taskType}/{tableNameWithType}/state")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all tasks for the given task type")
  public Map<String, TaskState> getTaskStatesByTable(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
      String tableNameWithType, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    return _pinotHelixTaskResourceManager.getTaskStatesByTable(taskType, tableNameWithType);
  }

  @GET
  @Path("/tasks/{taskType}/{tableNameWithType}/metadata")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get task metadata for the given task type and table")
  public String getTaskMetadataByTable(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
      String tableNameWithType, @Context HttpHeaders headers) {
    try {
      tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
      return _pinotHelixTaskResourceManager.getTaskMetadataByTable(taskType, tableNameWithType);
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER, String
          .format("Failed to format task metadata into Json for task type: %s from table: %s", taskType,
              tableNameWithType), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Path("/tasks/{taskType}/{tableNameWithType}/metadata")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Delete task metadata for the given task type and table")
  public SuccessResponse deleteTaskMetadataByTable(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
      String tableNameWithType, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    _pinotHelixTaskResourceManager.deleteTaskMetadataByTable(taskType, tableNameWithType);
    return new SuccessResponse(
        String.format("Successfully deleted metadata for task type: %s from table: %s", taskType, tableNameWithType));
  }

  @GET
  @Path("/tasks/{taskType}/taskcounts")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch count of sub-tasks for each of the tasks for the given task type")
  public Map<String, PinotHelixTaskResourceManager.TaskCount> getTaskCounts(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskCounts(taskType);
  }

  @GET
  @Path("/tasks/{taskType}/debug")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DEBUG_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch information for all the tasks for the given task type")
  public Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> getTasksDebugInfo(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "verbosity (Prints information for all the tasks for the given task type."
          + "By default, only prints subtask details for running and error tasks. "
          + "Value of > 0 prints subtask details for all tasks)")
      @DefaultValue("0") @QueryParam("verbosity") int verbosity) {
    return _pinotHelixTaskResourceManager.getTasksDebugInfo(taskType, verbosity);
  }

  @GET
  @Path("/tasks/{taskType}/{tableNameWithType}/debug")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DEBUG_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch information for all the tasks for the given task type and table")
  public Map<String, PinotHelixTaskResourceManager.TaskDebugInfo> getTasksDebugInfo(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "verbosity (Prints information for all the tasks for the given task type and table."
          + "By default, only prints subtask details for running and error tasks. "
          + "Value of > 0 prints subtask details for all tasks)")
      @DefaultValue("0") @QueryParam("verbosity") int verbosity, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    return _pinotHelixTaskResourceManager.getTasksDebugInfoByTable(taskType, tableNameWithType, verbosity);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tasks/generator/{tableNameWithType}/{taskType}/debug")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @ApiOperation("Fetch task generation information for the recent runs of the given task for the given table")
  public String getTaskGenerationDebugInto(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Whether to only lookup local cache for logs", defaultValue = "false") @QueryParam("localOnly")
          boolean localOnly, @Context HttpHeaders httpHeaders)
      throws JsonProcessingException {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, httpHeaders);
    if (localOnly) {
      BaseTaskGeneratorInfo taskGeneratorMostRecentRunInfo =
          _taskManagerStatusCache.fetchTaskGeneratorInfo(tableNameWithType, taskType);
      if (taskGeneratorMostRecentRunInfo == null) {
        throw new ControllerApplicationException(LOGGER, "Task generation information not found",
            Response.Status.NOT_FOUND);
      }

      return JsonUtils.objectToString(taskGeneratorMostRecentRunInfo);
    }

    // Call all controllers
    List<InstanceConfig> controllers = _pinotHelixResourceManager.getAllControllerInstanceConfigs();
    // Relying on original schema that was used to query the controller
    URI uri = _uriInfo.getRequestUri();
    String scheme = uri.getScheme();
    String finalTableNameWithType = tableNameWithType;
    List<String> controllerUrls = controllers.stream().map(controller -> String
        .format("%s://%s:%d/tasks/generator/%s/%s/debug?localOnly=true", scheme, controller.getHostName(),
            Integer.parseInt(controller.getPort()), finalTableNameWithType, taskType)).collect(Collectors.toList());

    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, HashBiMap.create(0));
    Map<String, String> requestHeaders = new HashMap<>();
    httpHeaders.getRequestHeaders().keySet().forEach(header -> {
      requestHeaders.put(header, httpHeaders.getHeaderString(header));
    });
    LOGGER.debug("Getting task generation info with controllerUrls: {}", controllerUrls);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(controllerUrls, null, true, requestHeaders, 10000);

    List<JsonNode> result = new ArrayList<>();
    serviceResponse._httpResponses.values().forEach(resp -> {
      try {
        result.add(JsonUtils.stringToJsonNode(resp));
      } catch (IOException e) {
        LOGGER.error("Failed to parse controller response {}", resp, e);
      }
    });

    return JsonUtils.objectToString(result);
  }

  @GET
  @Path("/tasks/task/{taskName}/debug")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DEBUG_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch information for the given task name")
  public PinotHelixTaskResourceManager.TaskDebugInfo getTaskDebugInfo(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName,
      @ApiParam(value = "verbosity (Prints information for the given task name."
          + "By default, only prints subtask details for running and error tasks. "
          + "Value of > 0 prints subtask details for all tasks)")
      @DefaultValue("0") @QueryParam("verbosity") int verbosity) {
    return _pinotHelixTaskResourceManager.getTaskDebugInfo(taskName, verbosity);
  }

  @GET
  @Path("/tasks/{taskType}/taskstates")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get a map from task to task state for the given task type")
  public Map<String, TaskState> getTaskStates(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    return _pinotHelixTaskResourceManager.getTaskStates(taskType);
  }

  @GET
  @Path("/tasks/task/{taskName}/state")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the task state for the given task")
  public TaskState getTaskState(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getTaskState(taskName);
  }

  @GET
  @Path("/tasks/subtask/{taskName}/state")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the states of all the sub tasks for the given task")
  public Map<String, TaskPartitionState> getSubtaskStates(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getSubtaskStates(taskName);
  }

  @GET
  @Path("/tasks/task/{taskName}/config")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the task config (a list of child task configs) for the given task")
  public List<PinotTaskConfig> getTaskConfigs(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getSubtaskConfigs(taskName);
  }

  @GET
  @Path("/tasks/task/{taskName}/runtime/config")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the task runtime config for the given task")
  public Map<String, String> getTaskConfig(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    return _pinotHelixTaskResourceManager.getTaskRuntimeConfig(taskName);
  }

  @GET
  @Path("/tasks/subtask/{taskName}/config")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the configs of specified sub tasks for the given task")
  public Map<String, PinotTaskConfig> getSubtaskConfigs(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName,
      @ApiParam(value = "Sub task names separated by comma") @QueryParam("subtaskNames") @Nullable
          String subtaskNames) {
    return _pinotHelixTaskResourceManager.getSubtaskConfigs(taskName, subtaskNames);
  }

  @GET
  @Path("/tasks/subtask/{taskName}/progress")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get progress of specified sub tasks for the given task tracked by minion worker in memory")
  public String getSubtaskProgress(@Context HttpHeaders httpHeaders,
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName,
      @ApiParam(value = "Sub task names separated by comma") @QueryParam("subtaskNames") @Nullable
          String subtaskNames) {
    // Relying on original schema that was used to query the controller
    String scheme = _uriInfo.getRequestUri().getScheme();
    List<InstanceConfig> workers = _pinotHelixResourceManager.getAllMinionInstanceConfigs();
    Map<String, String> workerEndpoints = new HashMap<>();
    for (InstanceConfig worker : workers) {
      workerEndpoints.put(worker.getId(),
          String.format("%s://%s:%d", scheme, worker.getHostName(), Integer.parseInt(worker.getPort())));
    }
    Map<String, String> requestHeaders = new HashMap<>();
    httpHeaders.getRequestHeaders().keySet().forEach(header -> {
      requestHeaders.put(header, httpHeaders.getHeaderString(header));
    });
    int timeoutMs = _controllerConf.getMinionAdminRequestTimeoutSeconds() * 1000;
    try {
      Map<String, Object> progress = _pinotHelixTaskResourceManager
          .getSubtaskProgress(taskName, subtaskNames, _executor, _connectionManager, workerEndpoints, requestHeaders,
              timeoutMs);
      return JsonUtils.objectToString(progress);
    } catch (UnknownTaskTypeException | NoTaskScheduledException e) {
      throw new ControllerApplicationException(LOGGER, "Not task with name: " + taskName, Response.Status.NOT_FOUND, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String
          .format("Failed to get worker side progress for task: %s due to error: %s", taskName,
              ExceptionUtils.getStackTrace(e)), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/tasks/subtask/workers/progress")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get progress of all subtasks with specified state tracked by minion worker in memory")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getSubtaskOnWorkerProgress(@Context HttpHeaders httpHeaders,
      @ApiParam(value = "Subtask state (UNKNOWN,IN_PROGRESS,SUCCEEDED,CANCELLED,ERROR)", required = true)
      @QueryParam("subTaskState") String subTaskState,
      @ApiParam(value = "Minion worker IDs separated by comma") @QueryParam("minionWorkerIds") @Nullable
          String minionWorkerIds) {
    Set<String> selectedMinionWorkers = new HashSet<>();
    if (StringUtils.isNotEmpty(minionWorkerIds)) {
      selectedMinionWorkers.addAll(
          Arrays.stream(StringUtils.split(minionWorkerIds, ',')).map(String::trim).collect(Collectors.toList()));
    }
    // Relying on original schema that was used to query the controller
    String scheme = _uriInfo.getRequestUri().getScheme();
    List<InstanceConfig> allMinionWorkerInstanceConfigs = _pinotHelixResourceManager.getAllMinionInstanceConfigs();
    Map<String, String> selectedMinionWorkerEndpoints = new HashMap<>();
    for (InstanceConfig worker : allMinionWorkerInstanceConfigs) {
      if (selectedMinionWorkers.isEmpty() || selectedMinionWorkers.contains(worker.getId())) {
        selectedMinionWorkerEndpoints.put(worker.getId(),
            String.format("%s://%s:%d", scheme, worker.getHostName(), Integer.parseInt(worker.getPort())));
      }
    }
    Map<String, String> requestHeaders = new HashMap<>();
    httpHeaders.getRequestHeaders().keySet().forEach(header ->
        requestHeaders.put(header, httpHeaders.getHeaderString(header)));
    int timeoutMs = _controllerConf.getMinionAdminRequestTimeoutSeconds() * 1000;
    try {
      Map<String, Object> minionWorkerIdSubtaskProgressMap =
          _pinotHelixTaskResourceManager.getSubtaskOnWorkerProgress(subTaskState, _executor, _connectionManager,
              selectedMinionWorkerEndpoints, requestHeaders, timeoutMs);
      return JsonUtils.objectToString(minionWorkerIdSubtaskProgressMap);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to get minion worker side progress for subtasks with state %s due to error: %s",
              subTaskState, ExceptionUtils.getStackTrace(e)), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/tasks/scheduler/information")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEDULER_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch cron scheduler information")
  public Map<String, Object> getCronSchedulerInformation()
      throws SchedulerException {
    Scheduler scheduler = _pinotTaskManager.getScheduler();
    if (scheduler == null) {
      throw new NotFoundException("Task scheduler is disabled");
    }
    SchedulerMetaData metaData = scheduler.getMetaData();
    Map<String, Object> schedulerMetaData = new HashMap<>();
    schedulerMetaData.put("Version", metaData.getVersion());
    schedulerMetaData.put("SchedulerName", metaData.getSchedulerName());
    schedulerMetaData.put("SchedulerInstanceId", metaData.getSchedulerInstanceId());
    schedulerMetaData.put("getThreadPoolClass", metaData.getThreadPoolClass());
    schedulerMetaData.put("getThreadPoolSize", metaData.getThreadPoolSize());
    schedulerMetaData.put("SchedulerClass", metaData.getSchedulerClass());
    schedulerMetaData.put("Clustered", metaData.isJobStoreClustered());
    schedulerMetaData.put("JobStoreClass", metaData.getJobStoreClass());
    schedulerMetaData.put("NumberOfJobsExecuted", metaData.getNumberOfJobsExecuted());
    schedulerMetaData.put("InStandbyMode", metaData.isInStandbyMode());
    schedulerMetaData.put("RunningSince", metaData.getRunningSince());
    List<Map> jobDetails = new ArrayList<>();
    for (String groupName : scheduler.getJobGroupNames()) {
      for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        Map<String, Object> jobMap = new HashMap<>();
        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
        jobMap.put("JobKey", jobKey);
        jobMap.put("NextFireTime", triggers.get(0).getNextFireTime());
        jobMap.put("PreviousFireTime", triggers.get(0).getPreviousFireTime());
        jobDetails.add(jobMap);
      }
    }
    schedulerMetaData.put("JobDetails", jobDetails);
    return schedulerMetaData;
  }

  @GET
  @Path("/tasks/scheduler/jobKeys")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEDULER_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch cron scheduler job keys")
  public List<JobKey> getCronSchedulerJobKeys()
      throws SchedulerException {
    Scheduler scheduler = _pinotTaskManager.getScheduler();
    if (scheduler == null) {
      throw new NotFoundException("Task scheduler is disabled");
    }
    List<JobKey> jobKeys = new ArrayList<>();
    for (String group : scheduler.getTriggerGroupNames()) {
      jobKeys.addAll(scheduler.getJobKeys(GroupMatcher.groupEquals(group)));
    }
    return jobKeys;
  }

  @GET
  @Path("/tasks/scheduler/jobDetails")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEDULER_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Fetch cron scheduler job keys")
  public Map<String, Object> getCronSchedulerJobDetails(
      @ApiParam(value = "Table name (with type suffix)") @QueryParam("tableName") String tableName,
      @ApiParam(value = "Task type") @QueryParam("taskType") String taskType, @Context HttpHeaders headers)
      throws SchedulerException {
    Scheduler scheduler = _pinotTaskManager.getScheduler();
    if (scheduler == null) {
      throw new NotFoundException("Task scheduler is disabled");
    }
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    JobKey jobKey = JobKey.jobKey(tableName, taskType);
    if (!scheduler.checkExists(jobKey)) {
      throw new NotFoundException(
          "Unable to find job detail for table name - " + tableName + ", task type - " + taskType);
    }
    JobDetail schedulerJobDetail = scheduler.getJobDetail(jobKey);
    Map<String, Object> jobDetail = new HashMap<>();
    jobDetail.put("JobKey", schedulerJobDetail.getKey());
    jobDetail.put("Description", schedulerJobDetail.getDescription());
    jobDetail.put("JobClass", schedulerJobDetail.getJobClass());
    JobDataMap jobData = schedulerJobDetail.getJobDataMap();
    Map<String, String> jobDataMap = new HashMap<>();
    for (String key : jobData.getKeys()) {
      jobDataMap.put(key, jobData.get(key).toString());
    }
    jobDetail.put("JobDataMap", jobDataMap);
    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
    List<Map> triggerMaps = new ArrayList<>();
    if (!triggers.isEmpty()) {
      for (Trigger trigger : triggers) {
        Map<String, Object> triggerMap = new HashMap<>();
        if (trigger instanceof SimpleTrigger) {
          SimpleTrigger simpleTrigger = (SimpleTrigger) trigger;
          triggerMap.put("TriggerType", SimpleTrigger.class.getSimpleName());
          triggerMap.put("RepeatInterval", simpleTrigger.getRepeatInterval());
          triggerMap.put("RepeatCount", simpleTrigger.getRepeatCount());
          triggerMap.put("TimesTriggered", simpleTrigger.getTimesTriggered());
          triggerMap.put("NextFireTime", simpleTrigger.getNextFireTime());
          triggerMap.put("PreviousFireTime", simpleTrigger.getPreviousFireTime());
        } else if (trigger instanceof CronTrigger) {
          CronTrigger cronTrigger = (CronTrigger) trigger;
          triggerMap.put("TriggerType", CronTrigger.class.getSimpleName());
          triggerMap.put("TimeZone", cronTrigger.getTimeZone());
          triggerMap.put("CronExpression", cronTrigger.getCronExpression());
          triggerMap.put("ExpressionSummary", cronTrigger.getExpressionSummary());
          triggerMap.put("NextFireTime", cronTrigger.getNextFireTime());
          triggerMap.put("PreviousFireTime", cronTrigger.getPreviousFireTime());
        }
        triggerMaps.add(triggerMap);
      }
    }
    jobDetail.put("Triggers", triggerMaps);
    return jobDetail;
  }

  @POST
  @Path("/tasks/schedule")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CREATE_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation("Schedule tasks and return a map from task type to task name scheduled")
  public Map<String, String> scheduleTasks(@ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "Table name (with type suffix)") @QueryParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tasks/execute")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.EXECUTE_TASK)
  @Authenticate(AccessType.CREATE)
  @ApiOperation("Execute a task on minion")
  public void executeAdhocTask(AdhocTaskConfig adhocTaskConfig, @Suspended AsyncResponse asyncResponse,
      @Context Request requestContext) {
    try {
      asyncResponse.resume(_pinotTaskManager.createTask(adhocTaskConfig.getTaskType(), adhocTaskConfig.getTableName(),
          adhocTaskConfig.getTaskName(), adhocTaskConfig.getTaskConfigs()));
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table: " + adhocTaskConfig.getTableName(),
          Response.Status.NOT_FOUND, e);
    } catch (TaskAlreadyExistsException e) {
      throw new ControllerApplicationException(LOGGER, "Task already exists: " + adhocTaskConfig.getTaskName(),
          Response.Status.CONFLICT, e);
    } catch (UnknownTaskTypeException e) {
      throw new ControllerApplicationException(LOGGER, "Unknown task type: " + adhocTaskConfig.getTaskType(),
          Response.Status.NOT_FOUND, e);
    } catch (NoTaskScheduledException e) {
      throw new ControllerApplicationException(LOGGER,
          "No task is generated for table: " + adhocTaskConfig.getTableName() + ", with task type: "
              + adhocTaskConfig.getTaskType(), Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to create adhoc task: " + ExceptionUtils.getStackTrace(e), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Path("/tasks/{taskType}/cleanup")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CLEANUP_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation("Clean up finished tasks (COMPLETED, FAILED) for the given task type")
  public SuccessResponse cleanUpTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
    return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/stop")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.STOP_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation("Stop all running/pending tasks (as well as the task queue) for the given task type")
  public SuccessResponse stopTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.stopTaskQueue(taskType);
    return new SuccessResponse("Successfully stopped tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/resume")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RESUME_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation("Resume all stopped tasks (as well as the task queue) for the given task type")
  public SuccessResponse resumeTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.resumeTaskQueue(taskType);
    return new SuccessResponse("Successfully resumed tasks for task type: " + taskType);
  }

  @DELETE
  @Path("/tasks/{taskType}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.DELETE)
  @ApiOperation("Delete all tasks (as well as the task queue) for the given task type")
  public SuccessResponse deleteTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Whether to force deleting the tasks (expert only option, enable with cautious")
      @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete) {
    _pinotHelixTaskResourceManager.deleteTaskQueue(taskType, forceDelete);
    return new SuccessResponse("Successfully deleted tasks for task type: " + taskType);
  }

  @DELETE
  @Path("/tasks/task/{taskName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.DELETE)
  @ApiOperation("Delete a single task given its task name")
  public SuccessResponse deleteTask(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName,
      @ApiParam(value = "Whether to force deleting the task (expert only option, enable with cautious")
      @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete) {
    _pinotHelixTaskResourceManager.deleteTask(taskName, forceDelete);
    return new SuccessResponse("Successfully deleted task: " + taskName);
  }
}
