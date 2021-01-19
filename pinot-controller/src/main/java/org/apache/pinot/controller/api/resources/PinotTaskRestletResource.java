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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.minion.PinotTaskConfig;
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

  @GET
  @Path("/tasks/scheduler/information")
  @ApiOperation("Fetch cron scheduler information")
  public Map<String, Object> getCronSchedulerInformation()
      throws SchedulerException {
    Scheduler scheduler = _pinotTaskManager.getScheduler();
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
  @ApiOperation("Fetch cron scheduler job keys")
  public List<JobKey> getCronSchedulerJobKeys()
      throws SchedulerException {
    List<JobKey> jobKeys = new ArrayList<>();
    Scheduler scheduler = _pinotTaskManager.getScheduler();
    for (String group : scheduler.getTriggerGroupNames()) {
      jobKeys.addAll(scheduler.getJobKeys(GroupMatcher.groupEquals(group)));
    }
    return jobKeys;
  }

  @GET
  @Path("/tasks/scheduler/jobDetails")
  @ApiOperation("Fetch cron scheduler job keys")
  public Map<String, Object> getCronSchedulerJobDetails(
      @ApiParam(value = "Table name (with type suffix)") @QueryParam("tableName") String tableName,
      @ApiParam(value = "Task type") @QueryParam("taskType") String taskType)
      throws SchedulerException {
    Scheduler scheduler = _pinotTaskManager.getScheduler();
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
  @ApiOperation("Schedule tasks and return a map from task type to task name scheduled")
  public Map<String, String> scheduleTasks(@ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "Table name (with type suffix)") @QueryParam("tableName") String tableName) {
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
  public Map<String, String> scheduleTasksDeprecated() {
    return _pinotTaskManager.scheduleTasks();
  }

  @PUT
  @Path("/tasks/{taskType}/cleanup")
  @ApiOperation("Clean up finished tasks (COMPLETED, FAILED) for the given task type")
  public SuccessResponse cleanUpTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
    return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
  }

  @Deprecated
  @PUT
  @Path("/tasks/cleanuptasks/{taskType}")
  @ApiOperation("Clean up finished tasks (COMPLETED, FAILED) for the given task type (deprecated)")
  public SuccessResponse cleanUpTasksDeprecated(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.cleanUpTaskQueue(taskType);
    return new SuccessResponse("Successfully cleaned up tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/stop")
  @ApiOperation("Stop all running/pending tasks (as well as the task queue) for the given task type")
  public SuccessResponse stopTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.stopTaskQueue(taskType);
    return new SuccessResponse("Successfully stopped tasks for task type: " + taskType);
  }

  @PUT
  @Path("/tasks/{taskType}/resume")
  @ApiOperation("Resume all stopped tasks (as well as the task queue) for the given task type")
  public SuccessResponse resumeTasks(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    _pinotHelixTaskResourceManager.resumeTaskQueue(taskType);
    return new SuccessResponse("Successfully resumed tasks for task type: " + taskType);
  }

  @Deprecated
  @PUT
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Stop/resume a task queue (deprecated)")
  public SuccessResponse toggleTaskQueueState(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "state", required = true) @QueryParam("state") String state) {
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
      @ApiParam(value = "Whether to force deleting the tasks (expert only option, enable with cautious") @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete) {
    _pinotHelixTaskResourceManager.deleteTaskQueue(taskType, forceDelete);
    return new SuccessResponse("Successfully deleted tasks for task type: " + taskType);
  }

  @Deprecated
  @DELETE
  @Path("/tasks/taskqueue/{taskType}")
  @ApiOperation("Delete a task queue (deprecated)")
  public SuccessResponse deleteTaskQueue(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType,
      @ApiParam(value = "Whether to force delete the task queue (expert only option, enable with cautious") @DefaultValue("false") @QueryParam("forceDelete") boolean forceDelete) {
    _pinotHelixTaskResourceManager.deleteTaskQueue(taskType, forceDelete);
    return new SuccessResponse("Successfully deleted task queue for task type: " + taskType);
  }
}
