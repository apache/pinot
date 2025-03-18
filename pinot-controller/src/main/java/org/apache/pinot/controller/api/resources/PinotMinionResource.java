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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

/**
 * Endpoints for managing Pinot minions
 */
@Api(tags = Constants.TASK_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/minions")
public class PinotMinionResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotMinionResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @GET
  @Path("/instances")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_INSTANCE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all minion instances")
  public Set<String> getMinionInstances() {
    return _pinotHelixResourceManager.getAllInstancesForMinion();
  }

  @GET
  @Path("/instances/tagged")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_INSTANCE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all minion instances with a tag")
  public Map<String, Set<String>> getTaggedMinionInstances(
      @ApiParam(value = "Tag name (e.g. DefaultTenant_OFFLINE)") @QueryParam("tag") @DefaultValue("") String tag) {
    if (tag.isEmpty()) {
      return _pinotHelixResourceManager.getMinionInstancesWithTags();
    } else {
      return Map.of(tag, _pinotHelixResourceManager.getInstancesWithTag(tag));
    }
  }

  @GET
  @Path("/taskcount")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the count of pending minion tasks")
  public Map<String, Object> getPendingTaskCount() {
    Map<String, Object> taskCounts = new HashMap<>();
    
    // Get total pending task count across all task types
    int totalPendingTasks = _pinotTaskManager.getTotalPendingTaskCount();
    taskCounts.put("totalPendingTasks", totalPendingTasks);
    
    // Get pending task count per task type
    Map<String, Integer> pendingTasksByType = new HashMap<>();
    for (String taskType : _pinotHelixTaskResourceManager.getTaskTypes()) {
      int pendingCount = _pinotTaskManager.getPendingTaskCount(taskType);
      pendingTasksByType.put(taskType, pendingCount);
    }
    taskCounts.put("pendingTasksByType", pendingTasksByType);
    
    // Calculate scaling metrics
    int totalMinionInstances = _pinotHelixResourceManager.getAllInstancesForMinion().size();
    taskCounts.put("totalMinionInstances", totalMinionInstances);
    
    // Tasks per minion - used for autoscaling decisions
    double tasksPerMinion = totalMinionInstances > 0 ? (double) totalPendingTasks / totalMinionInstances : 0;
    taskCounts.put("tasksPerMinion", tasksPerMinion);
    
    return taskCounts;
  }

  @GET
  @Path("/taskcount/{taskType}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the count of pending minion tasks for a specific task type")
  public Map<String, Object> getPendingTaskCountByType(
      @ApiParam(value = "Task type", required = true) @PathParam("taskType") String taskType) {
    Map<String, Object> taskTypeCount = new HashMap<>();
    
    int pendingCount = _pinotTaskManager.getPendingTaskCount(taskType);
    taskTypeCount.put("pendingTasks", pendingCount);
    
    // Get total tasks for this task type (all states)
    Map<String, TaskState> taskStates = _pinotHelixTaskResourceManager.getTaskStates(taskType);
    taskTypeCount.put("totalTasks", taskStates.size());
    
    // Count by state
    Map<TaskState, Integer> countByState = new HashMap<>();
    for (TaskState state : taskStates.values()) {
      countByState.put(state, countByState.getOrDefault(state, 0) + 1);
    }
    taskTypeCount.put("taskStateCount", countByState);
    
    // Calculate scaling metrics
    // Find minions with the appropriate tag for this task type
    Set<String> minionInstances = _pinotHelixResourceManager.getInstancesWithTag(
        _pinotHelixTaskResourceManager.getTaskQueueTag(taskType));
    taskTypeCount.put("availableMinionInstances", minionInstances.size());
    
    // Tasks per minion - used for autoscaling decisions
    double tasksPerMinion = minionInstances.size() > 0 ? (double) pendingCount / minionInstances.size() : 0;
    taskTypeCount.put("tasksPerMinion", tasksPerMinion);
    
    return taskTypeCount;
  }
}