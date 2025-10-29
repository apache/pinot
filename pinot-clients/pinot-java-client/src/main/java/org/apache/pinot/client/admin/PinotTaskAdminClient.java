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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client for task administration operations.
 * Provides methods to monitor and manage Pinot tasks.
 */
public class PinotTaskAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskAdminClient.class);

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotTaskAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all task types available in the cluster.
   *
   * @return Set of task type names
   * @throws PinotAdminException If the request fails
   */
  public Set<String> listTaskTypes()
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/tasktypes", null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("taskTypes"), Set.class);
  }

  /**
   * Gets the state (task queue state) for the given task type.
   *
   * @param taskType Task type name
   * @return Task state
   * @throws PinotAdminException If the request fails
   */
  public TaskState getTaskQueueState(String taskType)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/state", null, _headers);
    return TaskState.valueOf(response.get("state").asText());
  }

  /**
   * Lists all tasks for the given task type.
   *
   * @param taskType Task type name
   * @return Set of task names
   * @throws PinotAdminException If the request fails
   */
  public Set<String> getTasks(String taskType)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/tasks", null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("tasks"), Set.class);
  }

  /**
   * Gets the count of all tasks for the given task type.
   *
   * @param taskType Task type name
   * @return Task count
   * @throws PinotAdminException If the request fails
   */
  public int getTasksCount(String taskType)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/tasks/count", null, _headers);
    return response.get("count").asInt();
  }

  /**
   * Lists all tasks for the given task type and table.
   *
   * @param taskType Task type name
   * @param tableNameWithType Table name with type suffix
   * @return Map of task names to task states
   * @throws PinotAdminException If the request fails
   */
  public Map<String, TaskState> getTaskStatesByTable(String taskType, String tableNameWithType)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/" + tableNameWithType + "/state",
            null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("taskStates"), Map.class);
  }

  /**
   * Gets task metadata for the given task type and table.
   *
   * @param taskType Task type name
   * @param tableNameWithType Table name with type suffix
   * @return Task metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTaskMetadataByTable(String taskType, String tableNameWithType)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/" + tableNameWithType + "/metadata",
            null, _headers);
    return response.toString();
  }

  /**
   * Deletes task metadata for the given task type and table.
   *
   * @param taskType Task type name
   * @param tableNameWithType Table name with type suffix
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteTaskMetadataByTable(String taskType, String tableNameWithType)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeDelete(_controllerAddress, "/tasks/" + taskType + "/" + tableNameWithType + "/metadata",
            null, _headers);
    return response.toString();
  }

  /**
   * Fetches count of sub-tasks for each of the tasks for the given task type.
   *
   * @param taskType Task type name
   * @param state Task state(s) to filter by (optional)
   * @param tableNameWithType Table name with type to filter by (optional)
   * @param minNumSubtasks Minimum number of subtasks to filter by (optional)
   * @return Task counts as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTaskCounts(String taskType, String state, String tableNameWithType,
      Integer minNumSubtasks)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (state != null) {
      queryParams.put("state", state);
    }
    if (tableNameWithType != null) {
      queryParams.put("tableNameWithType", tableNameWithType);
    }
    if (minNumSubtasks != null) {
      queryParams.put("minNumSubtasks", String.valueOf(minNumSubtasks));
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/taskcounts",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Fetches debug information for all tasks for the given task type.
   *
   * @param taskType Task type name
   * @param verbosity Verbosity level (0 by default)
   * @return Task debug information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTasksDebugInfo(String taskType, int verbosity)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("verbosity", String.valueOf(verbosity));

    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/debug", queryParams, _headers);
    return response.toString();
  }

  /**
   * Fetches debug information for all tasks for the given task type and table.
   *
   * @param taskType Task type name
   * @param tableNameWithType Table name with type suffix
   * @param verbosity Verbosity level (0 by default)
   * @return Task debug information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTasksDebugInfo(String taskType, String tableNameWithType, int verbosity)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("verbosity", String.valueOf(verbosity));

    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/" + tableNameWithType + "/debug",
            queryParams, _headers);
    return response.toString();
  }

  /**
   * Fetches task generation information for the recent runs of the given task for the given table.
   *
   * @param taskType Task type name
   * @param tableNameWithType Table name with type suffix
   * @param localOnly Whether to only lookup local cache for logs
   * @return Task generation debug information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTaskGenerationDebugInfo(String taskType, String tableNameWithType, boolean localOnly)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("localOnly", String.valueOf(localOnly));

    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/generator/" + tableNameWithType + "/" + taskType + "/debug",
            queryParams, _headers);
    return response.toString();
  }

  /**
   * Fetches debug information for the given task name.
   *
   * @param taskName Task name
   * @param verbosity Verbosity level (0 by default)
   * @param tableNameWithType Table name with type to filter by (optional)
   * @return Task debug information as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getTaskDebugInfo(String taskName, int verbosity, String tableNameWithType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("verbosity", String.valueOf(verbosity));
    if (tableNameWithType != null) {
      queryParams.put("tableNameWithType", tableNameWithType);
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/task/" + taskName + "/debug",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets a map from task to task state for the given task type.
   *
   * @param taskType Task type name
   * @return Map of task names to task states
   * @throws PinotAdminException If the request fails
   */
  public Map<String, TaskState> getTaskStates(String taskType)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/" + taskType + "/taskstates", null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("taskStates"), Map.class);
  }

  /**
   * Gets the task state for the given task.
   *
   * @param taskName Task name
   * @return Task state
   * @throws PinotAdminException If the request fails
   */
  public TaskState getTaskState(String taskName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/tasks/task/" + taskName + "/state", null, _headers);
    return TaskState.valueOf(response.get("state").asText());
  }

  /**
   * Gets the states of all sub-tasks for the given task.
   *
   * @param taskName Task name
   * @return Map of sub-task names to sub-task states
   * @throws PinotAdminException If the request fails
   */
  public Map<String, String> getSubtaskStates(String taskName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/tasks/subtask/" + taskName + "/state", null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("subtaskStates"), Map.class);
  }

  // Async versions of key methods

  /**
   * Lists all task types available in the cluster (async).
   */
  public CompletableFuture<Set<String>> listTaskTypesAsync() {
    return _transport.executeGetAsync(_controllerAddress, "/tasks/tasktypes", null, _headers)
        .thenApply(response -> PinotAdminTransport.getObjectMapper()
            .convertValue(response.get("taskTypes"), Set.class));
  }

  /**
   * Gets the state for the given task type (async).
   */
  public CompletableFuture<TaskState> getTaskQueueStateAsync(String taskType) {
    return _transport.executeGetAsync(_controllerAddress, "/tasks/" + taskType + "/state", null, _headers)
        .thenApply(response -> TaskState.valueOf(response.get("state").asText()));
  }

  /**
   * Lists all tasks for the given task type (async).
   */
  public CompletableFuture<Set<String>> getTasksAsync(String taskType) {
    return _transport.executeGetAsync(_controllerAddress, "/tasks/" + taskType + "/tasks", null, _headers)
        .thenApply(response -> PinotAdminTransport.getObjectMapper().convertValue(response.get("tasks"), Set.class));
  }

  /**
   * Gets the task state for the given task (async).
   */
  public CompletableFuture<TaskState> getTaskStateAsync(String taskName) {
    return _transport.executeGetAsync(_controllerAddress, "/tasks/task/" + taskName + "/state", null, _headers)
        .thenApply(response -> TaskState.valueOf(response.get("state").asText()));
  }
}
