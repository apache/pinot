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

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;


public class PinotTaskRestletResource extends BasePinotControllerRestletResource {
  private static final String TASK_QUEUE_STATE_STOP = "STOP";
  private static final String TASK_QUEUE_STATE_RESUME = "RESUME";

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
  @Get
  @Override
  public Representation get() {
    ConcurrentMap<String, Object> attributes = getRequest().getAttributes();
    String taskName = (String) attributes.get(TASK_NAME);
    String taskType = (String) attributes.get(TASK_TYPE);

    String requestType = getReference().getSegments().get(1);
    switch (requestType) {
      case "tasktypes":
        // "/tasks/tasktypes"
        return getTaskTypes();
      case "tasks":
        // "/tasks/tasks/{taskType}"
        return getTasks(taskType);
      case "taskconfig":
        // "/tasks/taskconfig/{taskName}"
        return getTaskConfig(taskName);
      case "taskstates":
        // "/tasks/taskstates/{taskType}"
        return getTaskStates(taskType);
      case "taskstate":
        // "/tasks/taskstate/{taskName}"
        return getTaskState(taskName);
      case "taskqueues":
        // "/tasks/taskqueues"
        return getTaskQueues();
      case "taskqueuestate":
        // "/tasks/taskqueuestate/{taskType}"
        return getTaskQueueState(taskType);
      default:
        throw new IllegalStateException("Unsupported request type: " + requestType);
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
  @Post
  @Override
  public Representation post(Representation entity) {
    String taskType = (String) getRequest().getAttributes().get(TASK_TYPE);
    String requestType = getReference().getSegments().get(1);
    switch (requestType) {
      case "taskqueue":
        // "/tasks/taskqueue/{taskType}"
        return createTaskQueue(taskType);
      case "task":
        // "/tasks/task/{taskType}"
        return submitTask(taskType, entity);
      default:
        throw new IllegalStateException("Unsupported request type: " + requestType);
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
  @Put
  @Override
  public Representation put(Representation entity) {
    String requestType = getReference().getSegments().get(1);
    switch (requestType) {
      case "taskqueue":
        // "/tasks/taskqueue/{taskType}?state={state}"
        String taskType = (String) getRequest().getAttributes().get(TASK_TYPE);
        String state = getReference().getQueryAsForm().getValues(STATE);
        return toggleTaskQueueState(taskType, state);
      case "scheduletasks":
        // "/tasks/scheduletasks"
        return scheduleTasks();
      default:
        throw new IllegalStateException("Unsupported request type: " + requestType);
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
  @Delete
  @Override
  public Representation delete() {
    String taskType = (String) getRequest().getAttributes().get(TASK_TYPE);
    String requestType = getReference().getSegments().get(1);
    switch (requestType) {
      case "taskqueue":
        // "/tasks/taskqueue/{taskType}"
        return deleteTaskQueue(taskType);
      default:
        throw new IllegalStateException("Unsupported request type: " + requestType);
    }
  }

  @HttpVerb("get")
  @Summary("List all task types")
  @Tags({"task"})
  @Paths({"/tasks/tasktypes", "/tasks/tasktypes/"})
  private Representation getTaskTypes() {
    try {
      List<String> taskTypes = new ArrayList<>(_pinotHelixTaskResourceManager.getTaskTypes());
      Collections.sort(taskTypes);
      return new StringRepresentation(new JSONArray(taskTypes).toString());
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("List all tasks")
  @Tags({"task"})
  @Paths({"/tasks/tasks/{taskType}", "/tasks/tasks/{taskType}/"})
  private Representation getTasks(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType) {
    try {
      List<String> tasks = new ArrayList<>(_pinotHelixTaskResourceManager.getTasks(taskType));
      Collections.sort(tasks);
      return new StringRepresentation(new JSONArray(tasks).toString());
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get a task's configuration")
  @Tags({"task"})
  @Paths({"/tasks/taskconfig/{taskName}", "/tasks/taskconfig/{taskName}/"})
  private Representation getTaskConfig(
      @Parameter(name = "taskName", in = "path", description = "Task name", required = true) String taskName) {
    try {
      PinotTaskConfig taskConfig = _pinotHelixTaskResourceManager.getTaskConfig(taskName);
      JSONObject result = new JSONObject();
      result.put("taskType", taskConfig.getTaskType());
      result.put("configs", new JSONObject(taskConfig.getConfigs()));
      return new StringRepresentation(result.toString(2));
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get all tasks' configuration")
  @Tags({"task"})
  @Paths({"/tasks/taskstates/{taskType}", "/tasks/taskstates/{taskType}/"})
  private Representation getTaskStates(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType) {
    try {
      return new StringRepresentation(
          new JSONObject(_pinotHelixTaskResourceManager.getTaskStates(taskType)).toString(2));
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get a task's state")
  @Tags({"task"})
  @Paths({"/tasks/taskstate/{taskName}", "/tasks/taskstate/{taskName}/"})
  private Representation getTaskState(
      @Parameter(name = "taskName", in = "path", description = "Task name", required = true) String taskName) {
    try {
      return new StringRepresentation(_pinotHelixTaskResourceManager.getTaskState(taskName).toString());
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("List all task queues")
  @Tags({"task"})
  @Paths({"/tasks/taskqueues", "/tasks/taskqueues/"})
  private Representation getTaskQueues() {
    try {
      List<String> taskQueues = new ArrayList<>(_pinotHelixTaskResourceManager.getTaskQueues());
      Collections.sort(taskQueues);
      return new StringRepresentation(new JSONArray(taskQueues).toString());
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get a task queue's state")
  @Tags({"task"})
  @Paths({"/tasks/taskqueuestate/{taskType}", "/tasks/taskqueuestate/{taskType}/"})
  private Representation getTaskQueueState(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType) {
    try {
      return new StringRepresentation(_pinotHelixTaskResourceManager.getTaskQueueState(taskType).toString());
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("post")
  @Summary("Create a task queue")
  @Tags({"task"})
  @Paths({"/tasks/taskqueue/{taskType}", "/tasks/taskqueue/{taskType}/"})
  private Representation createTaskQueue(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType) {
    try {
      _pinotHelixTaskResourceManager.createTaskQueue(taskType);
      return new StringRepresentation("Successfully created task queue for task type: " + taskType);
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("post")
  @Summary("Submit a task")
  @Tags({"task"})
  @Paths({"/tasks/task/{taskType}", "/tasks/task/{taskType}/"})
  private Representation submitTask(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType,
      @Nullable Representation entity) {
    try {
      Map<String, String> configs = new HashMap<>();
      PinotTaskConfig pinotTaskConfig;
      if (entity != null) {
        JSONObject jsonConfig = new JSONObject(entity.getText());
        Iterator iterator = jsonConfig.keys();
        while (iterator.hasNext()) {
          String key = (String) iterator.next();
          configs.put(key, jsonConfig.getString(key));
        }
      }
      pinotTaskConfig = new PinotTaskConfig(taskType, configs);
      String taskName = _pinotHelixTaskResourceManager.submitTask(pinotTaskConfig);
      return new StringRepresentation("Successfully submitted task: " + taskName);
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("put")
  @Summary("Stop/resume a task queue")
  @Tags({"task"})
  @Paths({"/tasks/taskqueue/{taskType}?state={state}", "/tasks/taskqueue/{taskType}?state={state}/"})
  private Representation toggleTaskQueueState(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType,
      @Parameter(name = "state", in = "query", description = "Target state {stop|resume}", required = true) String state) {
    try {
      switch (state.toUpperCase()) {
        case TASK_QUEUE_STATE_STOP:
          _pinotHelixTaskResourceManager.stopTaskQueue(taskType);
          return new StringRepresentation("Successfully stopped task queue for task type: " + taskType);
        case TASK_QUEUE_STATE_RESUME:
          _pinotHelixTaskResourceManager.resumeTaskQueue(taskType);
          return new StringRepresentation("Successfully resumed task queue for task type: " + taskType);
        default:
          throw new IllegalArgumentException("Unsupported state: " + state);
      }
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("put")
  @Summary("Schedule tasks")
  @Tags({"task"})
  @Paths({"/tasks/scheduletasks", "/tasks/scheduletasks/"})
  private Representation scheduleTasks() {
    try {
      _pinotTaskManager.scheduleTasks();
      return new StringRepresentation("Succeeded");
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("delete")
  @Summary("Delete a task queue")
  @Tags({"task"})
  @Paths({"/tasks/taskqueue", "/tasks/taskqueue/"})
  private Representation deleteTaskQueue(
      @Parameter(name = "taskType", in = "path", description = "Task type", required = true) String taskType) {
    try {
      _pinotHelixTaskResourceManager.deleteTaskQueue(taskType);
      return new StringRepresentation("Successfully deleted task queue for task type: " + taskType);
    } catch (Exception e) {
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }
}
