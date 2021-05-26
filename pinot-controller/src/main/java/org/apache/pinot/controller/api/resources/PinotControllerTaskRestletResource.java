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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.task.TaskState;
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
import org.apache.pinot.core.periodictask.PeriodicTaskState;
import org.apache.pinot.core.periodictask.TaskExecutionResult;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.api.resources.Constants.*;


/**
 * API related to triggering periodic controller tasks.
 */
@Api(tags = Constants.TRIGGER_TAG)
@Path("/")
public class PinotControllerTaskRestletResource {
  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  PeriodicTaskScheduler _periodicTaskScheduler;

  @GET
  @Path("/triggers/tasknames")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List all available controller periodic tasks that can be triggered by the user")
  public List<PeriodicTaskInfo> listTaskTypes() {
    /* The assumption made here is that *any* of the registered periodic tasks could be triggered externally by the user.
     * If there is a need to present only a subset of periodic tasks amenable to external user triggers, then the
     * task has to be annotated with additional info so that we can present a filtered list for user triggers.
     */
    List<PeriodicTaskInfo> taskList = _periodicTaskScheduler.getRegisteredTasks();
    return taskList;
  }

  /**
   * <ul>
   *   <li>Sample CURL request to schedule a task</li>
   *   curl -i -X PUT -H 'Content-Type: application/json' -d
   *   '{
   *     "taskName" : "SegmentRelocator",
   *   }' http://localhost:1234/triggers/schedule
   * </ul>
   */

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/triggers/schedule")
  @ApiOperation("Schedule a controller task in an async fashion and return the current execution status")
  public TaskExecutionResult schedule(@ApiParam(value = "Task name", required = true) @QueryParam("taskName") String taskName) {
    try {
      return _periodicTaskScheduler.schedule(taskName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to execute task", Response.Status.BAD_REQUEST);
    }
  }

  /**
   * <li>GET '/triggers/task/{taskName}/state': Get the task state for the given task</li>
   * Note that, the task state transitions to {@code IDLE} after a successful run. Might be worth allowing users
   * to monitor the completion status of a previously scheduled task - needs more book-keeping.
   */
  @GET
  @Path("/triggers/task/{taskName}/state")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the state for the given task name")
  public PeriodicTaskState getTaskState(
      @ApiParam(value = "Task name", required = true) @PathParam("taskName") String taskName) {
    try {
      return _periodicTaskScheduler.getTaskState(taskName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to fetch task state", Response.Status.BAD_REQUEST);
    }
  }
}
