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
package org.apache.pinot.minion.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.event.MinionTaskState;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Get finer grained progress of tasks running on the minion worker.
 */
@Api(tags = "Progress", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotTaskProgressResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskProgressResource.class);

  @GET
  @Path("/tasks/subtask/progress")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get finer grained task progress tracked in memory for the given subtasks")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getSubtaskProgress(
      @ApiParam(value = "Sub task names separated by comma") @QueryParam("subtaskNames") String subtaskNames) {
    try {
      LOGGER.debug("Getting progress for subtasks: {}", subtaskNames);
      Map<String, Object> progress = new HashMap<>();
      for (String subtaskName : StringUtils.split(subtaskNames, CommonConstants.Minion.TASK_LIST_SEPARATOR)) {
        MinionEventObserver observer = MinionEventObservers.getInstance().getMinionEventObserver(subtaskName);
        if (observer != null) {
          progress.put(subtaskName, observer.getProgress());
        }
      }
      LOGGER.debug("Got subtasks progress: {}", progress);
      return JsonUtils.objectToString(progress);
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
          String.format("Failed to get task progress for subtasks: %s due to error: %s", subtaskNames, e.getMessage()))
          .build());
    }
  }

  @GET
  @Path("/tasks/subtask/state/progress")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get finer grained task progress tracked in memory for given subtasks or given state")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getSubtaskProgress(
      @ApiParam(value = "Sub task names separated by comma") @QueryParam("subtaskNames") @Nullable String subtaskNames,
      @ApiParam(value = "Subtask state", required = true) @QueryParam("subTaskState") @Nullable String subTaskState) {
    try {
      Map<String, MinionEventObserver> progress = new HashMap<>();
      if (StringUtils.isEmpty(subtaskNames) && StringUtils.isEmpty(subTaskState)) {
        LOGGER.debug("Getting progress of all subtasks");
        progress.putAll(MinionEventObservers.getInstance().getMinionEventObservers());
      } else if (!StringUtils.isEmpty(subtaskNames) && !StringUtils.isEmpty(subTaskState)) {
        throw new Exception("Subtask names and state should not be specified at the same time");
      } else if (!StringUtils.isEmpty(subTaskState)) {
        MinionTaskState minionTaskState = MinionTaskState.IN_PROGRESS;
        try {
          minionTaskState = MinionTaskState.valueOf(subTaskState.toUpperCase());
        } catch (IllegalArgumentException e) {
          LOGGER.warn("{} is not a valid subtask state, defaulting to IN_PROGRESS", subTaskState);
          subTaskState = MinionTaskState.IN_PROGRESS.toString();
        }
        LOGGER.debug("Getting progress for subtasks with state {}", subTaskState);
        progress.putAll(MinionEventObservers.getInstance().getMinionEventObserverWithGivenState(minionTaskState));
      } else {
        // !StringUtils.isEmpty(subtaskNames) is true
        LOGGER.debug("Getting progress for subtasks: {}", subtaskNames);
        List<String> subTaskNames =
            Arrays.stream(StringUtils.split(subtaskNames, CommonConstants.Minion.TASK_LIST_SEPARATOR))
                .map(String::trim)
                .collect(Collectors.toList());
        for (String subtaskName : subTaskNames) {
          MinionEventObserver observer = MinionEventObservers.getInstance().getMinionEventObserver(subtaskName);
          if (observer != null) {
            progress.put(subtaskName, observer);
          }
        }
      }
      LOGGER.debug("Got subtasks progress: {}", progress);
      return JsonUtils.objectToString(progress);
    } catch (Exception e) {
      throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
              String.format("Failed to get task progress for subtasks %s with state %s due to error: %s",
                  StringUtils.isEmpty(subtaskNames) ? "NOT_SPECIFIED" : subtaskNames,
                  StringUtils.isEmpty(subTaskState) ? "NOT_SPECIFIED" : subTaskState,
                  e.getMessage()))
          .build());
    }
  }
}
