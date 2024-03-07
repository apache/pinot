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
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.PERIODIC_TASK_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/periodictask")
public class PinotControllerPeriodicTaskRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerPeriodicTaskRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PeriodicTaskScheduler _periodicTaskScheduler;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/run")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.EXECUTE_TASK)
  @ApiOperation(value = "Run periodic task against table. If table name is missing, task will run against all tables.")
  public Response runPeriodicTask(
      @ApiParam(value = "Periodic task name", required = true) @QueryParam("taskname") String periodicTaskName,
      @ApiParam(value = "Name of the table") @QueryParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE | REALTIME") @QueryParam("type") String tableType, @Context HttpHeaders headers) {

    if (!_periodicTaskScheduler.hasTask(periodicTaskName)) {
      throw new WebApplicationException("Periodic task '" + periodicTaskName + "' not found.",
          Response.Status.NOT_FOUND);
    }

    if (tableName != null) {
      tableName = DatabaseUtils.translateTableName(tableName, headers).trim();
      List<String> matchingTableNamesWithType = ResourceUtils
          .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableType),
              LOGGER);

      if (matchingTableNamesWithType.size() > 1) {
        throw new WebApplicationException(
            "More than one table matches Table '" + tableName + "'. Matching names: " + matchingTableNamesWithType
                .toString());
      }

      tableName = matchingTableNamesWithType.get(0);
    }

    return Response.ok()
        .entity(_pinotHelixResourceManager.invokeControllerPeriodicTask(tableName, periodicTaskName, null))
        .build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/names")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK)
  @ApiOperation(value = "Get comma-delimited list of all available periodic task names.")
  public List<String> getPeriodicTaskNames() {
    return _periodicTaskScheduler.getTaskNames();
  }
}
