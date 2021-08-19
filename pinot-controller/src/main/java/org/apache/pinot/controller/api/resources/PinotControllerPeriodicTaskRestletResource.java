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
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.PERIODIC_TASK_TAG)
@Path("/periodictask")
public class PinotControllerPeriodicTaskRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerPeriodicTaskRestletResource.class);
  private static final String API_REQUEST_ID_PREFIX = "api-";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PeriodicTaskScheduler _periodicTaskScheduler;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/run")
  @ApiOperation(value = "Run a periodic task against specified table. If no table name is specified, task will run against all tables.")
  public String runPeriodicTask(
      @ApiParam(value = "Periodic task name", required = true) @QueryParam("taskname") String periodicTaskName,
      @ApiParam(value = "Name of the table") @QueryParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE | REALTIME") @QueryParam("type") String tableType) {

    if (!_periodicTaskScheduler.hasTask(periodicTaskName)) {
      throw new WebApplicationException("Periodic task '" + periodicTaskName + "' not found.",
          Response.Status.NOT_FOUND);
    }

    if (tableName != null) {
      tableName = tableName.trim();
      List<String> matchingTableNamesWithType = ResourceUtils
          .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableType),
              LOGGER);

      if (matchingTableNamesWithType.size() > 1) {
        throw new WebApplicationException(
            "More than one table matches Table '" + tableName + "'. Matching names: " + matchingTableNamesWithType.toString());
      }

      tableName = matchingTableNamesWithType.get(0);
    }

    // Generate an id for this request by taking first eight characters of a randomly generated UUID. This request id
    // is returned to the user and also appended to log messages so that user can locate all log messages associated
    // with this PeriodicTask's execution.
    String periodicTaskRequestId = API_REQUEST_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8);

    LOGGER.info(
        "[TaskRequestId: {}] Sending periodic task execution message to all controllers for running task {} against {}.",
        periodicTaskRequestId, periodicTaskName, tableName != null ? " table '" + tableName + "'" : "all tables");

    // Create and send message to send to all controllers (including this one)
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setResource(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    recipientCriteria.setSelfExcluded(false);
    RunPeriodicTaskMessage runPeriodicTaskMessage =
        new RunPeriodicTaskMessage(periodicTaskRequestId, periodicTaskName, tableName);

    ClusterMessagingService clusterMessagingService =
        _pinotHelixResourceManager.getHelixZkManager().getMessagingService();
    int messageCount = clusterMessagingService.send(recipientCriteria, runPeriodicTaskMessage, null, -1);
    LOGGER.info("[TaskRequestId: {}] Periodic task execution message sent to {} controllers.", periodicTaskRequestId,
        messageCount);

    return "{\"Log Request Id\": \"" + periodicTaskRequestId + "\",\"Controllers notified\":" + (messageCount > 0)
        + "}";
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/names")
  @ApiOperation(value = "Get comma-delimited list of all available periodic task names.")
  public List<String> getPeriodicTaskNames() {
    return _periodicTaskScheduler.getTaskNames();
  }
}
